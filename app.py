import json
import uuid
import asyncio
import traceback
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, HTTPException, Header, Request
from pydantic import BaseModel, Field
from datetime import datetime
from loguru import logger
import kafka.errors
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.consumer.subscription_state import OffsetAndMetadata

# Configure structured logging with file rotation
logger.remove()
logger.add(
    "kafka_app.log",
    format="{time} | {level} | {message}",
    level="INFO",
    rotation="10 MB"  # Rotate log files when they reach 10MB
)
logger.add(lambda msg: print(msg), level="INFO")

# Kafka configuration constants
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
ORDER_TOPIC = "order-events"
DLQ_TOPIC = "order-events-dlq"  # Dead Letter Queue for failed messages
CONSUMER_GROUP = "order-processor"  # Consumer group for load balancing

app = FastAPI(title="Kafka with Python")

# Pydantic models for request/response validation
class OrderItem(BaseModel):
    product_id: str
    quantity: int
    price: float

class Order(BaseModel):
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))  # Auto-generate unique ID
    customer_id: str
    items: List[OrderItem]
    total_amount: float
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())  # Auto-timestamp
    idempotency_key: Optional[str] = None  # For preventing duplicate message processing

# Global Kafka components - initialized at startup
producer = None
admin_client = None
consumer = None
processed_orders = set()  # In-memory set for simple idempotency tracking
consumer_running = False

# Dictionary to track orders being processed (request_id -> order_id)
# This prevents duplicate API submissions before Kafka processing
processing_orders = {}

@app.on_event("startup")
async def startup_event():
    """Initialize Kafka components with connection retry logic"""
    global producer, admin_client, consumer, consumer_running
    
    # Wait for Kafka to be ready with retry mechanism
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info("Attempting to connect to Kafka...")
            
            # Initialize admin client to create and manage topics
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                client_id='kafka-admin-client'
            )
            
            # Create topics if they don't exist
            topic_list = []
            # Main topic with 3 partitions for parallel processing and load distribution
            # replication_factor=1 means each message is stored on only 1 broker (no redundancy)
            # In production, use replication_factor=3 for fault tolerance
            topic_list.append(NewTopic(name=ORDER_TOPIC, num_partitions=3, replication_factor=1))
            # DLQ with single partition since failed messages are typically low volume
            # replication_factor=1 means no backup copies (acceptable for DLQ in dev/test)
            topic_list.append(NewTopic(name=DLQ_TOPIC, num_partitions=1, replication_factor=1))
            
            try:
                admin_client.create_topics(new_topics=topic_list, validate_only=False)
                logger.info(f"Created topics: {ORDER_TOPIC}, {DLQ_TOPIC}")
            except kafka.errors.TopicAlreadyExistsError:
                logger.info("Topics already exist")
                
            # Initialize producer with reliability configurations
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # JSON serialization
                key_serializer=lambda v: v.encode('utf-8') if v else None,  # String key serialization
                acks='all',  # Wait for all in-sync replicas to acknowledge (highest durability)
                max_in_flight_requests_per_connection=1  # Ensure strict message ordering
            )
            
            logger.info("Successfully connected to Kafka")
            break
        
        except Exception as e:
            retry_count += 1
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            if retry_count >= max_retries:
                logger.error("Max retries reached, could not connect to Kafka")
                raise
            await asyncio.sleep(5)  # Wait before retry
    
    # Start the consumer in a background task
    consumer_running = True
    asyncio.create_task(run_consumer())

@app.on_event("shutdown")
async def shutdown_event():
    """Gracefully shutdown all Kafka components"""
    global consumer_running
    
    # Signal consumer to stop processing
    consumer_running = False
    
    if producer:
        logger.info("Closing Kafka producer")
        producer.close()
    
    if admin_client:
        logger.info("Closing Kafka admin client")
        admin_client.close()

async def run_consumer():
    """Background task to run the Kafka consumer and process messages"""
    logger.info("Starting Kafka consumer...")
    global consumer, consumer_running
    
    try:
        # Initialize consumer with manual offset management for better control
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,  # Consumer group enables load balancing across multiple consumers
            auto_offset_reset='earliest',  # Start from beginning if no committed offset exists
            enable_auto_commit=False,  # Disable automatic offset commits for precise error handling
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # JSON deserialization
            key_deserializer=lambda m: m.decode('utf-8') if m else None  # String key deserialization
        )
        
        # Subscribe to the main order topic
        consumer.subscribe([ORDER_TOPIC])
        logger.info(f"Consumer subscribed to {ORDER_TOPIC}")
        
        # Main consumer polling loop
        while consumer_running:
            try:
                # Poll for messages with 1 second timeout
                messages = consumer.poll(timeout_ms=1000)
                
                if not messages:
                    # No messages received during this poll cycle
                    await asyncio.sleep(0.1)
                    continue
                    
                logger.info(f"Received {sum(len(records) for records in messages.values())} messages from Kafka")
                
                # Process messages grouped by topic partition
                for tp, records in messages.items():
                    for record in records:
                        try:
                            logger.info(f"Processing record: partition={tp.partition}, offset={record.offset}, key={record.key}")
                            
                            # DUAL-LAYER IDEMPOTENCY PROTECTION:
                            
                            # Layer 1: Business-level deduplication using order_id
                            # Prevents processing the same business order multiple times
                            # This handles cases where the same order gets submitted multiple times
                            order_id = record.value.get('order_id')
                            if order_id in processed_orders:
                                logger.info(f"Skipping duplicate order with order_id: {order_id}")
                                
                                # Commit offset even for duplicates to avoid reprocessing
                                consumer.commit({
                                    tp: OffsetAndMetadata(record.offset + 1, None)
                                })
                                logger.info(f"Committed offset {record.offset + 1} for partition {tp.partition} (duplicate message)")
                                continue
                            
                            # Layer 2: Client-provided idempotency key deduplication
                            # Handles cases where client retries failed API calls with same idempotency key
                            # Client can provide same key for retries to ensure exactly-once processing
                            idempotency_key = record.value.get('idempotency_key')
                            if idempotency_key and idempotency_key in processed_orders:
                                logger.info(f"Skipping duplicate order with idempotency_key: {idempotency_key}")
                                
                                # Commit offset for duplicate message
                                consumer.commit({
                                    tp: OffsetAndMetadata(record.offset + 1, None)
                                })
                                logger.info(f"Committed offset {record.offset + 1} for partition {tp.partition} (duplicate message)")
                                continue
                            
                            # Process the order (business logic)
                            await process_order(record.value)
                            
                            # Track both identifiers for comprehensive idempotency protection
                            # order_id: Prevents duplicate business orders
                            # idempotency_key: Prevents duplicate client retries
                            processed_orders.add(order_id)
                            if idempotency_key:
                                processed_orders.add(idempotency_key)
                            
                            # Manual offset commit after successful processing
                            # Offset+1 because we want to commit the NEXT message to be consumed
                            # This ensures we don't reprocess this message if consumer restarts
                            consumer.commit({
                                tp: OffsetAndMetadata(record.offset + 1, None)
                            })
                            logger.info(f"Committed offset {record.offset + 1} for partition {tp.partition}")
                            
                        except Exception as e:
                            error_message = f"Error processing message: {str(e)}\n{traceback.format_exc()}"
                            logger.error(error_message)
                            
                            # Send failed message to Dead Letter Queue for manual review
                            send_to_dlq(record.value, str(e))
                            
                            # Commit offset to prevent infinite retry of same failed message
                            consumer.commit({
                                tp: OffsetAndMetadata(record.offset + 1, None)
                            })
                            logger.info(f"Committed offset for failed message: {record.offset + 1} for partition {tp.partition}")
                
                # Small delay to prevent CPU hogging
                await asyncio.sleep(0.1)
                
            except Exception as e:
                error_message = f"Error in consumer poll loop: {str(e)}\n{traceback.format_exc()}"
                logger.error(error_message)
                await asyncio.sleep(5)  # Wait before retrying poll loop
            
    except Exception as e:
        error_message = f"Consumer error: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
    finally:
        if consumer:
            try:
                consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {str(e)}")

async def process_order(order_data: Dict[str, Any]):
    """Process an order from Kafka - contains business logic"""
    logger.info(f"Processing order: {order_data['order_id']}")
    
    # Simulate processing time (database operations, external API calls, etc.)
    await asyncio.sleep(0.5)
    
    # Simulate business logic failure conditions
    should_fail = False
    failure_reason = []
    
    # Example business rule: fail orders for customers ending with 'f'
    if order_data['customer_id'].endswith('f'):
        should_fail = True
        failure_reason.append("customer_id ends with 'f'")
    
    # Raise exception to trigger error handling and DLQ
    if should_fail:
        raise Exception(f"Order processing failed (simulated error): {', '.join(failure_reason)}")
    
    logger.info(f"Successfully processed order: {order_data['order_id']}")

def send_to_dlq(message: Dict[str, Any], error_reason: str):
    """Send a failed message to the Dead Letter Queue with error context"""
    if not producer:
        logger.error("Producer not initialized")
        return
    
    # Enrich message with error information for debugging
    dlq_message = message.copy()
    dlq_message['error'] = {
        'reason': error_reason,
        'timestamp': datetime.now().isoformat()
    }
    
    # Send to DLQ topic for manual investigation
    producer.send(
        DLQ_TOPIC,
        key=message.get('order_id', str(uuid.uuid4())),  # Use order_id as key
        value=dlq_message
    )
    producer.flush()  # Ensure message is sent immediately
    logger.info(f"Sent message to DLQ: {message.get('order_id')}")

@app.post("/orders/", status_code=201)
async def create_order(order: Order, request: Request, x_idempotency_key: Optional[str] = Header(None)):
    """Create a new order and send it to Kafka topic"""
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer not available")
    
    # IDEMPOTENCY AT API LEVEL:
    # Generate client-specific idempotency key if not provided in header
    # This prevents duplicate API requests before they even reach Kafka
    client_idempotency_key = x_idempotency_key or f"client-{request.client.host}-{datetime.now().isoformat()}"
    
    # Check if this exact API request is already being processed
    # This is different from Kafka-level idempotency - it's API-level protection
    if client_idempotency_key in processing_orders:
        logger.info(f"Duplicate request detected with idempotency key: {client_idempotency_key}")
        order_id = processing_orders[client_idempotency_key]
        return {
            "status": "success",
            "message": f"Order {order_id} already created (duplicate request)",
            "duplicate": True
        }
    
    # Set idempotency key from header if provided
    if x_idempotency_key:
        order.idempotency_key = x_idempotency_key
    
    order_dict = order.model_dump()
    logger.info(f"Creating order: {order.order_id} with idempotency key: {order.idempotency_key}")
    
    # Track that we're processing this order to prevent duplicates
    processing_orders[client_idempotency_key] = order.order_id
    
    try:
        # Send to Kafka with order_id as partition key for related message grouping
        # Partition key ensures orders from same customer/order go to same partition
        # This maintains ordering for related messages within the partition
        future = producer.send(
            ORDER_TOPIC,
            key=order.order_id,  # Partition key for message routing and ordering
            value=order_dict
        )
        
        # Wait for send confirmation and get metadata
        # get() blocks until message is acknowledged by Kafka brokers
        # timeout=10 prevents indefinite blocking
        record_metadata = future.get(timeout=10)
        logger.info(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
        
        return {
            "status": "success",
            "message": f"Order {order.order_id} created successfully",
            "kafka_metadata": {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset
            }
        }
    except Exception as e:
        logger.error(f"Failed to send message: {str(e)}")
        # Remove from processing orders on failure to allow retry
        if client_idempotency_key in processing_orders:
            del processing_orders[client_idempotency_key]
        raise HTTPException(status_code=500, detail=f"Failed to send message to Kafka: {str(e)}")

@app.get("/")
async def root():
    return {"message": "Kafka with Python - Use /docs to see the API documentation"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True) 