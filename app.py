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

# Configure logger
logger.remove()
logger.add(
    "kafka_app.log",
    format="{time} | {level} | {message}",
    level="INFO",
    rotation="10 MB"
)
logger.add(lambda msg: print(msg), level="INFO")

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
ORDER_TOPIC = "order-events"
DLQ_TOPIC = "order-events-dlq"
CONSUMER_GROUP = "order-processor"

app = FastAPI(title="Kafka with Python")

class OrderItem(BaseModel):
    product_id: str
    quantity: int
    price: float

class Order(BaseModel):
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    items: List[OrderItem]
    total_amount: float
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    idempotency_key: Optional[str] = None

producer = None
admin_client = None
consumer = None
processed_orders = set()  # For simple idempotency check
consumer_running = False

# Dictionary to track orders being processed (request_id -> order_id)
# This is used to prevent duplicate submissions
processing_orders = {}

@app.on_event("startup")
async def startup_event():
    global producer, admin_client, consumer, consumer_running
    
    # Wait for Kafka to be ready
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info("Attempting to connect to Kafka...")
            
            # Initialize admin client to create topics
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                client_id='kafka-admin-client'
            )
            
            # Create topics if they don't exist
            topic_list = []
            topic_list.append(NewTopic(name=ORDER_TOPIC, num_partitions=3, replication_factor=1))
            topic_list.append(NewTopic(name=DLQ_TOPIC, num_partitions=1, replication_factor=1))
            
            try:
                admin_client.create_topics(new_topics=topic_list, validate_only=False)
                logger.info(f"Created topics: {ORDER_TOPIC}, {DLQ_TOPIC}")
            except kafka.errors.TopicAlreadyExistsError:
                logger.info("Topics already exist")
                
            # Initialize producer
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: v.encode('utf-8') if v else None,
                acks='all',  # Wait for all replicas to acknowledge the message
                max_in_flight_requests_per_connection=1  # Ensure ordering
            )
            
            logger.info("Successfully connected to Kafka")
            break
        
        except Exception as e:
            retry_count += 1
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            if retry_count >= max_retries:
                logger.error("Max retries reached, could not connect to Kafka")
                raise
            await asyncio.sleep(5)
    
    # Start the consumer in a background task
    consumer_running = True
    asyncio.create_task(run_consumer())

@app.on_event("shutdown")
async def shutdown_event():
    global consumer_running
    
    # Signal consumer to stop
    consumer_running = False
    
    if producer:
        logger.info("Closing Kafka producer")
        producer.close()
    
    if admin_client:
        logger.info("Closing Kafka admin client")
        admin_client.close()

async def run_consumer():
    """Background task to run the Kafka consumer"""
    logger.info("Starting Kafka consumer...")
    global consumer, consumer_running
    
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset='earliest',  # Start from the earliest message if no offset is stored
            enable_auto_commit=False,  # Manual commit for better control
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None
        )
        
        consumer.subscribe([ORDER_TOPIC])
        logger.info(f"Consumer subscribed to {ORDER_TOPIC}")
        
        while consumer_running:
            try:
                # Poll for messages with a timeout
                messages = consumer.poll(timeout_ms=1000)
                
                if not messages:
                    # No messages received during this poll
                    await asyncio.sleep(0.1)
                    continue
                    
                logger.info(f"Received {sum(len(records) for records in messages.values())} messages from Kafka")
                
                for tp, records in messages.items():
                    for record in records:
                        try:
                            logger.info(f"Processing record: partition={tp.partition}, offset={record.offset}, key={record.key}")
                            
                            # Check for idempotency based on order_id first (most reliable)
                            order_id = record.value.get('order_id')
                            if order_id in processed_orders:
                                logger.info(f"Skipping duplicate order with order_id: {order_id}")
                                
                                # Still commit the offset even for duplicates
                                consumer.commit({
                                    tp: OffsetAndMetadata(record.offset + 1, None)
                                })
                                logger.info(f"Committed offset {record.offset + 1} for partition {tp.partition} (duplicate message)")
                                continue
                            
                            # Then check idempotency_key if provided
                            idempotency_key = record.value.get('idempotency_key')
                            if idempotency_key and idempotency_key in processed_orders:
                                logger.info(f"Skipping duplicate order with idempotency_key: {idempotency_key}")
                                
                                # Still commit the offset even for duplicates
                                consumer.commit({
                                    tp: OffsetAndMetadata(record.offset + 1, None)
                                })
                                logger.info(f"Committed offset {record.offset + 1} for partition {tp.partition} (duplicate message)")
                                continue
                            
                            # Process the order
                            await process_order(record.value)
                            
                            # Add to processed orders set for idempotency
                            processed_orders.add(order_id)
                            if idempotency_key:
                                processed_orders.add(idempotency_key)
                            
                            # Commit offset for this message
                            consumer.commit({
                                tp: OffsetAndMetadata(record.offset + 1, None)
                            })
                            logger.info(f"Committed offset {record.offset + 1} for partition {tp.partition}")
                            
                        except Exception as e:
                            error_message = f"Error processing message: {str(e)}\n{traceback.format_exc()}"
                            logger.error(error_message)
                            
                            # Send to Dead Letter Queue
                            send_to_dlq(record.value, str(e))
                            
                            # Still commit the offset to avoid reprocessing the same failed message
                            consumer.commit({
                                tp: OffsetAndMetadata(record.offset + 1, None)
                            })
                            logger.info(f"Committed offset for failed message: {record.offset + 1} for partition {tp.partition}")
                
                # Small delay to prevent CPU hogging
                await asyncio.sleep(0.1)
                
            except Exception as e:
                error_message = f"Error in consumer poll loop: {str(e)}\n{traceback.format_exc()}"
                logger.error(error_message)
                await asyncio.sleep(5)  # Wait a bit before retrying
            
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
    """Process an order from Kafka"""
    logger.info(f"Processing order: {order_data['order_id']}")
    
    # Simulate processing time
    await asyncio.sleep(0.5)
    
    # Enhanced failure simulation - check multiple fields
    should_fail = False
    failure_reason = []
    
    # Check if customer_id ends with 'f'
    if order_data['customer_id'].endswith('f'):
        should_fail = True
        failure_reason.append("customer_id ends with 'f'")
    
    # Simulate failure if any of the conditions are met
    if should_fail:
        raise Exception(f"Order processing failed (simulated error): {', '.join(failure_reason)}")
    
    logger.info(f"Successfully processed order: {order_data['order_id']}")

def send_to_dlq(message: Dict[str, Any], error_reason: str):
    """Send a failed message to the Dead Letter Queue"""
    if not producer:
        logger.error("Producer not initialized")
        return
    
    # Add error information
    dlq_message = message.copy()
    dlq_message['error'] = {
        'reason': error_reason,
        'timestamp': datetime.now().isoformat()
    }
    
    # Send to DLQ
    producer.send(
        DLQ_TOPIC,
        key=message.get('order_id', str(uuid.uuid4())),
        value=dlq_message
    )
    producer.flush()
    logger.info(f"Sent message to DLQ: {message.get('order_id')}")

@app.post("/orders/", status_code=201)
async def create_order(order: Order, request: Request, x_idempotency_key: Optional[str] = Header(None)):
    """Create a new order and send it to Kafka"""
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer not available")
    
    # Use provided idempotency key or generate one if not provided
    client_idempotency_key = x_idempotency_key or f"client-{request.client.host}-{datetime.now().isoformat()}"
    
    # Check if this order is already being processed (prevent duplicate submission)
    if client_idempotency_key in processing_orders:
        logger.info(f"Duplicate request detected with idempotency key: {client_idempotency_key}")
        order_id = processing_orders[client_idempotency_key]
        return {
            "status": "success",
            "message": f"Order {order_id} already created (duplicate request)",
            "duplicate": True
        }
    
    # Set order idempotency key if provided via header
    if x_idempotency_key:
        order.idempotency_key = x_idempotency_key
    
    order_dict = order.model_dump()
    logger.info(f"Creating order: {order.order_id} with idempotency key: {order.idempotency_key}")
    
    # Track that we're processing this order
    processing_orders[client_idempotency_key] = order.order_id
    
    try:
        # Send to Kafka with the order_id as the key for partitioning
        future = producer.send(
            ORDER_TOPIC,
            key=order.order_id,
            value=order_dict
        )
        
        # Wait for the send to complete and get metadata
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
        # Remove from processing orders on failure
        if client_idempotency_key in processing_orders:
            del processing_orders[client_idempotency_key]
        raise HTTPException(status_code=500, detail=f"Failed to send message to Kafka: {str(e)}")

@app.get("/")
async def root():
    return {"message": "Kafka with Python - Use /docs to see the API documentation"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True) 