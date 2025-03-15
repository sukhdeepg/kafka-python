# Understanding Kafka with implementation in Python
A simple demonstration of Kafka with Python and FastAPI, showing a real-world e-commerce order processing system.

## Features
- Kafka in KRaft mode.
- **Producer & Consumer**: Complete implementation with best practices.
- **Dead Letter Queue (DLQ)**: Handles failed message processing.
- **Application-level Idempotency**: Prevents duplicate order processing using idempotency keys.
- **Manual Offset Commitment**: Explicit consumer offset management.
- **Detailed Logging**: Comprehensive logging for understanding Kafka concepts.

## Architecture
This demo simulates an e-commerce order processing system:

1. **Producer**: REST API endpoint for creating orders, which publishes events to Kafka.
2. **Consumer**: Background service that consumes order events and processes them.
3. **DLQ**: Failed orders are sent to a Dead Letter Queue topic for later inspection.
4. **Idempotency**: Using application-level idempotency keys to prevent duplicate processing.

## Setup and Running

### Prerequisites
- Docker and Docker Compose
- Python 3.8+

### Running the Application
1. Start Kafka using Docker Compose:

```bash
docker-compose up -d
```

2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

3. Run the FastAPI application:

```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

4. Visit the API documentation at http://localhost:8000/docs

## API Usage

### Create a new order
```bash
curl -X POST http://localhost:8000/orders/ \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer123",
    "items": [
      {
        "product_id": "product456",
        "quantity": 2,
        "price": 29.99
      }
    ],
    "total_amount": 59.98
  }'
```

## Kafka Concepts Demonstrated
1. **Topic Partitioning**: Order topic has multiple partitions for parallelism.
2. **Consumer Groups**: Consumer works within a consumer group for scalability.
3. **Manual Offset Commitment**: Explicit control over offset commits.
4. **Dead Letter Queue**: Handling failed message processing.
5. **Idempotency**: Preventing duplicate message processing at the application level.
6. **Producer Acknowledgment**: Using `acks=all` for reliable delivery.

## Testing Scenarios
1. **Normal Processing**: Submit regular orders to see them processed successfully.
2. **Failure Handling**: Orders with `customer_id` ending in 'f' will fail (simulated) and go to DLQ.
3. **Idempotency**: Submit the same order twice (same idempotency key) to see deduplication.

## Logging
All operations are extensively logged to both console and the `kafka_app.log` file. 