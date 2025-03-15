# Understanding Kafka with implementation in Python
A simple demonstration of Kafka with Python and FastAPI, showing a real-world e-commerce order processing system.

## Features
- Kafka in KRaft mode.
- **Producer & Consumer**: Complete implementation with best practices.
- **Dead Letter Queue (DLQ)**: Handles failed message processing.
- **Application-level Idempotency**: Prevents duplicate order processing using idempotency keys.
- **Manual Offset Commitment**: Explicit consumer offset management.
- **Detailed Logging**: Comprehensive logging for understanding Kafka concepts.

## Screenshots
- Creating an order
<img width="598" alt="Screenshot 2025-03-15 at 9 46 15 PM" src="https://github.com/user-attachments/assets/f1d50af7-3ad1-42b1-9bb3-a039b68d8dc6" />

- Sending message to the partition
<img width="602" alt="Screenshot 2025-03-15 at 9 46 36 PM" src="https://github.com/user-attachments/assets/09d402e5-a556-4f04-a246-6b9855e82de5" />

- Showing how many records did we get in the message
<img width="637" alt="Screenshot 2025-03-15 at 9 47 12 PM" src="https://github.com/user-attachments/assets/5af06824-a397-4ad2-9a88-dc3217250396" />

- Consumer processing the message
<img width="606" alt="Screenshot 2025-03-15 at 9 47 41 PM" src="https://github.com/user-attachments/assets/ac53f97f-d935-4176-aca9-22c374917b3a" />

- Consumer committed the offset
<img width="603" alt="Screenshot 2025-03-15 at 9 48 20 PM" src="https://github.com/user-attachments/assets/71eebbd7-6a87-4ff1-a202-d7cb73fe14c1" />

- Testing idempotency
<img width="594" alt="Screenshot 2025-03-15 at 9 57 13 PM" src="https://github.com/user-attachments/assets/00d6194b-9afe-4ab5-b9b3-e343c226be5d" />

- Testing simulated failure
<img width="613" alt="Screenshot 2025-03-15 at 10 07 30 PM" src="https://github.com/user-attachments/assets/f0b66c95-f0dd-4648-bf47-4c28477e60be" />

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
