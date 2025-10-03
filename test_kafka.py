from kafka import KafkaProducer
import json

# Kafka server and topic
KAFKA_BROKER = 'ec2-3-84-97-131.compute-1.amazonaws.com:9094'
TOPIC = 'bottle-art-clickstream'

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize dict to JSON
)

# Example dictionary message
message = {
    "user_id": 123,
    "action": "click",
    "item": "bottle-art",
    "timestamp": "2025-10-02T12:00:00Z"
}

# Send message
producer.send(TOPIC, value=message)

# Wait for all messages to be sent
producer.flush()
print("Message sent successfully!")
