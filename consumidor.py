import os
import signal
import logging
from kafka import KafkaConsumer

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "consumo_streaming")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("KafkaConsumer")

is_running = True

def signal_handler(sig, frame):
    """Handles SIGINT for graceful shutdown."""
    global is_running
    logger.warning("Interrupt signal received. Stopping consumer...")
    is_running = False

signal.signal(signal.SIGINT, signal_handler)

def init_consumer():
    """Initializes and returns the Kafka consumer."""
    logger.info(f"Connecting Consumer to broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: v.decode('utf-8'),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='verification_group_dev',
            api_version=(0, 10, 1)
        )
        logger.info("Consumer successfully subscribed to the topic.")
        return consumer
    except Exception as e:
        logger.error(f"Failed to start Kafka Consumer: {e}")
        exit(1)

def listen_messages(consumer):
    """Continuously polls the topic for new messages."""
    global is_running
    logger.info(f"Polling topic '{KAFKA_TOPIC}'...")
    logger.info("[Press Ctrl+C to exit cleanly]")
    
    processed_count = 0
    try:
        while is_running:
            # Poll with timeout to prevent blocking thread entirely
            message_batch = consumer.poll(timeout_ms=1000)
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    if not is_running:
                        break
                    logger.info(f"-> RECEIVED {topic_partition}: {message.value}")
                    processed_count += 1
                    
    except Exception as e:
        logger.error(f"Consumer exception during poll: {e}")
    finally:
        logger.info(f"Closing consumer session. Events received: {processed_count}")
        consumer.close()

if __name__ == "__main__":
    logger.info("--- Starting Verification Consumer ---")
    consumer_instance = init_consumer()
    listen_messages(consumer_instance)
