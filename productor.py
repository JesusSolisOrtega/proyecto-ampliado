import os
import time
import signal
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

ARCHIVO_DATOS = os.getenv("ARCHIVO_DATOS", "data/endesa_streaming_dev.csv")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "consumo_streaming")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("KafkaProducer")

is_running = True

def signal_handler(sig, frame):
    """Handles SIGINT for graceful shutdown."""
    global is_running
    logger.warning("Interrupt signal received. Initiating graceful shutdown...")
    is_running = False

signal.signal(signal.SIGINT, signal_handler)

def init_producer():
    """Initializes and returns the Kafka producer."""
    logger.info(f"Connecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: str(v).encode('utf-8'),
            api_version=(0, 10, 1),
            retries=3
        )
        logger.info("Kafka connection established successfully.")
        return producer
    except Exception as e:
        logger.error(f"Critical failure connecting to Kafka: {e}")
        exit(1)

def publish_data(producer):
    """
    Reads the CSV file line-by-line (lazy loading) to prevent high memory consumption
    and streams records to Kafka.
    """
    global is_running
    logger.info(f"Starting to read from: {ARCHIVO_DATOS}")
    processed_count = 0
    
    try:
        with open(ARCHIVO_DATOS, 'r', encoding='utf-8') as file:
            for line in file:
                if not is_running:
                    logger.info("Producer interrupted by user.")
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    # Asynchronous send
                    producer.send(KAFKA_TOPIC, value=line)
                    processed_count += 1
                except KafkaError as e:
                    logger.error(f"Failed to send message to Kafka: {e}")
                
                if processed_count % 100 == 0:
                    logger.info(f"Published {processed_count} events to '{KAFKA_TOPIC}'...")
                
                # Throttling to avoid saturating the broker
                time.sleep(0.05)
                
    except FileNotFoundError:
        logger.error(f"Dataset {ARCHIVO_DATOS} not found.")
    except Exception as e:
        logger.error(f"Unexpected error during file read: {e}")
    finally:
        logger.info("Flushing Kafka buffers...")
        producer.flush(timeout=5)
        producer.close()
        logger.info(f"Producer closed cleanly. Total records pushed: {processed_count}.")

if __name__ == "__main__":
    logger.info("--- Starting Streaming Producer ---")
    producer_instance = init_producer()
    publish_data(producer_instance)
