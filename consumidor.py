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
logger = logging.getLogger("Consumidor")

is_running = True

def signal_handler(sig, frame):
    global is_running
    logger.warning("Interrupción recibida. Cerrando...")
    is_running = False

signal.signal(signal.SIGINT, signal_handler)

def init_consumer():
    logger.info(f"Conectando al broker en {KAFKA_BOOTSTRAP_SERVERS}...")
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
        logger.info(f"Suscrito al topic '{KAFKA_TOPIC}'.")
        return consumer
    except Exception as e:
        logger.error(f"Error al iniciar el consumidor: {e}")
        exit(1)

def listen_messages(consumer):
    global is_running
    logger.info(f"Escuchando topic '{KAFKA_TOPIC}'... (Ctrl+C para salir)")

    count = 0
    try:
        while is_running:
            batch = consumer.poll(timeout_ms=1000)
            for topic_partition, messages in batch.items():
                for message in messages:
                    if not is_running:
                        break
                    logger.info(f"-> RECIBIDO {topic_partition}: {message.value}")
                    count += 1
    except Exception as e:
        logger.error(f"Error durante el polling: {e}")
    finally:
        logger.info(f"Consumidor cerrado. Mensajes recibidos: {count}")
        consumer.close()

if __name__ == "__main__":
    logger.info("--- Iniciando Consumidor de Verificación ---")
    consumer_instance = init_consumer()
    listen_messages(consumer_instance)
