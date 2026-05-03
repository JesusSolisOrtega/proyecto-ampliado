import os
import time
import signal
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

ARCHIVO_DATOS = os.getenv("ARCHIVO_DATOS", "data/endesaAgregada")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "consumo_streaming")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Productor")

is_running = True

def signal_handler(sig, frame):
    global is_running
    logger.warning("Interrupción recibida. Cerrando...")
    is_running = False

signal.signal(signal.SIGINT, signal_handler)

def init_producer():
    logger.info(f"Conectando al broker Kafka en {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: str(v).encode('utf-8'),
            api_version=(0, 10, 1),
            retries=3
        )
        logger.info("Conexión establecida.")
        return producer
    except Exception as e:
        logger.error(f"Error al conectar con Kafka: {e}")
        exit(1)

def publish_data(producer):
    global is_running
    logger.info(f"Leyendo desde: {ARCHIVO_DATOS}")
    count = 0

    try:
        with open(ARCHIVO_DATOS, 'r', encoding='utf-8') as file:
            for line in file:
                if not is_running:
                    break

                line = line.strip()
                if not line:
                    continue

                try:
                    producer.send(KAFKA_TOPIC, value=line)
                    count += 1
                except KafkaError as e:
                    logger.error(f"Error al enviar mensaje: {e}")

                if count >= 50000:
                    logger.info("Límite de 50.000 registros alcanzado. Parando.")
                    break

                if count % 1000 == 0:
                    logger.info(f"Enviados {count} eventos al topic '{KAFKA_TOPIC}'...")
                    producer.flush()

                time.sleep(0.005)

    except FileNotFoundError:
        logger.error(f"No se encontró el fichero {ARCHIVO_DATOS}.")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
    finally:
        logger.info("Vaciando buffers...")
        producer.flush(timeout=5)
        producer.close()
        logger.info(f"Productor cerrado. Total enviados: {count}.")

if __name__ == "__main__":
    logger.info("--- Iniciando Productor Kafka ---")
    producer_instance = init_producer()
    publish_data(producer_instance)
