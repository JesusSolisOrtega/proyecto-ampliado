import os
import time
import json
import signal
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# =========================================================
# CONFIGURACIÓN GLOBAL (Mejorado con SO Environs)
# =========================================================
# Si el entorno tiene declarada ARCHIVO_DATOS la usa, si no, usa el CSV por defecto.
ARCHIVO_DATOS = os.getenv("ARCHIVO_DATOS", "data/endesa_streaming_dev.csv")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "consumo_streaming")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# =========================================================
# CONFIGURACIÓN DE LOGGER (Nivel Profesional)
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("KafkaProducer")

# Control de ejecución
corriendo = True

def manejador_senales(sig, frame):
    """
    Captura Ctrl+C para un apagado limpio (Graceful Shutdown)
    """
    global corriendo
    logger.warning("Señal de interrupción recibida. Iniciando apagado seguro del inyector...")
    corriendo = False

signal.signal(signal.SIGINT, manejador_senales)

def inicializar_productor():
    """
    Inicializa y devuelve el productor de Kafka.
    """
    logger.info(f"Conectando al Broker de Kafka en {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: str(v).encode('utf-8'),
            api_version=(0, 10, 1),
            retries=3  # Resiliencia: si falla, reintenta 3 veces
        )
        logger.info("Conexión establecida con éxito.")
        return producer
    except Exception as e:
        logger.error(f"Fallo crítico al conectar con Kafka: {e}")
        exit(1)

def publicar_datos(producer):
    """
    Lee el archivo CSV en modo Lazy Loading de forma segura.
    """
    global corriendo
    logger.info(f"Iniciando lectura desde: {ARCHIVO_DATOS}")
    contador = 0
    
    try:
        with open(ARCHIVO_DATOS, 'r', encoding='utf-8') as archivo:
            for linea in archivo:
                if not corriendo:
                    logger.info("Válvula de inyección cortada tempranamente por el usuario.")
                    break
                
                linea = linea.strip()
                if not linea:
                    continue
                
                try:
                    # Envío asíncrono optimizado
                    producer.send(KAFKA_TOPIC, value=linea)
                    contador += 1
                except KafkaError as e:
                    logger.error(f"Error al enviar mensaje a Kafka: {e}")
                
                if contador % 100 == 0:
                    logger.info(f"Subidos {contador} eventos a '{KAFKA_TOPIC}'...")
                
                # CRITICO - ANTI CRASH: Pausa para no saturar la RAM
                time.sleep(0.05)
                
    except FileNotFoundError:
        logger.error(f"El dataset {ARCHIVO_DATOS} no existe.")
    except Exception as e:
        logger.error(f"Error inesperado en lectura: {e}")
    finally:
        logger.info("Aplicando flush (vaciado seguro de buffers) en Kafka...")
        producer.flush(timeout=5)
        producer.close()
        logger.info(f"✔ Proceso cerrado limpiamente. Total inyectado: {contador} registros.")

if __name__ == "__main__":
    logger.info("--- Iniciando Servicio de Producción Eléctrica ---")
    test_producer = inicializar_productor()
    publicar_datos(test_producer)
