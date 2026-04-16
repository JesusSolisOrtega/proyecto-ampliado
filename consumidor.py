import os
import signal
import logging
from kafka import KafkaConsumer

# =========================================================
# CONFIGURACIÓN GLOBAL
# =========================================================
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "consumo_streaming")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# =========================================================
# CONFIGURACIÓN DE LOGGER
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("KafkaConsumer")

corriendo = True

def manejador_senales(sig, frame):
    global corriendo
    logger.warning("Señal de cierre recibida. Deteniendo escucha...")
    corriendo = False

signal.signal(signal.SIGINT, manejador_senales)

def inicializar_consumidor():
    """
    Inicializa y devuelve el consumidor de Kafka.
    """
    logger.info(f"Conectando Consumidor al broker en {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: v.decode('utf-8'),
            auto_offset_reset='latest', # Solo queremos ver lo vivo actualmente
            enable_auto_commit=True,
            group_id='grupo_verificacion_dev',
            api_version=(0, 10, 1)
        )
        logger.info("Consumidor suscrito exitosamente al topic.")
        return consumer
    except Exception as e:
        logger.error(f"Fallo al arrancar el Consumidor: {e}")
        exit(1)

def escuchar_mensajes(consumer):
    """
    Escucha de forma continua los mensajes del topic usando poll seguro.
    """
    global corriendo
    logger.info(f"Esperando lecturas en '{KAFKA_TOPIC}'...")
    logger.info("[Pulsa Ctrl+C para salir limpiamente]")
    
    contador = 0
    try:
        while corriendo:
            # poll con timeout, no bloquea permanentemente
            mensajes_batch = consumer.poll(timeout_ms=1000)
            
            for particion, mensajes in mensajes_batch.items():
                for mensaje in mensajes:
                    if not corriendo:
                        break
                    logger.info(f"-> RECIBIDO {particion}: {mensaje.value}")
                    contador += 1
                    
    except Exception as e:
        logger.error(f"Excepción en la etapa de lectura: {e}")
    finally:
        logger.info(f"Sesión cerrada. Eventos interceptados en esta prueba: {contador}")
        consumer.close()

if __name__ == "__main__":
    logger.info("--- Iniciando Monitor Analítico de Kafka ---")
    test_consumer = inicializar_consumidor()
    escuchar_mensajes(test_consumer)
