# Proyecto Final Big Data - Pipeline de Ingesta y Procesamiento en Streaming

Este repositorio contiene la infraestructura y el código fuente para el despliegue de una tubería de datos completa (*Data Pipeline*). El sistema está diseñado para leer conjuntos de datos masivos de consumo eléctrico mediante **Apache Kafka**, consumirlos en tiempo real con **Apache Spark Structured Streaming** y almacenarlos de forma persistente.

## 🎯 Meta Final del Proyecto

El objetivo de este proyecto es simular en un entorno local un clúster *Big Data* de producción completo:
1. **Zookeeper y Kafka Broker**: Orquestan y reciben los eventos (líneas de un dataset de 364MB) inyectados de forma secuencial sin hacer colapsar la RAM del host.
2. **Spark Master/Worker**: Procesan en Streaming los datos leídos de Kafka.
3. **Consumo y Transformación (Próximos Pasos)**: A través de Spark SQL o cuadernos en Apache Zeppelin, los datos se limpian, enriquecen y se almacenan en sistema de ficheros distribuido (HDFS o Parquet) para evidenciar su persistencia y permitir el análisis masivo.

## 📌 Estado Actual

**[✅] Fase 1: Infraestructura Docker y Monitorización Terminada**
- `docker-compose.yml` unificado levantando: Zookeeper, Kafka, Kafka-UI, Spark-Master, Spark-Worker y Zeppelin en red compartida (`bigdata-network`). 
- Limitador térmico de memoria JVM (`-Xmx512m` y `SPARK_DAEMON_MEMORY: 512m`) aplicado con éxito.
- Directorios persistentes `/notebooks`, `/data` y `/spark-home` correctamente enlazados y validados.

**[✅] Fase 2: Ingesta de Datos (Productor Kafka) Profesionalizada y Terminada**
- Script `productor.py` desarrollado en Python con técnica *Lazy Loading* y espaciado de peticiones (`time.sleep`) para lectura del CSV sin afectar a la RAM.
- Gestión de errores, métricas por Logger de consola y apagado seguro (*Graceful Shutdown*).
- Entorno virtual (`.venv`) y librerías testeadas. 

**[✅] Fase 3: Verificación Completa terminada**
- Panel visual de Kafka-UI operativo para auditar topics.
- Creado script `consumidor.py` de control que demuestra la circulación de eventos por el clúster.

**[⏳] Fase 4: Procesamiento de Spark (Asignado/Pendiente)**
- (En desarrollo por el compañero) Job de `Spark Structured Streaming` que leerá el topic *consumo_streaming*, hará la transformación y hundirá (Sink) en Parquet/HDFS.

---

## 🚀 Instrucciones de Despliegue y Ejecución

### 1. Requisitos Previos
- Tener instalado **Docker** y **Docker Compose**.
- Tener **Python 3** y crear un entorno virtual para las dependencias.

### 2. Preparar el Entorno Virtual (Python)
Para garantizar la compatibilidad (evitando errores de paquetes del sistema operativo) crea y activa un espacio virtual de Python:

```bash
# Crear entorno virtual (solo la primera vez)
python3 -m venv .venv

# Activar el entorno virtual en Linux/MacOS
source .venv/bin/activate

# Instalar librerías
pip install kafka-python
```

### 3. Levantar la Infraestructura (*Clúster*)
Enciende los servicios. La primera vez puede tardar un poco mientras descarga las imágenes.

```bash
docker compose up -d
```
Verifica que todo está "Up" comprobándolo con `docker compose ps`.

Podrás acceder a los paneles web en:
- Panel Kafka-UI (Monitor de clúster): [http://localhost:8082](http://localhost:8082)
- Apache Zeppelin: [http://localhost:8888](http://localhost:8888)
- Spark Master: [http://localhost:8080](http://localhost:8080)

### 4. Ejecutar el Simulador de Streaming
El día de la entrega, cambiar la variable global de `productor.py` a apuntar a `data/endesaAgregada`. 
Para probar el flujo, abre dos terminales (ambas con el entorno virtual activado):

**Terminal 1:** (Monitoriza qué llega al Broker)
```bash
python consumidor.py
```

**Terminal 2:** (Inicia la inyección de datos a la tubería)
```bash
python productor.py
```
> Si todo está correcto, la terminal 1 empezará a recibir flujos constantes de líneas CSV. Además, puedes entrar en [Kafka-UI (localhost:8082)](http://localhost:8082) y ver en vivo cómo entran los mensajes pulsando en la pestaña "Topics".

### 5. Apagar
Para apagar tu entorno sin borrar la configuración:
```bash
docker compose stop
```
(Para borrar por completo los contenedores y redes `docker compose down`)
