# Proyecto Final Big Data - Pipeline de Ingesta y Procesamiento en Streaming

Pipeline completo para procesar datos de consumo eléctrico en tiempo real. Lee el dataset Endesa Agregada (364 MB) línea a línea, lo envía a un topic de Kafka, y Spark Structured Streaming lo consume, transforma y persiste en Parquet.

## Estado del proyecto

**[Completado] Fase 1: Infraestructura Docker**
- `docker-compose.yml` con Zookeeper, Kafka, Kafka-UI, Spark Master, Spark Worker y Zeppelin en red compartida (`bigdata-network`).
- Límites de memoria JVM configurados para ejecutar en 16 GB de RAM.

**[Completado] Fase 2: Ingesta de datos (Productor Kafka)**
- `productor.py` lee el CSV línea a línea (sin cargarlo en memoria) y envía cada registro al topic.
- Throttling de 5ms entre envíos, flush cada 1000 mensajes, cierre limpio con Ctrl+C.

**[Completado] Fase 3: Verificación del pipeline**
- Kafka-UI en http://localhost:8082 para monitorizar el topic en tiempo real.
- `consumidor.py` para comprobar que los mensajes llegan correctamente al broker.

**[Completado] Fase 4: Procesamiento Spark Structured Streaming**
- `spark_streaming_job.py` lee del topic `consumo_streaming`, parsea las líneas CSV, calcula consumos totales y persiste en Parquet particionado por año y mes en `data/parquet_output/`.
- Notebook de Apache Zeppelin con 11 queries analíticas sobre los datos persistidos.

---

## Arranque rápido

### Opción A — Script automático (recomendado)

```bash
# Instalar dependencias Python (solo la primera vez)
python -m venv .venv
source .venv/Scripts/activate   # Windows Git Bash
pip install kafka-python

# Arrancar todo el pipeline
bash start.sh

# En la misma terminal, lanzar el productor cuando start.sh lo indique
python productor.py
```

```bash
# Parar todo limpiamente
bash stop.sh
```

---

### Opción B — Paso a paso manual

#### 1. Requisitos previos
- Docker y Docker Compose instalados.
- Python 3 con entorno virtual.

#### 2. Preparar el entorno virtual

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows Git Bash
source .venv/bin/activate        # Linux/macOS
pip install kafka-python
```

#### 3. Levantar el clúster

```bash
docker compose up -d
docker compose ps    # verificar que todo está "Up"
```

Paneles web disponibles:
- Kafka-UI: http://localhost:8082
- Spark Master UI: http://localhost:8080
- Apache Zeppelin: http://localhost:8888

#### 4. Crear el topic de Kafka (primera vez)

> En Windows con Git Bash añadir `MSYS_NO_PATHCONV=1` delante de cada comando `docker exec`.

```bash
MSYS_NO_PATHCONV=1 docker exec kafka kafka-topics --create --if-not-exists --topic consumo_streaming --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#### 5. Lanzar el job de Spark (Terminal 1)

```bash
MSYS_NO_PATHCONV=1 docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /app/spark_streaming_job.py
```

> La primera ejecución descarga el conector Kafka (~60 MB desde Maven). Las siguientes usan caché local.

Ver el log (si se lanzó en background con start.sh):
```bash
tail -f logs/spark_job.log
```

Parar el job manualmente:
```bash
MSYS_NO_PATHCONV=1 docker exec spark-master pkill -f spark_streaming_job.py
```

#### 6. Lanzar el productor (Terminal 2)

```bash
python productor.py
```

El productor envía hasta 50.000 registros del dataset real (`data/endesaAgregada`) con 5ms de retardo entre envíos. El proceso tarda aproximadamente 4 minutos.

#### 7. Verificar los Parquet generados

Spark escribe en `data/parquet_output/` particionado por año y mes:
```
data/parquet_output/
├── anyo=2014/
│   ├── mes=09/  part-*.snappy.parquet
│   ├── mes=10/  part-*.snappy.parquet
│   └── ...
└── anyo=2015/
    └── mes=01/  part-*.snappy.parquet
```

Columnas por registro: `cups`, `periodo`, `anyo`, `mes`, `tarifa`, `provincia`, `municipio`, `h1`..`h24` (consumo activo por hora en Wh), `r1`..`r24` (consumo reactivo por hora en VARh), `consumo_activo_total_wh`, `consumo_reactivo_total_varh`.

#### 8. Análisis con Apache Zeppelin

Abrir http://localhost:8888 y ejecutar el notebook **Pipeline Consumo Electrico - Evidencia End-to-End**. Contiene 11 queries sobre los datos persistidos: consumo por mes, comparativa de tarifas, top provincias, curva de carga horaria, top consumidores, ratio reactivo/activo, contadores inactivos, pico y valle por hora, y segmentación por tramos.

#### 9. Apagar

```bash
bash stop.sh                  # Para el Spark job y el cluster
docker compose down           # Elimina también los contenedores y la red
```

---

## Estructura del repositorio

```
ProyectoAmpliadoBigData/
├── docker-compose.yml              # Orquestación del clúster completo
├── start.sh                        # Arranque automático del pipeline
├── stop.sh                         # Parada limpia
├── productor.py                    # Productor Kafka (lectura línea a línea del CSV)
├── consumidor.py                   # Consumidor de verificación
├── spark_streaming_job.py          # Job Spark Structured Streaming
├── data/
│   └── endesaAgregada              # Dataset real (364 MB, 1.17M registros)
└── notebooks/
    └── Pipeline_Consumo_Electrico.zpln   # Notebook Zeppelin con queries analíticas
```
