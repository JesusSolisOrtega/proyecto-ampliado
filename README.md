# Proyecto Final Big Data - Pipeline de Ingesta y Procesamiento en Streaming

Este repositorio contiene la infraestructura y el código fuente para el despliegue de una tubería de datos completa (*Data Pipeline*). El sistema está diseñado para leer conjuntos de datos masivos de consumo eléctrico mediante **Apache Kafka**, consumirlos en tiempo real con **Apache Spark Structured Streaming**, transformarlos y almacenarlos de forma persistente en formato columnar **Parquet**.

## 🎯 Meta Final del Proyecto

Simular en un entorno local un clúster *Big Data* de producción completo:
1. **Zookeeper y Kafka Broker**: Orquestan y reciben los eventos (líneas de un dataset de 364MB) inyectados de forma secuencial sin colapsar la RAM del host.
2. **Spark Master/Worker**: Procesan en Streaming los datos leídos de Kafka, aplican transformaciones y calculan agregados.
3. **Persistencia en Parquet**: Los datos transformados se almacenan en formato columnar particionado por año y mes, listos para análisis con Spark SQL o Apache Zeppelin.

## 📌 Estado Actual

**[✅] Fase 1: Infraestructura Docker y Monitorización**
- `docker-compose.yml` unificado levantando: Zookeeper, Kafka, Kafka-UI, Spark-Master, Spark-Worker y Zeppelin en red compartida (`bigdata-network`).
- Limitadores de memoria JVM (`-Xmx512m` y `SPARK_DAEMON_MEMORY: 512m`) aplicados.
- Directorios persistentes `/notebooks`, `/data` y `/spark-home` correctamente enlazados.

**[✅] Fase 2: Ingesta de Datos (Productor Kafka)**
- Script `productor.py` con técnica *Lazy Loading* y throttling (`time.sleep(0.05)`) para lectura del CSV sin afectar a la RAM.
- Gestión de errores, métricas por Logger de consola y apagado seguro (*Graceful Shutdown*).

**[✅] Fase 3: Verificación del Pipeline**
- Panel visual de Kafka-UI operativo para auditar topics.
- Script `consumidor.py` de control que demuestra la circulación de eventos por el clúster.

**[✅] Fase 4: Procesamiento Spark Structured Streaming**
- Script `spark_streaming_job.py` que lee del topic `consumo_streaming`, parsea las líneas CSV, calcula consumos totales diarios y persiste en Parquet particionado por año y mes en `data/parquet_output/`.
- Notebook de Apache Zeppelin con 8 queries analíticas sobre los datos persistidos.

---

## 🚀 Arranque Rápido

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

#### 1. Requisitos Previos
- **Docker** y **Docker Compose** instalados.
- **Python 3** con entorno virtual.

#### 2. Preparar el Entorno Virtual (Python)

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows Git Bash
source .venv/bin/activate        # Linux/macOS
pip install kafka-python
```

#### 3. Levantar el Clúster

```bash
docker compose up -d
docker compose ps    # verificar que todo está "Up"
```

Paneles web disponibles:
- **Kafka-UI**: http://localhost:8082
- **Spark Master UI**: http://localhost:8080
- **Apache Zeppelin**: http://localhost:8888

#### 4. Crear el Topic de Kafka (primera vez)

> En Windows con Git Bash añadir `MSYS_NO_PATHCONV=1` delante de cada comando `docker exec`.

```bash
MSYS_NO_PATHCONV=1 docker exec kafka kafka-topics --create --if-not-exists --topic consumo_streaming --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#### 5. Lanzar el Job de Spark (Terminal 1)

```bash
MSYS_NO_PATHCONV=1 docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /app/spark_streaming_job.py
```

> La primera ejecución descarga el conector Kafka (~60MB desde Maven). Las siguientes usan caché.

Ver el log (si se lanzó en background):
```bash
tail -f logs/spark_job.log
```

Parar el job manualmente:
```bash
MSYS_NO_PATHCONV=1 docker exec spark-master pkill -f spark_streaming_job.py
```

#### 6. Lanzar el Productor (Terminal 2)

```bash
python productor.py
```

> Para la entrega final cambiar `ARCHIVO_DATOS = "data/endesaAgregada"` en `productor.py`.

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

Abrir http://localhost:8888 y ejecutar el notebook **Pipeline Consumo Electrico - Evidencia End-to-End**. Contiene 8 queries analíticas sobre los datos persistidos: consumo por tarifa, top provincias, curva de carga horaria, ratio reactivo/activo, etc.

#### 9. Apagar

```bash
bash stop.sh                  # Para el Spark job y el cluster
docker compose down           # Elimina también contenedores y redes
```

---

## 📁 Estructura del Repositorio

```
ProyectoAmpliadoBigData/
├── docker-compose.yml              # Orquestación del clúster completo
├── start.sh                        # Script de arranque automático
├── stop.sh                         # Script de parada limpia
├── productor.py                    # Productor Kafka (lazy loading desde CSV)
├── consumidor.py                   # Consumidor de verificación
├── spark_streaming_job.py          # Job Spark Structured Streaming (Fase 4)
├── data/
│   └── endesa_streaming_dev.csv    # Dataset de desarrollo (1000 filas)
└── notebooks/
    └── Pipeline_Consumo_Electrico.zpln   # Notebook Zeppelin con queries analíticas
```
