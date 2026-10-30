#!/usr/bin/env bash
# setup_colab.sh — Run once at the top of a Colab session.
# Usage (inside a Colab cell):  !bash scripts/setup_colab.sh

KAFKA_VERSION="3.7.0"
SCALA_VERSION="2.13"
KAFKA_DIR="/opt/kafka"
KAFKA_TARBALL="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TARBALL}"

echo "========================================"
echo "  HyperLogistics-Stream: Colab Setup"
echo "========================================"

echo "[1/5] Checking Java..."
java -version 2>&1 | head -1
export JAVA_HOME=$(readlink -f /usr/bin/java | sed 's|/bin/java||')
echo "  JAVA_HOME=$JAVA_HOME"

echo "[2/5] Installing Python packages..."
pip install -q \
    "pyspark==3.5.0" \
    "confluent-kafka>=2.3.0" \
    "fastapi>=0.111.0" \
    "uvicorn[standard]>=0.29.0" \
    "pydeck>=0.9.0" \
    "plotly>=5.20.0" \
    "pyarrow>=14.0.0,<15.0.0" \
    "pyyaml>=6.0" \
    "tqdm>=4.66.0" \
    "tabulate>=0.9.0" \
    "tzdata>=2024.1"
echo "  Python packages installed."

echo "[3/5] Setting up Kafka ${KAFKA_VERSION}..."
if [ ! -d "$KAFKA_DIR" ]; then
    mkdir -p /opt /tmp/kafka_dl
    cd /tmp/kafka_dl

    echo "  Downloading Kafka (113 MB)..."
    wget -q --show-progress "$KAFKA_URL" -O "$KAFKA_TARBALL"

    if ! gzip -t "$KAFKA_TARBALL" 2>/dev/null; then
        echo "ERROR: Downloaded tarball is corrupt. Re-run the cell." >&2
        rm -f "$KAFKA_TARBALL"
        exit 1
    fi

    tar -xzf "$KAFKA_TARBALL" -C /opt
    mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} $KAFKA_DIR
    echo "  Kafka extracted to $KAFKA_DIR"
else
    echo "  Kafka already installed at $KAFKA_DIR"
fi

echo "[4/5] Starting Zookeeper..."
$KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
sleep 4

for i in $(seq 1 10); do
    if nc -z localhost 2181 2>/dev/null; then
        echo "  Zookeeper ready (attempt $i)."
        break
    fi
    sleep 2
done

echo "[5/5] Starting Kafka broker..."
cat >> $KAFKA_DIR/config/server.properties <<'EOF'

# HyperLogistics-Stream tuning
message.max.bytes=10485760
log.retention.hours=2
num.partitions=8
auto.create.topics.enable=true
EOF

$KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties

echo "  Waiting for Kafka broker..."
READY=false
for i in $(seq 1 30); do
    if $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        READY=true
        echo "  Kafka ready (attempt $i)."
        break
    fi
    sleep 2
done

if [ "$READY" = false ]; then
    echo "ERROR: Kafka did not become ready within 60 s. Check /opt/kafka/logs/server.log" >&2
    exit 1
fi

$KAFKA_DIR/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic flights_stream \
    --partitions 8 \
    --replication-factor 1 \
    && echo "  Topic flights_stream created."

echo ""
echo "========================================"
echo "  Setup complete!"
echo "  Kafka:      localhost:9092"
echo "  JAVA_HOME:  $JAVA_HOME"
echo "  PySpark:    $(python -c 'import pyspark; print(pyspark.__version__)')"
echo "  PyArrow:    $(python -c 'import pyarrow; print(pyarrow.__version__)')"
echo "========================================"
echo ""
echo "Next: mount Google Drive, then run pipeline/01_batch_train.py"
