#!/usr/bin/env bash
# setup_colab.sh
# Run once at the top of a Colab session to prepare the full HyperLogistics-Stream environment.
# Usage (inside a Colab cell):
#   !bash scripts/setup_colab.sh

set -e

KAFKA_VERSION="3.7.0"
SCALA_VERSION="2.13"
KAFKA_DIR="/opt/kafka"
KAFKA_TARBALL="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TARBALL}"

echo "========================================"
echo "  HyperLogistics-Stream: Colab Setup"
echo "========================================"

# --- 1. Java 11 ---
echo "[1/5] Installing Java 11..."
apt-get install -y -qq openjdk-11-jdk-headless
export JAVA_HOME=$(readlink -f /usr/bin/java | sed 's|/bin/java||')
echo "  JAVA_HOME=$JAVA_HOME"
java -version

# --- 2. Python packages ---
echo "[2/5] Installing Python packages..."
pip install -q \
    "pyspark==3.5.0" \
    "confluent-kafka>=2.3.0" \
    "fastapi>=0.111.0" \
    "uvicorn[standard]>=0.29.0" \
    "pydeck>=0.9.0" \
    "plotly>=5.20.0" \
    "pyarrow>=14.0.0" \
    "pyyaml>=6.0" \
    "tqdm>=4.66.0" \
    "tabulate>=0.9.0" \
    "tzdata>=2024.1"
echo "  Python packages installed."

# --- 3. Kafka binary (no Docker needed) ---
echo "[3/5] Setting up Kafka ${KAFKA_VERSION}..."
if [ ! -d "$KAFKA_DIR" ]; then
    mkdir -p /opt
    cd /tmp
    if [ ! -f "$KAFKA_TARBALL" ]; then
        echo "  Downloading Kafka..."
        wget -q "$KAFKA_URL" -O "$KAFKA_TARBALL"
    fi
    tar -xzf "$KAFKA_TARBALL" -C /opt
    mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} $KAFKA_DIR
    echo "  Kafka extracted to $KAFKA_DIR"
else
    echo "  Kafka already installed at $KAFKA_DIR"
fi

# --- 4. Start Zookeeper ---
echo "[4/5] Starting Zookeeper..."
$KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
sleep 4
echo "  Zookeeper started."

# --- 5. Start Kafka broker ---
echo "[5/5] Starting Kafka broker..."
# Set max message size and log retention for Colab session
cat >> $KAFKA_DIR/config/server.properties <<EOF

# HyperLogistics-Stream tuning
message.max.bytes=10485760
log.retention.hours=2
num.partitions=8
auto.create.topics.enable=true
EOF
$KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties
sleep 6

# Create flights_stream topic
$KAFKA_DIR/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic flights_stream \
    --partitions 8 \
    --replication-factor 1

echo ""
echo "========================================"
echo "  Setup complete!"
echo "  Kafka:      localhost:9092"
echo "  JAVA_HOME:  $JAVA_HOME"
echo "  PySpark:    $(python -c 'import pyspark; print(pyspark.__version__)')"
echo "========================================"
echo ""
echo "Next: mount Google Drive and set COLAB_ROOT to your Drive path."
