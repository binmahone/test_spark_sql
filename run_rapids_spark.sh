#!/bin/bash

# Configurable parameters
SPARK_CORES=${1:-10}  

# Set SPARK_HOME to the specified directory
export SPARK_HOME="/home/hongbin/develop/spark-3.2.1-bin-hadoop2.7"

# Add Spark bin directory to PATH
export PATH="$SPARK_HOME/bin:$PATH"

echo "Starting RAPIDS GPU-accelerated Spark Shell with custom script..."
echo "SPARK_HOME: $SPARK_HOME"
echo "Using $SPARK_CORES cores"
echo "Loading script: spark_example.scala"
echo "RAPIDS GPU acceleration enabled with celeborn shuffle"
echo "AQE disabled, fixed 300 shuffle partitions"
echo "Testing concurrentGpuTasks configurations:"
echo "  1. Dynamic (adaptive) concurrency"
echo "  2. Fixed values: 1, 3, 5, 7, 9"
echo ""

# Create log directory if it doesn't exist
mkdir -p log

# # First, test with dynamic concurrentGpuTasks
# echo "=========================================="
# echo "Testing with dynamic concurrentGpuTasks"
# echo "=========================================="

# Generate timestamp for unique log files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="log/rapids_driver_dynamic_${TIMESTAMP}.log"
GC_LOG_FILE="log/rapids_gc_dynamic_${TIMESTAMP}.log"


echo "Driver logs will be saved to: $LOG_FILE"
echo "GC logs will be saved to: $GC_LOG_FILE"
echo ""

# Run spark-shell with RAPIDS GPU acceleration and dynamic concurrentGpuTasks
spark-shell \
    --driver-memory 80g --master "local[$SPARK_CORES]"  \
    --conf spark.rapids.sql.concurrentGpuTasks.dynamic=true \
    --conf spark.celeborn.client.shuffle.compression.codec=zstd \
    --conf spark.io.compression.codec=zstd \
    --conf spark.rapids.memory.pinnedPool.size=20G \
    --conf spark.rapids.memory.host.offHeapLimit.enabled=true \
    --conf spark.rapids.memory.host.offHeapLimit.size=40G \
    --conf spark.sql.files.maxPartitionBytes=1g \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.log4j.logger.io.netty=DEBUG \
    --conf spark.rapids.sql.metrics.level='DEBUG' \
    --conf spark.eventLog.enabled=true \
    --conf spark.shuffle.manager=org.apache.spark.shuffle.celeborn.SparkShuffleManager \
    --conf spark.celeborn.master.endpoints=10.19.129.151:9097 \
    --conf spark.driver.extraJavaOptions="-Dai.rapids.cudf.nvtx.enabled=true -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:${GC_LOG_FILE} -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer -Dai.rapids.memory.bookkeep=true -Dai.rapids.memory.bookkeep.callstack=true" \
    --conf spark.rapids.sql.batchSizeBytes=1g \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.sql.adaptive.coalescePartitions.enabled=false \
    --conf spark.sql.shuffle.partitions=300 \
    --conf spark.rapids.flameGraph.pathPrefixXXX=/home/hongbin/data/flame \
    --conf spark.rapids.sql.asyncRead.shuffle.enabled=true \
    --conf spark.default.parallelism=20 \
    --jars /home/hongbin/develop/spark-3.2.1-bin-hadoop2.7/rapids_jars/2510_fresh.jar \
    --name "rapids-dynamic-cores$SPARK_CORES-2wave" \
    -i spark_example.scala 2>&1 | tee "$LOG_FILE"

echo ""
echo "Completed testing with dynamic concurrentGpuTasks"
echo "Log saved to: $LOG_FILE"
echo ""

# Optional: Add a brief pause before fixed concurrency tests
sleep 5

# Define concurrentGpuTasks values to test
CONCURRENT_TASKS_VALUES=(1)

# Loop through different concurrentGpuTasks values
for CONCURRENT_TASKS in "${CONCURRENT_TASKS_VALUES[@]}"; do
    echo "=========================================="
    echo "Testing with concurrentGpuTasks = $CONCURRENT_TASKS"
    echo "=========================================="
    
    # Generate timestamp for unique log files
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    LOG_FILE="log/rapids_driver_tasks${CONCURRENT_TASKS}_${TIMESTAMP}.log"
    GC_LOG_FILE="log/rapids_gc_tasks${CONCURRENT_TASKS}_${TIMESTAMP}.log"
    
    echo "Driver logs will be saved to: $LOG_FILE"
    echo "GC logs will be saved to: $GC_LOG_FILE"
    echo ""
    
    # Run spark-shell with RAPIDS GPU acceleration and optimized configurations
    # Redirect all output to log file
    spark-shell \
        --driver-memory 80g --master "local[$SPARK_CORES]"  \
        --conf spark.rapids.sql.concurrentGpuTasks=$CONCURRENT_TASKS \
        --conf spark.rapids.sql.concurrentGpuTasks.dynamic=false \
        --conf spark.celeborn.client.shuffle.compression.codec=zstd \
        --conf spark.io.compression.codec=zstd \
        --conf spark.rapids.memory.pinnedPool.size=20G \
        --conf spark.rapids.memory.host.offHeapLimit.enabled=true \
        --conf spark.rapids.memory.host.offHeapLimit.size=40G \
        --conf spark.sql.files.maxPartitionBytes=1g \
        --conf spark.plugins=com.nvidia.spark.SQLPlugin \
        --conf spark.log4j.logger.io.netty=DEBUG \
        --conf spark.rapids.sql.metrics.level='DEBUG' \
        --conf spark.eventLog.enabled=true \
        --conf spark.shuffle.manager=org.apache.spark.shuffle.celeborn.SparkShuffleManager \
        --conf spark.celeborn.master.endpoints=10.19.129.151:9097 \
        --conf spark.driver.extraJavaOptions="-Dai.rapids.cudf.nvtx.enabled=true -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:${GC_LOG_FILE} -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer -Dai.rapids.memory.bookkeep=true -Dai.rapids.memory.bookkeep.callstack=true" \
        --conf spark.rapids.sql.batchSizeBytes=1g \
        --conf spark.sql.adaptive.enabled=false \
        --conf spark.sql.adaptive.coalescePartitions.enabled=false \
        --conf spark.sql.shuffle.partitions=300 \
        --conf spark.rapids.flameGraph.pathPrefixXXX=/home/hongbin/data/flame \
        --conf spark.rapids.sql.asyncRead.shuffle.enabled=true \
        --conf spark.default.parallelism=20 \
        --jars /home/hongbin/develop/spark-3.2.1-bin-hadoop2.7/rapids_jars/2510_fresh.jar \
        --name "rapids-fixed-gpu${CONCURRENT_TASKS}-cores$SPARK_CORES-2wave" \
        -i spark_example.scala 2>&1 | tee "$LOG_FILE"
    
    echo ""
    echo "Completed testing with concurrentGpuTasks = $CONCURRENT_TASKS"
    echo "Log saved to: $LOG_FILE"
    echo ""
    
    # Optional: Add a brief pause between runs
    sleep 5
done

echo "=========================================="
echo "All concurrentGpuTasks tests completed!"
echo "Tests performed:"
echo "  ✓ Dynamic concurrency (adaptive)"
echo "  ✓ Fixed concurrency: 1, 3, 5, 7, 9"
echo "=========================================="


