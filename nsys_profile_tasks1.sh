#!/bin/bash

# NSys profiling script for CONCURRENT_TASKS=3

# Set SPARK_HOME to the specified directory
export SPARK_HOME="/home/hongbin/develop/spark-3.2.1-bin-hadoop2.7"
export PATH="$SPARK_HOME/bin:$PATH"

# Create log and profiling output directories
mkdir -p log
mkdir -p nsys_profiles

# Generate timestamp for unique files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
CONCURRENT_TASKS=1
LOG_FILE="log/rapids_driver_tasks${CONCURRENT_TASKS}_nsys_${TIMESTAMP}.log"
GC_LOG_FILE="log/rapids_gc_tasks${CONCURRENT_TASKS}_nsys_${TIMESTAMP}.log"
NSYS_OUTPUT="nsys_profiles/rapids_tasks${CONCURRENT_TASKS}_${TIMESTAMP}"

echo "=========================================="
echo "NSys Profiling with concurrentGpuTasks = $CONCURRENT_TASKS"
echo "=========================================="
echo "Driver logs will be saved to: $LOG_FILE"
echo "GC logs will be saved to: $GC_LOG_FILE"  
echo "NSys profile will be saved to: ${NSYS_OUTPUT}.nsys-rep"
echo ""

# Run nsys profile with the complete spark-shell command
nsys profile \
    --output="${NSYS_OUTPUT}" \
    --force-overwrite=true \
    bash -c "
        spark-shell \
        --driver-memory 80g --master 'local[10]'  \
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
        --conf spark.driver.extraJavaOptions=\"-Dai.rapids.cudf.nvtx.enabled=true -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:${GC_LOG_FILE} -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer -Dai.rapids.memory.bookkeep=true -Dai.rapids.memory.bookkeep.callstack=true\" \
        --conf spark.rapids.sql.batchSizeBytes=1g \
        --conf spark.sql.adaptive.enabled=false \
        --conf spark.sql.adaptive.coalescePartitions.enabled=false \
        --conf spark.sql.shuffle.partitions=300 \
        --conf spark.rapids.flameGraph.pathPrefixXXX=/home/hongbin/data/flame \
        --conf spark.rapids.sql.asyncRead.shuffle.enabled=true \
        --jars /home/hongbin/develop/spark-3.2.1-bin-hadoop2.7/rapids_jars/2510_fresh.jar \
        --name \"rapids-fixed-gpu${CONCURRENT_TASKS}-cores$SPARK_CORES-nsys\" \
        -i spark_example.scala 2>&1 | tee \"$LOG_FILE\"
    "

echo ""
echo "=========================================="
echo "NSys profiling completed for concurrentGpuTasks = $CONCURRENT_TASKS"
echo "Profile saved to: ${NSYS_OUTPUT}.nsys-rep"
echo "Log saved to: $LOG_FILE"
echo "=========================================="
