#!/bin/bash

# Set SPARK_HOME to the specified directory
export SPARK_HOME="/home/hongbin/develop/spark-3.2.1-bin-hadoop2.7"

# Add Spark bin directory to PATH
export PATH="$SPARK_HOME/bin:$PATH"

echo "Starting Spark Shell with custom script..."
echo "SPARK_HOME: $SPARK_HOME"
echo "Loading script: spark_example.scala"
echo ""

# Run spark-shell with the Scala script and increased memory
spark-shell --master 'local[20]' --driver-memory 40g --executor-memory 40g --conf spark.eventLog.enabled=true -i spark_example.scala
