# Spark Scala Large Dataset Analysis

This directory contains a Spark Scala script for testing query optimization with large datasets.

## Files

- `spark_example.scala` - Main Scala script for large dataset analysis (10M rows)
- `run_spark.sh` - Shell script to run Spark with 40GB memory configuration
- `run_rapids_spark.sh` - Shell script to run RAPIDS GPU-accelerated Spark with celeborn shuffle
- `scalastyle-config.xml` - Scala style configuration (100 char line limit)

## Usage

### Method 1: Using the regular CPU Spark shell script
```bash
./run_spark.sh
```

### Method 2: Using RAPIDS GPU-accelerated Spark (Recommended for GPU systems)
```bash
./run_rapids_spark.sh
```

### Method 3: Direct spark-shell command
```bash
export SPARK_HOME="/home/hongbin/develop/spark-3.2.1-bin-hadoop2.7"
export PATH="$SPARK_HOME/bin:$PATH"
spark-shell -i spark_example.scala
```

### Method 4: Load script interactively in Spark Shell
```bash
export SPARK_HOME="/home/hongbin/develop/spark-3.2.1-bin-hadoop2.7"
export PATH="$SPARK_HOME/bin:$PATH"
spark-shell
```

Then in the Spark Shell:
```scala
:load spark_example.scala
```

## What the script does

The script demonstrates large-scale Spark SQL operations:

1. **Data Generation**: Creates 100 million rows using `spark.range()`
2. **Column Design**: 
   - High cardinality column `a` (10M distinct values)
   - Low cardinality column `b` (100 distinct values)
   - 80 measure columns with deterministic values for aggregation
3. **Two-Stage Aggregation**:
   - First: `GROUP BY a, b` with detailed metrics
   - Second: `GROUP BY b` with conditional aggregations
4. **Query Optimization Testing**: Prevents optimizer from merging group by operations
5. **Performance Analysis**: Times each operation and shows execution plans

## Key Features

- **Large Scale**: 10 million row dataset with 80 measure columns + 2 string dimensions
- **SQL-First Approach**: Uses Spark SQL instead of DataFrame API  
- **GPU Acceleration**: RAPIDS GPU support for massive performance improvements
- **Query Optimization**: Tests multi-stage aggregations with CTE to prevent merging
- **Performance Metrics**: Detailed timing and execution plan analysis
- **Conditional Aggregations**: Uses `CASE WHEN` to prevent query merging
- **Advanced Shuffle**: Celeborn shuffle manager with ZSTD compression
- **Memory Optimization**: 50GB driver memory with 40GB off-heap GPU memory

## Requirements

### For CPU-only execution:
- Spark 3.2.1 installed at `/home/hongbin/develop/spark-3.2.1-bin-hadoop2.7`
- Java 8 or 11 (required by Spark)
- Scala 2.12 (comes with Spark)
- 40GB+ RAM for driver memory

### For RAPIDS GPU acceleration:
- NVIDIA GPU with CUDA capability 6.0+ (Pascal, Volta, Turing, Ampere, Ada, Hopper)
- CUDA 11.0+ runtime libraries
- RAPIDS Spark plugin jar at `/home/hongbin/develop/spark-3.2.1-bin-hadoop2.7/rapids_jars/2510_fresh.jar`
- Celeborn shuffle service running on `10.19.129.151:9097`
- 50GB+ RAM + 40GB+ GPU memory
- AQE (Adaptive Query Execution) disabled for consistent performance testing
- Fixed 3000 shuffle partitions for predictable parallelism

## Performance Notes

### CPU Performance:
- Data generation: ~1.4 seconds (10M rows)
- Combined aggregation: ~26-50 seconds depending on hardware
- Total execution: ~50-95 seconds
- Memory usage: ~40GB driver memory

### GPU Performance (Expected with RAPIDS):
- Data generation: ~1.4 seconds (same as CPU)
- Combined aggregation: ~5-15 seconds (3-10x speedup)
- Total execution: ~10-25 seconds (2-5x overall speedup)
- Memory usage: 50GB RAM + 40GB GPU memory

### Query Optimization:
- Query plans will show whether two-level aggregations were preserved
- CTE structure prevents optimizer from merging GROUP BY operations
- Celeborn shuffle provides better performance than default shuffle

## Code Style

The code follows the Scala style guidelines defined in `scalastyle-config.xml`:
- Maximum line length: 100 characters
- Standard Scala naming conventions
- Proper indentation and formatting
