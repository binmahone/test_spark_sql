# Spark Event Log Analysis - Stage Execution Times

## First Test Run - Total Time Across All Tasks (seconds)

| App Name (App ID)                   | Stage 0 | Stage 1 | Stage 2 | Stage 3 |   Total |
|-------------------------------------|---------|---------|---------|---------|---------|
| rapids-dynamic (local-1755762828536) |  454.19 |  489.03 |   90.61 |    0.52 | 1034.35 |
| rapids-tasks8 (local-1755763482886) |  916.28 |  424.79 |   92.44 |    0.42 | 1433.93 |
| rapids-tasks4 (local-1755763384003) |  464.40 |  407.13 |   84.02 |    0.52 |  956.07 |
| rapids-tasks2 (local-1755763286834) |  387.44 |  440.94 |  116.05 |    0.55 |  944.98 |
| rapids-tasks1 (local-1755763188342) |  359.97 |  495.42 |  121.05 |    0.45 |  976.89 |


## Latest Test Run - Total Time Across All Tasks (seconds)

| App Name (App ID)                   | Stage 0 | Stage 1 | Stage 2 | Stage 3 |   Total |
|-------------------------------------|---------|---------|---------|---------|---------|
| rapids-dynamic (local-1755766683604) |  454.55 |  537.47 |   86.80 |    0.53 | 1079.35 |
| rapids-tasks8 (local-1755765175032) | 1018.21 |  366.60 |   94.06 |    0.36 | 1479.23 |
| rapids-tasks4 (local-1755765069149) |  585.51 |  395.47 |   82.17 |    0.32 | 1063.47 |
| rapids-tasks2 (local-1755764973060) |  393.94 |  406.99 |   98.55 |    0.37 |  899.85 |
| rapids-tasks1 (local-1755764874993) |  360.07 |  484.31 |  121.82 |    0.53 |  966.73 |


## maxConcurrentGpuTasks for each stage in rapids-dynamic

https://github.com/NVIDIA/spark-rapids/pull/13359 empowered us to know how big concurrentGpuTasks each
stage reached, which we can observe from the accumulator called `maxConcurrentGpuTasks`

For rapids-dynamic, the maxConcurrentGpuTasks for each stage is : 4, 20, 20, 1 respectively.