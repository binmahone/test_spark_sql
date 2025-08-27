// Spark Scala Script for Large Dataset Analysis - Direct Two-Level Aggregation
// This script generates 100M rows and tests direct DataFrame aggregation
// Usage: spark-shell -i spark_example.scala

println("=== Starting Large Dataset Generation ===")

// Generate 100 million rows using spark.range (reduced for memory)
val numRows = 200000000L
println(s"Generating $numRows rows...")

val startTime = System.currentTimeMillis()

// Create base dataset with spark.range
val baseDF = spark.range(numRows)

// Create synthetic data with high and low cardinality columns
val syntheticDF = baseDF.selectExpr(
  "id",
  // High cardinality column 'a' - string format with prefix
  "CONCAT('DIM_A_', CAST(id % 100000000 AS STRING)) as a",
  // Low cardinality column 'b' - string format with prefix
  "CONCAT('CAT_B_', CAST(id % 100 AS STRING)) as b",
  // 80 measure columns with deterministic increments
  "CAST((id + 1) % 1000 AS DOUBLE) as measure1",
  "CAST((id + 10) % 500 AS DOUBLE) as measure2", 
  "CAST((id + 100) % 200 AS INT) as measure3",
  "CAST((id + 1000) % 10000 AS DOUBLE) as measure4",
  "CAST((id + 10000) % 50 AS DOUBLE) as measure5",
  "CAST((id + 100000) % 1000000 AS LONG) as measure6",
  "CAST((id + 1000000) % 100 AS INT) as measure7",
  "CAST((id + 10000000) % 2000 AS DOUBLE) as measure8",
  "CAST((id + 2) % 800 AS DOUBLE) as measure9",
  "CAST((id + 20) % 400 AS DOUBLE) as measure10",
  "CAST((id + 200) % 150 AS INT) as measure11",
  "CAST((id + 2000) % 8000 AS DOUBLE) as measure12",
  "CAST((id + 20000) % 40 AS DOUBLE) as measure13",
  "CAST((id + 200000) % 800000 AS LONG) as measure14",
  "CAST((id + 2000000) % 80 AS INT) as measure15",
  "CAST((id + 20000000) % 1600 AS DOUBLE) as measure16",
  "CAST((id + 3) % 1200 AS DOUBLE) as measure17",
  "CAST((id + 30) % 600 AS DOUBLE) as measure18",
  "CAST((id + 300) % 250 AS INT) as measure19",
  "CAST((id + 3000) % 12000 AS DOUBLE) as measure20",
  "CAST((id + 30000) % 60 AS DOUBLE) as measure21",
  "CAST((id + 300000) % 1200000 AS LONG) as measure22",
  "CAST((id + 3000000) % 120 AS INT) as measure23",
  "CAST((id + 30000000) % 2400 AS DOUBLE) as measure24",
  "CAST((id + 4) % 1400 AS DOUBLE) as measure25",
  "CAST((id + 40) % 700 AS DOUBLE) as measure26",
  "CAST((id + 400) % 350 AS INT) as measure27",
  "CAST((id + 4000) % 14000 AS DOUBLE) as measure28",
  "CAST((id + 40000) % 70 AS DOUBLE) as measure29",
  "CAST((id + 400000) % 1400000 AS LONG) as measure30",
  "CAST((id + 4000000) % 140 AS INT) as measure31",
  "CAST((id + 40000000) % 2800 AS DOUBLE) as measure32",
  "CAST((id + 5) % 1600 AS DOUBLE) as measure33",
  "CAST((id + 50) % 800 AS DOUBLE) as measure34",
  "CAST((id + 500) % 450 AS INT) as measure35",
  "CAST((id + 5000) % 16000 AS DOUBLE) as measure36",
  "CAST((id + 50000) % 80 AS DOUBLE) as measure37",
  "CAST((id + 500000) % 1600000 AS LONG) as measure38",
  "CAST((id + 5000000) % 160 AS INT) as measure39",
  "CAST((id + 50000000) % 3200 AS DOUBLE) as measure40",
  "CAST((id + 6) % 1800 AS DOUBLE) as measure41",
  "CAST((id + 60) % 900 AS DOUBLE) as measure42",
  "CAST((id + 600) % 550 AS INT) as measure43",
  "CAST((id + 6000) % 18000 AS DOUBLE) as measure44",
  "CAST((id + 60000) % 90 AS DOUBLE) as measure45",
  "CAST((id + 600000) % 1800000 AS LONG) as measure46",
  "CAST((id + 6000000) % 180 AS INT) as measure47",
  "CAST((id + 60000000) % 3600 AS DOUBLE) as measure48",
  "CAST((id + 7) % 2000 AS DOUBLE) as measure49",
  "CAST((id + 70) % 1000 AS DOUBLE) as measure50",
  "CAST((id + 700) % 650 AS INT) as measure51",
  "CAST((id + 7000) % 20000 AS DOUBLE) as measure52",
  "CAST((id + 70000) % 100 AS DOUBLE) as measure53",
  "CAST((id + 700000) % 2000000 AS LONG) as measure54",
  "CAST((id + 7000000) % 200 AS INT) as measure55",
  "CAST((id + 70000000) % 4000 AS DOUBLE) as measure56",
  "CAST((id + 8) % 2200 AS DOUBLE) as measure57",
  "CAST((id + 80) % 1100 AS DOUBLE) as measure58",
  "CAST((id + 800) % 750 AS INT) as measure59",
  "CAST((id + 8000) % 22000 AS DOUBLE) as measure60",
  "CAST((id + 80000) % 110 AS DOUBLE) as measure61",
  "CAST((id + 800000) % 2200000 AS LONG) as measure62",
  "CAST((id + 8000000) % 220 AS INT) as measure63",
  "CAST((id + 80000000) % 4400 AS DOUBLE) as measure64",
  "CAST((id + 9) % 2400 AS DOUBLE) as measure65",
  "CAST((id + 90) % 1200 AS DOUBLE) as measure66",
  "CAST((id + 900) % 850 AS INT) as measure67",
  "CAST((id + 9000) % 24000 AS DOUBLE) as measure68",
  "CAST((id + 90000) % 120 AS DOUBLE) as measure69",
  "CAST((id + 900000) % 2400000 AS LONG) as measure70",
  "CAST((id + 9000000) % 240 AS INT) as measure71",
  "CAST((id + 90000000) % 4800 AS DOUBLE) as measure72",
  "CAST((id + 11) % 2600 AS DOUBLE) as measure73",
  "CAST((id + 110) % 1300 AS DOUBLE) as measure74",
  "CAST((id + 1100) % 950 AS INT) as measure75",
  "CAST((id + 11000) % 26000 AS DOUBLE) as measure76",
  "CAST((id + 110000) % 130 AS DOUBLE) as measure77",
  "CAST((id + 1100000) % 2600000 AS LONG) as measure78",
  "CAST((id + 11000000) % 260 AS INT) as measure79",
  "CAST((id + 99000000) % 5200 AS DOUBLE) as measure80"
)

val genTime = System.currentTimeMillis() - startTime
println(s"Data generation completed in ${genTime}ms")

println("=== Two-Level Aggregation Query (Direct DataFrame Processing) ===")
val queryStartTime = System.currentTimeMillis()

// Direct Two-Level Aggregation using DataFrame API
// 第一层聚合: 按 a, b 分组
val firstAgg = syntheticDF.groupBy($"a", $"b")
  .agg(
    count("*").as("row_count"),
    min($"measure1").as("min_measure1"), max($"measure1").as("max_measure1"),
    sum($"measure2").as("sum_measure2"), avg($"measure3").as("avg_measure3"),
    min($"measure4").as("min_measure4"), max($"measure5").as("max_measure5"),
    sum($"measure6").as("sum_measure6"), avg($"measure7").as("avg_measure7"),
    min($"measure8").as("min_measure8"), max($"measure9").as("max_measure9"),
    sum($"measure10").as("sum_measure10"), avg($"measure11").as("avg_measure11"),
    min($"measure12").as("min_measure12"), max($"measure13").as("max_measure13"),
    sum($"measure14").as("sum_measure14"), avg($"measure15").as("avg_measure15"),
    min($"measure16").as("min_measure16"), max($"measure17").as("max_measure17"),
    sum($"measure18").as("sum_measure18"), avg($"measure19").as("avg_measure19"),
    min($"measure20").as("min_measure20"), max($"measure21").as("max_measure21"),
    sum($"measure22").as("sum_measure22"), avg($"measure23").as("avg_measure23"),
    min($"measure24").as("min_measure24"), max($"measure25").as("max_measure25"),
    sum($"measure26").as("sum_measure26"), avg($"measure27").as("avg_measure27"),
    min($"measure28").as("min_measure28"), max($"measure29").as("max_measure29"),
    sum($"measure30").as("sum_measure30"), avg($"measure31").as("avg_measure31"),
    min($"measure32").as("min_measure32"), max($"measure33").as("max_measure33"),
    sum($"measure34").as("sum_measure34"), avg($"measure35").as("avg_measure35"),
    min($"measure36").as("min_measure36"), max($"measure37").as("max_measure37"),
    sum($"measure38").as("sum_measure38"), avg($"measure39").as("avg_measure39"),
    min($"measure40").as("min_measure40"), max($"measure41").as("max_measure41"),
    sum($"measure42").as("sum_measure42"), avg($"measure43").as("avg_measure43"),
    min($"measure44").as("min_measure44"), max($"measure45").as("max_measure45"),
    sum($"measure46").as("sum_measure46"), avg($"measure47").as("avg_measure47"),
    min($"measure48").as("min_measure48"), max($"measure49").as("max_measure49"),
    sum($"measure50").as("sum_measure50"), avg($"measure51").as("avg_measure51"),
    min($"measure52").as("min_measure52"), max($"measure53").as("max_measure53"),
    sum($"measure54").as("sum_measure54"), avg($"measure55").as("avg_measure55"),
    min($"measure56").as("min_measure56"), max($"measure57").as("max_measure57"),
    sum($"measure58").as("sum_measure58"), avg($"measure59").as("avg_measure59"),
    min($"measure60").as("min_measure60"), max($"measure61").as("max_measure61"),
    sum($"measure62").as("sum_measure62"), avg($"measure63").as("avg_measure63"),
    min($"measure64").as("min_measure64"), max($"measure65").as("max_measure65"),
    sum($"measure66").as("sum_measure66"), avg($"measure67").as("avg_measure67"),
    min($"measure68").as("min_measure68"), max($"measure69").as("max_measure69"),
    sum($"measure70").as("sum_measure70"), avg($"measure71").as("avg_measure71"),
    min($"measure72").as("min_measure72"), max($"measure73").as("max_measure73"),
    sum($"measure74").as("sum_measure74"), avg($"measure75").as("avg_measure75"),
    min($"measure76").as("min_measure76"), max($"measure77").as("max_measure77"),
    sum($"measure78").as("sum_measure78"), avg($"measure79").as("avg_measure79"),
    min($"measure80").as("min_measure80")
  )

// 第二层聚合: 按 b 分组，对第一层结果再聚合
val combinedGroupBy = firstAgg.groupBy($"b")
  .agg(
    count("*").as("group_count"), 
    sum($"row_count").as("total_rows_in_b"),
    min($"min_measure1").as("overall_min_m1"), 
    max($"max_measure1").as("overall_max_m1"),
    sum($"sum_measure2").as("total_m2"), 
    avg($"avg_measure3").as("avg_of_avg_m3"),
    min($"min_measure4").as("overall_min_m4"), 
    max($"max_measure5").as("overall_max_m5"),
    sum($"sum_measure6").as("total_m6"), 
    avg($"avg_measure7").as("avg_of_avg_m7"),
    min($"min_measure8").as("overall_min_m8"), 
    max($"max_measure9").as("overall_max_m9"),
    sum($"sum_measure10").as("total_m10"), 
    avg($"avg_measure11").as("avg_of_avg_m11"),
    min($"min_measure12").as("overall_min_m12"), 
    max($"max_measure13").as("overall_max_m13"),
    sum($"sum_measure14").as("total_m14"), 
    avg($"avg_measure15").as("avg_of_avg_m15"),
    min($"min_measure16").as("overall_min_m16"), 
    max($"max_measure17").as("overall_max_m17"),
    sum($"sum_measure18").as("total_m18"), 
    avg($"avg_measure19").as("avg_of_avg_m19"),
    min($"min_measure20").as("overall_min_m20"), 
    max($"max_measure21").as("overall_max_m21"),
    sum($"sum_measure22").as("total_m22"), 
    avg($"avg_measure23").as("avg_of_avg_m23"),
    min($"min_measure24").as("overall_min_m24"), 
    max($"max_measure25").as("overall_max_m25"),
    sum($"sum_measure26").as("total_m26"), 
    avg($"avg_measure27").as("avg_of_avg_m27"),
    min($"min_measure28").as("overall_min_m28"), 
    max($"max_measure29").as("overall_max_m29"),
    sum($"sum_measure30").as("total_m30"), 
    avg($"avg_measure31").as("avg_of_avg_m31"),
    min($"min_measure32").as("overall_min_m32"), 
    max($"max_measure33").as("overall_max_m33"),
    sum($"sum_measure34").as("total_m34"), 
    avg($"avg_measure35").as("avg_of_avg_m35"),
    min($"min_measure36").as("overall_min_m36"), 
    max($"max_measure37").as("overall_max_m37"),
    sum($"sum_measure38").as("total_m38"), 
    avg($"avg_measure39").as("avg_of_avg_m39"),
    min($"min_measure40").as("overall_min_m40"), 
    max($"max_measure41").as("overall_max_m41"),
    sum($"sum_measure42").as("total_m42"), 
    avg($"avg_measure43").as("avg_of_avg_m43"),
    min($"min_measure44").as("overall_min_m44"), 
    max($"max_measure45").as("overall_max_m45"),
    sum($"sum_measure46").as("total_m46"), 
    avg($"avg_measure47").as("avg_of_avg_m47"),
    min($"min_measure48").as("overall_min_m48"), 
    max($"max_measure49").as("overall_max_m49"),
    sum($"sum_measure50").as("total_m50"), 
    avg($"avg_measure51").as("avg_of_avg_m51"),
    min($"min_measure52").as("overall_min_m52"), 
    max($"max_measure53").as("overall_max_m53"),
    sum($"sum_measure54").as("total_m54"), 
    avg($"avg_measure55").as("avg_of_avg_m55"),
    min($"min_measure56").as("overall_min_m56"), 
    max($"max_measure57").as("overall_max_m57"),
    sum($"sum_measure58").as("total_m58"), 
    avg($"avg_measure59").as("avg_of_avg_m59"),
    min($"min_measure60").as("overall_min_m60"), 
    max($"max_measure61").as("overall_max_m61"),
    sum($"sum_measure62").as("total_m62"), 
    avg($"avg_measure63").as("avg_of_avg_m63"),
    min($"min_measure64").as("overall_min_m64"), 
    max($"max_measure65").as("overall_max_m65"),
    sum($"sum_measure66").as("total_m66"), 
    avg($"avg_measure67").as("avg_of_avg_m67"),
    min($"min_measure68").as("overall_min_m68"), 
    max($"max_measure69").as("overall_max_m69"),
    sum($"sum_measure70").as("total_m70"), 
    avg($"avg_measure71").as("avg_of_avg_m71"),
    min($"min_measure72").as("overall_min_m72"), 
    max($"max_measure73").as("overall_max_m73"),
    sum($"sum_measure74").as("total_m74"), 
    avg($"avg_measure75").as("avg_of_avg_m75"),
    min($"min_measure76").as("overall_min_m76"), 
    max($"max_measure77").as("overall_max_m77"),
    sum($"sum_measure78").as("total_m78"), 
    avg($"avg_measure79").as("avg_of_avg_m79"),
    min($"min_measure80").as("overall_min_m80"),
    // 条件统计
    sum(when($"min_measure1" > 100, 1).otherwise(0)).as("groups_m1_gt_100"),
    sum(when($"sum_measure10" > 200, $"row_count").otherwise(0)).as("rows_m10_gt_200"),
    sum(when($"min_measure20" > 50, 1).otherwise(0)).as("groups_m20_gt_50"),
    sum(when($"sum_measure30" > 300, 1).otherwise(0)).as("groups_m30_gt_300"),
    sum(when($"min_measure40" > 400, $"row_count").otherwise(0)).as("rows_m40_gt_400"),
    sum(when($"sum_measure50" > 500, 1).otherwise(0)).as("groups_m50_gt_500"),
    sum(when($"min_measure60" > 600, 1).otherwise(0)).as("groups_m60_gt_600"),
    sum(when($"sum_measure70" > 700, $"row_count").otherwise(0)).as("rows_m70_gt_700"),
    sum(when($"min_measure80" > 800, 1).otherwise(0)).as("groups_m80_gt_800")
  ).orderBy($"b")

combinedGroupBy.show(20)

val queryTime = System.currentTimeMillis() - queryStartTime
println(s"Combined group by completed in ${queryTime}ms")

val totalTime = System.currentTimeMillis() - startTime
println(s"=== Total execution time: ${totalTime}ms ===")

println("=== Query Plan Analysis ===")
println("Combined two-level aggregation plan:")
combinedGroupBy.explain(true)

println("=== Performance Summary ===")
println(s"Data generation: ${genTime}ms")
println(s"Combined group by query: ${queryTime}ms") 
println(s"Total execution: ${totalTime}ms")

println("=== Script completed successfully! ===")

System.exit(0)