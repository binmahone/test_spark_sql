#!/bin/bash

# Batch runner for NSys profiling of CONCURRENT_TASKS=3 and CONCURRENT_TASKS=9

echo "=========================================="
echo "Starting NSys Profiling Batch for CONCURRENT_TASKS=3 and 9"
echo "=========================================="

# Create directories
mkdir -p log
mkdir -p nsys_profiles

echo "Step 1: Running NSys profiling for CONCURRENT_TASKS=3"
echo "Time: $(date)"
echo ""

./nsys_profile_tasks3.sh

echo ""
echo "=========================================="
echo "Completed CONCURRENT_TASKS=3. Waiting 30 seconds before next run..."
echo "=========================================="
sleep 30

echo ""
echo "Step 2: Running NSys profiling for CONCURRENT_TASKS=9"
echo "Time: $(date)"
echo ""

./nsys_profile_tasks9.sh

echo ""
echo "=========================================="
echo "All NSys profiling completed!"
echo "Time: $(date)"
echo "=========================================="
echo ""
echo "Generated files:"
echo "  📂 nsys_profiles/ - NSys profile files (.nsys-rep)"
echo "  📂 log/ - Spark driver and GC logs"
echo ""
echo "To view profiles use:"
echo "  nsys-ui nsys_profiles/rapids_tasks3_*.nsys-rep"
echo "  nsys-ui nsys_profiles/rapids_tasks9_*.nsys-rep"
echo "=========================================="
