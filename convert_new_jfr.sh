#!/bin/bash

# JFR to HTML Flame Graph Converter Script
# This script converts new JFR files to HTML flame graphs

# Configuration
JFR_DIR="/home/hongbin/data/flame"
CONVERTER_JAR="/home/hongbin/develop/async-profiler-3.0-linux-x64/lib/converter.jar"
MARKER_FILE="$JFR_DIR/.last_conversion_time"

echo "=== JFR to HTML Flame Graph Converter ==="
echo "JFR Directory: $JFR_DIR"
echo "Converter JAR: $CONVERTER_JAR"
echo ""

# Check if converter exists
if [ ! -f "$CONVERTER_JAR" ]; then
    echo "ERROR: Converter JAR not found at $CONVERTER_JAR"
    exit 1
fi

cd "$JFR_DIR"

# Get the timestamp of last conversion (or use a very old date if first run)
if [ -f "$MARKER_FILE" ]; then
    LAST_TIME=$(cat "$MARKER_FILE")
    echo "Last conversion time: $(date -d @$LAST_TIME)"
else
    LAST_TIME=0
    echo "First run - will process all JFR files"
fi

# Find JFR files newer than last conversion
NEW_JFR_FILES=$(find . -name "*.jfr" -newer "$MARKER_FILE" 2>/dev/null || find . -name "*.jfr")

if [ -z "$NEW_JFR_FILES" ]; then
    echo "No new JFR files found."
    exit 0
fi

echo "Found new JFR files:"
echo "$NEW_JFR_FILES" | sed 's/^/  /'
echo ""

# Convert each new JFR file
CONVERTED_COUNT=0
FAILED_COUNT=0
DELETED_COUNT=0

# Generate timestamp prefix (YYYYMMDD_HHMMSS)
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

for jfr_file in $NEW_JFR_FILES; do
    # Remove ./ prefix and .jfr suffix
    base_name=$(basename "$jfr_file" .jfr)
    html_file="${TIMESTAMP}_${base_name}.html"
    
    echo "Converting: $jfr_file -> $html_file"
    
    if java -cp "$CONVERTER_JAR" jfr2flame "$jfr_file" "$html_file" 2>/dev/null; then
        echo "  ✅ Success: $html_file created"
        CONVERTED_COUNT=$((CONVERTED_COUNT + 1))
        
        # Delete the original JFR file after successful conversion
        if rm "$jfr_file" 2>/dev/null; then
            echo "  🗑️  Deleted: $jfr_file"
            DELETED_COUNT=$((DELETED_COUNT + 1))
        else
            echo "  ⚠️  Warning: Failed to delete $jfr_file"
        fi
    else
        echo "  ❌ Failed to convert $jfr_file"
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
done

# Update marker file with current timestamp
date +%s > "$MARKER_FILE"

echo ""
echo "=== Conversion Summary ==="
echo "Successfully converted: $CONVERTED_COUNT files"
echo "Failed conversions: $FAILED_COUNT files"
echo "JFR files deleted: $DELETED_COUNT files"
echo "HTML files are ready for viewing in browser"

# List generated HTML files
if [ $CONVERTED_COUNT -gt 0 ]; then
    echo ""
    echo "Generated HTML files (with timestamp prefix):"
    ls -la ${TIMESTAMP}_*.html 2>/dev/null | sed 's/^/  /' || echo "  No HTML files found with current timestamp"
fi

echo ""
echo "Script completed at $(date)"
