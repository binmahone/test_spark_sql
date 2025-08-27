#!/usr/bin/env python3
import json
import sys
from collections import defaultdict

def extract_stage_times(event_log_file):
    stage_times = defaultdict(int)  # stage_id -> total_time_ms
    app_name = ""
    app_id = ""
    
    with open(event_log_file, 'r') as f:
        for line in f:
            try:
                event = json.loads(line.strip())
                
                # Get app name and app id
                if event.get('Event') == 'SparkListenerApplicationStart':
                    app_name = event.get('App Name', 'Unknown')
                    app_id = event.get('App ID', 'Unknown')
                
                # Process task completion events
                elif event.get('Event') == 'SparkListenerTaskEnd':
                    stage_id = event.get('Stage ID', -1)
                    task_info = event.get('Task Info', {})
                    
                    # Calculate task execution time (Finish Time - Launch Time)
                    finish_time = task_info.get('Finish Time', 0)
                    launch_time = task_info.get('Launch Time', 0)
                    
                    if finish_time > 0 and launch_time > 0:
                        task_duration = finish_time - launch_time
                        stage_times[stage_id] += task_duration
                        
            except json.JSONDecodeError:
                continue
    
    return app_name, app_id, stage_times

def main():
    import os
    import glob
    
    # Get all event log files and sort by modification time (newest first)
    event_log_pattern = '/tmp/spark-events/local-*'
    all_files = glob.glob(event_log_pattern)
    all_files.sort(key=os.path.getmtime, reverse=True)
    
    # Take the 6 most recent files
    files = all_files[:4]
    
    # Collect all data first
    data = [["App Name (App ID)", "Stage 0", "Stage 1", "Stage 2", "Stage 3", "Total"]]
    
    for event_log in files:
        try:
            app_name, app_id, stage_times = extract_stage_times(event_log)
            
            # Get times for stages 0-3, converting to seconds
            stage_0 = round(stage_times.get(0, 0) / 1000, 2) if stage_times.get(0, 0) > 0 else 0
            stage_1 = round(stage_times.get(1, 0) / 1000, 2) if stage_times.get(1, 0) > 0 else 0  
            stage_2 = round(stage_times.get(2, 0) / 1000, 2) if stage_times.get(2, 0) > 0 else 0
            stage_3 = round(stage_times.get(3, 0) / 1000, 2) if stage_times.get(3, 0) > 0 else 0
            total = stage_0 + stage_1 + stage_2 + stage_3
            
            data.append([f"{app_name} ({app_id})", str(stage_0), str(stage_1), str(stage_2), str(stage_3), f"{total:.2f}"])
            
        except Exception as e:
            print(f"Error processing {event_log}: {e}", file=sys.stderr)
    
    # Print formatted table
    print("=" * 120)
    print(" Latest 5 Spark Event Log Analysis - Total Time Across All Tasks (seconds)")
    print("=" * 120)
    print()
    
    # Calculate column widths
    col_widths = []
    for i in range(len(data[0])):
        col_widths.append(max(len(str(row[i])) for row in data))
    
    # Print header
    header = data[0]
    print("| " + " | ".join(f"{header[i]:>{col_widths[i]}}" for i in range(len(header))) + " |")
    print("|" + "|".join("-" * (col_widths[i] + 2) for i in range(len(header))) + "|")
    
    # Print data rows
    for row in data[1:]:
        print("| " + " | ".join(f"{row[i]:>{col_widths[i]}}" for i in range(len(row))) + " |")
    
    print()
    print("=" * 120)
    print(" Performance Ranking (by Total Time)")
    print("=" * 120)
    print()
    
    # Create performance ranking
    performance_data = []
    for row in data[1:]:
        app_name = row[0].split(' (')[0]  # Extract just app name
        total_time = float(row[5])
        performance_data.append((app_name, total_time))
    
    performance_data.sort(key=lambda x: x[1])
    
    for i, (app_name, total_time) in enumerate(performance_data, 1):
        if i == 1:
            print(f"🏆 {i}. {app_name}: {total_time:.2f}s - BEST")
        elif i == 2:
            print(f"🥈 {i}. {app_name}: {total_time:.2f}s")
        elif i == 3:
            print(f"🥉 {i}. {app_name}: {total_time:.2f}s")
        elif i == len(performance_data):
            print(f"❌ {i}. {app_name}: {total_time:.2f}s - WORST")
        else:
            print(f"   {i}. {app_name}: {total_time:.2f}s")
    
    print()
    print("=" * 120)

if __name__ == '__main__':
    main()
