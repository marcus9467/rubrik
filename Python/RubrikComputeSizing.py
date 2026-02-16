"""
RUBRIK DAILY CAPACITY PLANNER
================================================================================
Usage: 
    python3 rubrik_capacity_planner.py <filename.csv> [options]

Description:
    This tool calculates the THEORETICAL MINIMUM node count required to handle 
    your daily data protection volume. 

    It ignores "Peak Hour" congestion (Thunder Herds) and assumes you will 
    optimize your schedule to run jobs evenly over a 24-hour window.

    It calculates the total duration of all tasks in a day, finds the 
    average number of active tasks required to clear that work in 1440 minutes, 
    and applies hardware concurrency limits.

Options:
    filename.csv       : Path to the Protection Task Details CSV report.
    --rubrik           : Use standard Rubrik R6000 limits (Default is 3rd Party).
    -h, --help         : Show this help message.

Hardware Limits Used:
    1. 3rd Party (Default):
       - 1 Node = 0.75x capacity of a full Brik (approx 3x a standard node).
       - Limits: Backup=12, Replication=15, Archive=6 concurrent tasks.
    
    2. Rubrik R6000 (--rubrik):
       - Standard high-performance node.
       - Limits: Backup=4, Replication=5, Archive=2 concurrent tasks.

Output Interpretation:
    - DRIVER    : The specific task type (Backup, Repl, Arch) forcing the upgrade.
    - MIN NODES : The absolute floor for hardware. (Reality = Min Nodes + 20%).
================================================================================
"""

import pandas as pd
import sys
import os
import argparse
import math

# ==========================================
# CONFIGURATION
# ==========================================
# 1. 3rd Party Limits (Default)
# Scaling Logic: 1 Node = 0.75 Brik. (1 Brik = 4 Std Nodes).
# Therefore 1 3rd Party Node ~ 3x Std Node capacity.
LIMITS_3RD_PARTY = {
    'Backup': 12.0,      
    'Replication': 15.0, 
    'Archive': 6.0,      
    'Log': 10.0          
}

# 2. Rubrik R6000 Limits (via --rubrik flag)
LIMITS_RUBRIK = {
    'Backup': 4.0,
    'Replication': 5.0,
    'Archive': 2.0,
    'Log': 10.0
}

def get_category(task_type):
    t = str(task_type)
    if 'Log' in t: return 'Log'
    if 'Replication' in t: return 'Replication'
    if 'Archiv' in t: return 'Archive'
    return 'Backup' 

def analyze_daily_capacity(file_path, use_rubrik_limits):
    # Select Limits
    LIMITS = LIMITS_RUBRIK if use_rubrik_limits else LIMITS_3RD_PARTY
    hw_label = "Rubrik R6000" if use_rubrik_limits else "3rd Party Custom"
    
    print(f"Reading {file_path}...")
    print(f"Hardware Profile: {hw_label}")
    print(f"Swimlane Limits:  {LIMITS}")
    
    if not os.path.exists(file_path):
        print(f"\nERROR: File '{file_path}' not found.")
        return

    # Structure: { (Cluster, Date, Category): Total_Duration_Minutes }
    daily_load = {}
    
    chunk_size = 50000
    row_count = 0
    
    try:
        with pd.read_csv(file_path, chunksize=chunk_size, 
                         usecols=['Cluster Name', 'Task Type', 'Start Time', 'End Time']) as reader:
            
            for chunk in reader:
                row_count += len(chunk)
                sys.stdout.write(f"\rRows processed: {row_count:,}")
                sys.stdout.flush()
                
                # Clean Dates
                chunk['Start Time'] = pd.to_datetime(chunk['Start Time'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
                chunk['End Time'] = pd.to_datetime(chunk['End Time'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
                chunk.dropna(subset=['Start Time', 'End Time', 'Cluster Name'], inplace=True)
                
                # Calculate Duration
                chunk['Duration_Min'] = (chunk['End Time'] - chunk['Start Time']).dt.total_seconds() / 60.0
                chunk = chunk[chunk['Duration_Min'] > 0] 
                
                for row in chunk.itertuples(index=False):
                    c_name = str(row[0])
                    t_type = str(row[1])
                    start_t = row[2]
                    duration = row[4]
                    
                    cat = get_category(t_type)
                    date_key = start_t.strftime('%Y-%m-%d')
                    
                    key = (c_name, date_key, cat)
                    if key not in daily_load:
                        daily_load[key] = 0.0
                    
                    daily_load[key] += duration

    except Exception as e:
        print(f"\nError: {e}")
        return

    # Process Data
    cluster_dates = {}
    for (cluster, date, cat), duration in daily_load.items():
        if (cluster, date) not in cluster_dates:
            cluster_dates[(cluster, date)] = {'Backup': 0, 'Replication': 0, 'Archive': 0, 'Log': 0}
        cluster_dates[(cluster, date)][cat] += duration

    # ---------------------------------------------------------
    # 1. Detailed Daily Breakdown
    # ---------------------------------------------------------
    print("\n\n" + "="*120)
    print(f"{'CLUSTER NAME':<20} | {'DATE':<12} | {'DRIVER':<10} | {'AVG TASKS':<10} | {'REC NODES':<10} | {'(Details...)':<14}")
    print("="*120)

    # Dictionary to track the Peak Requirement per cluster (for the summary)
    cluster_summary = {}

    sorted_keys = sorted(cluster_dates.keys())
    for cluster, date in sorted_keys:
        stats = cluster_dates[(cluster, date)]
        
        # Calc Average Concurrent Tasks (Duration / 1440 mins)
        avg_backup = stats['Backup'] / 1440.0
        avg_repl = stats['Replication'] / 1440.0
        avg_arch = stats['Archive'] / 1440.0
        
        # Calc Nodes Needed
        nodes_backup = math.ceil(avg_backup / LIMITS['Backup'])
        nodes_repl = math.ceil(avg_repl / LIMITS['Replication'])
        nodes_arch = math.ceil(avg_arch / LIMITS['Archive'])
        
        # Find Bottleneck
        # We take the MAX because lanes are independent (if you have enough nodes for backups, you likely have enough for archive)
        reqs = {'Backup': nodes_backup, 'Replication': nodes_repl, 'Archive': nodes_arch}
        driver = max(reqs, key=reqs.get)
        rec_nodes = reqs[driver]
        
        # Update Cluster Summary (High Water Mark)
        if cluster not in cluster_summary:
            cluster_summary[cluster] = {'nodes': 0, 'driver': '', 'date': ''}
        
        if rec_nodes > cluster_summary[cluster]['nodes']:
            cluster_summary[cluster] = {'nodes': rec_nodes, 'driver': driver, 'date': date}

        # Print Daily Line (Only if significant)
        if rec_nodes > 0:
             print(f"{cluster:<20} | {date:<12} | {driver:<10} | {round(avg_backup+avg_repl+avg_arch,1):<10} | {rec_nodes:<10} | (B:{nodes_backup} R:{nodes_repl} A:{nodes_arch})")

    # ---------------------------------------------------------
    # 2. Executive Summary Table
    # ---------------------------------------------------------
    print("\n\n")
    print("="*90)
    print(f"FINAL RECOMMENDATION: MINIMUM NODE COUNT ({hw_label})")
    print("="*90)
    print(f"{'CLUSTER NAME':<25} | {'MIN NODES':<12} | {'PRIMARY BOTTLENECK':<20} | {'PEAK DATE':<12}")
    print("-" * 90)
    
    for cluster, stats in cluster_summary.items():
        print(f"{cluster:<25} | {stats['nodes']:<12} | {stats['driver']:<20} | {stats['date']:<12}")
        
    print("="*90)
    print("WARNING: These numbers represent the THEORETICAL MINIMUM.")
    print("This model assumes you will re-architect your schedule to run jobs 24/7.")
    print("If you must run all jobs within an 8-hour window, multiply these numbers by 3.")
    print("="*90)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Rubrik Daily Capacity Planner')
    parser.add_argument('filename', help='CSV Report Path')
    parser.add_argument('--rubrik', action='store_true', help='Use Rubrik R6000 limits instead of 3rd Party')
    
    args = parser.parse_args()
    
    analyze_daily_capacity(args.filename, args.rubrik)
