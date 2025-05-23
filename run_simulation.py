#!/usr/bin/env python3
"""
HTCondor Simulation Runner

This script runs a complete simulation of an HTCondor-like job pipeline,
generating school registry data and processing it into Parquet files.
"""

import os
import time
import argparse
import logging
import subprocess
import threading
import signal
import sys
import atexit
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("simulation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Simulation")


def run_generator(args):
    """Run the JSON generator process."""
    cmd = [
        "python", "json_generator.py",
        "--output-dir", args.input_dir,
        "--rate", str(args.generator_rate),
        "--std-dev", str(args.generator_stddev),
        "--min-size", str(args.min_batch_size),
        "--max-size", str(args.max_batch_size)
    ]
    
    if args.num_results:
        cmd.extend(["--num-iterations", str(args.num_results)])
    
    if args.seed is not None:
        cmd.extend(["--seed", str(args.seed)])
    
    logger.info(f"Starting JSON generator: {' '.join(cmd)}")
    return subprocess.Popen(cmd)


def run_listener(args):
    """Run the result listener process."""
    cmd = [
        "python", "result_listener.py",
        "--input-dir", args.input_dir,
        "--output-dir", args.output_dir,
        "--interval", str(args.listener_interval)
    ]
    
    if args.processed_dir:
        cmd.extend(["--processed-dir", args.processed_dir])
    
    logger.info(f"Starting result listener: {' '.join(cmd)}")
    return subprocess.Popen(cmd)


def create_directories(args):
    """Create necessary directories for the simulation."""
    Path(args.input_dir).mkdir(parents=True, exist_ok=True)
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    if args.processed_dir:
        Path(args.processed_dir).mkdir(parents=True, exist_ok=True)


def monitor_processes(processes, timeout=None):
    """Monitor processes and handle cleanup."""
    start_time = time.time()
    generator_process = processes[0]  # Assume first process is the generator
    listener_process = processes[1] if len(processes) > 1 else None
    
    try:
        while any(p.poll() is None for p in processes):
            # Check if timeout has been reached
            if timeout and (time.time() - start_time) > timeout:
                logger.info(f"Simulation timeout reached ({timeout}s). Stopping processes...")
                break
            
            # If generator has finished but listener is still running, we'll let the main
            # function decide when to stop the listener (to ensure all files are processed)
            if generator_process.poll() is not None:
                logger.info("Generator process has completed.")
                break
                
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Received interrupt. Stopping processes...")
        
        # Gracefully terminate all processes on interrupt
        for i, p in enumerate(processes):
            if p.poll() is None:  # Process is still running
                process_name = "generator" if i == 0 else "listener"
                logger.info(f"Terminating {process_name} process (PID: {p.pid})")
                try:
                    p.terminate()
                    p.wait(timeout=10)  # Give more time for graceful shutdown
                    logger.info(f"{process_name} process terminated gracefully")
                except subprocess.TimeoutExpired:
                    logger.warning(f"{process_name} process did not terminate gracefully. Forcing kill...")
                    p.kill()
                    p.wait(timeout=5)
                    logger.warning(f"{process_name} process killed")
                except Exception as e:
                    logger.error(f"Error terminating {process_name} process: {e}")


def display_performance_metrics(input_dir, output_dir, start_time, end_time, expected_count=None):
    """Display detailed performance metrics for the simulation run."""
    import os
    import pandas as pd
    import pyarrow.parquet as pq
    import json
    from datetime import datetime
    
    total_duration = end_time - start_time
    
    # Collect input JSON metrics
    input_path = Path(input_dir)
    json_files = list(input_path.glob("*.json"))
    processed_json_count = len(json_files)
    
    # Calculate JSON file sizes
    json_sizes = []
    json_entity_counts = []
    json_timestamps = []
    
    for json_file in json_files:
        # Get file size
        file_size = os.path.getsize(json_file)
        json_sizes.append(file_size)
        
        # Get timestamp from filename or file creation time
        try:
            # Try to extract timestamp from filename pattern school_data_YYYYMMDD_HHMMSS_ffffff.json
            timestamp_str = json_file.name.split('_')[2:5]
            if len(timestamp_str) >= 3:
                timestamp = datetime.strptime('_'.join(timestamp_str), '%Y%m%d_%H%M%S_%f')
            else:
                # Fallback to file creation time
                timestamp = datetime.fromtimestamp(os.path.getctime(json_file))
            json_timestamps.append(timestamp)
        except:
            # Fallback to file creation time
            json_timestamps.append(datetime.fromtimestamp(os.path.getctime(json_file)))
        
        # Count entities in the JSON
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                entity_count = sum(len(entities) for entities in data.values())
                json_entity_counts.append(entity_count)
        except:
            json_entity_counts.append(0)
    
    # Collect output Parquet metrics
    output_path = Path(output_dir)
    parquet_files = list(output_path.glob("*.parquet"))
    
    # Performance metrics to report
    metrics = {
        "simulation_duration_seconds": total_duration,
        "json_files_generated": processed_json_count,
        "json_generation_rate_per_second": processed_json_count / total_duration if total_duration > 0 else 0,
        "parquet_files_created": len(parquet_files),
    }
    
    # Add JSON size statistics if we have data
    if json_sizes:
        metrics.update({
            "json_size_bytes_avg": sum(json_sizes) / len(json_sizes),
            "json_size_bytes_min": min(json_sizes),
            "json_size_bytes_max": max(json_sizes),
            "json_size_bytes_total": sum(json_sizes),
        })
    
    # Add entity count statistics if we have data
    if json_entity_counts:
        metrics.update({
            "entities_per_json_avg": sum(json_entity_counts) / len(json_entity_counts),
            "entities_per_json_min": min(json_entity_counts),
            "entities_per_json_max": max(json_entity_counts),
            "entities_per_json_total": sum(json_entity_counts),
        })
    
    # Calculate interarrival times if we have timestamps
    if len(json_timestamps) > 1:
        sorted_timestamps = sorted(json_timestamps)
        interarrival_times = [(sorted_timestamps[i+1] - sorted_timestamps[i]).total_seconds() 
                              for i in range(len(sorted_timestamps)-1)]
        
        metrics.update({
            "json_interarrival_seconds_avg": sum(interarrival_times) / len(interarrival_times),
            "json_interarrival_seconds_min": min(interarrival_times),
            "json_interarrival_seconds_max": max(interarrival_times),
        })
    
    # Display metrics
    logger.info("\n" + "="*80)
    logger.info(f"SIMULATION PERFORMANCE METRICS (Duration: {total_duration:.2f} seconds)")
    logger.info("="*80)
    
    for metric, value in metrics.items():
        if isinstance(value, float):
            logger.info(f"{metric.replace('_', ' ').title()}: {value:.2f}")
        else:
            logger.info(f"{metric.replace('_', ' ').title()}: {value}")
    
    # Display completion status if expected count was provided
    if expected_count is not None:
        completion_percentage = (processed_json_count / expected_count) * 100
        logger.info(f"Completion: {completion_percentage:.1f}% ({processed_json_count}/{expected_count} files processed)")
    
    logger.info("-"*80)
    
    return metrics

def display_parquet_summary(output_dir):
    """Display a summary of the generated Parquet files."""
    try:
        import pandas as pd
        import pyarrow.parquet as pq
        
        output_path = Path(output_dir)
        parquet_files = list(output_path.glob("*.parquet"))
        
        if not parquet_files:
            logger.warning(f"No Parquet files found in {output_dir}")
            return
        
        logger.info("\n" + "="*80)
        logger.info("PARQUET FILES SUMMARY")
        logger.info("="*80)
        
        total_rows = 0
        file_sizes = []
        
        for pq_file in parquet_files:
            try:
                # Get file size
                file_size = os.path.getsize(pq_file)
                file_sizes.append(file_size)
                
                # Read Parquet metadata to get number of rows
                parquet_data = pq.read_metadata(pq_file)
                num_rows = parquet_data.num_rows
                
                # Read a sample of the data for column information
                df = pd.read_parquet(pq_file)
                num_cols = len(df.columns)
                
                logger.info(f"{pq_file.name}: {num_rows} rows, {num_cols} columns, {file_size/1024:.1f} KB")
                
                # Display first few column names
                sample_cols = ', '.join(df.columns[:5]) + (', ...' if len(df.columns) > 5 else '')
                logger.info(f"  Columns: {sample_cols}")
                
                total_rows += num_rows
            except Exception as e:
                logger.error(f"Error reading {pq_file}: {e}")
        
        logger.info("-"*80)
        logger.info(f"Total records across all files: {total_rows}")
        logger.info(f"Total Parquet data size: {sum(file_sizes)/1024/1024:.2f} MB")
        logger.info(f"Average record size: {sum(file_sizes)/total_rows:.2f} bytes per record" if total_rows > 0 else "No records found")
        logger.info("="*80)
    
    except ImportError:
        logger.warning("Could not import pandas or pyarrow to display summary.")


def main():
    """Main entry point for the simulation runner."""
    parser = argparse.ArgumentParser(description="Run HTCondor simulation with school registry data")
    
    # Directory settings
    parser.add_argument("--input-dir", default="./input", help="Directory for JSON files")
    parser.add_argument("--output-dir", default="./output", help="Directory for Parquet files")
    parser.add_argument("--processed-dir", default="./processed", help="Directory for processed JSON files")
    
    # Generator settings
    parser.add_argument("--generator-rate", type=float, default=0.33, 
                         help="Number of JSON files to generate per second (default: 0.33 per second)")
    parser.add_argument("--generator-stddev", type=float, default=0.1, 
                         help="Standard deviation for generation rate (affects timing variability)")
    parser.add_argument("--min-batch-size", type=int, default=1, help="Minimum entities per batch")
    parser.add_argument("--max-batch-size", type=int, default=10, help="Maximum entities per batch")
    parser.add_argument("--num-results", type=int, default=None, 
                         help="Number of results to generate (default: run for 10 minutes)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    
    # Listener settings
    parser.add_argument("--listener-interval", type=float, default=1.0, help="Interval for checking new files (seconds)")
    
    # Simulation control
    parser.add_argument("--timeout", type=int, default=600, 
                         help="Maximum simulation runtime in seconds (default: 600s = 10 minutes)")
    parser.add_argument("--summary", action="store_true", help="Display summary of generated Parquet files at the end")
    parser.add_argument("--performance-metrics", action="store_true", help="Display detailed performance metrics")
    parser.add_argument("--completion-wait", type=float, default=5.0, 
                        help="Wait time after generator completes to allow listener to finish (seconds)")
    
    args = parser.parse_args()
    
    # Create necessary directories
    create_directories(args)
    
    # Record start time
    start_time = time.time()
    
    # Start processes
    generator_process = run_generator(args)
    listener_process = run_listener(args)
    
    processes = [generator_process, listener_process]
    
    # Monitor and handle cleanup
    monitor_processes(processes, timeout=args.timeout)
    
    # If the generator has a fixed number of results, wait for listener to finish processing
    if args.num_results is not None and generator_process.poll() is not None:
        logger.info(f"Generator completed. Waiting {args.completion_wait} seconds for listener to finish processing...")
        
        # Give the listener extra time to finish processing all files
        time.sleep(args.completion_wait)
        
        # Now terminate the listener gracefully
        if listener_process.poll() is None:
            logger.info("Terminating listener process...")
            try:
                listener_process.terminate()
                listener_process.wait(timeout=10)
                logger.info("Listener process terminated gracefully")
            except subprocess.TimeoutExpired:
                logger.warning("Listener process did not terminate gracefully. Forcing kill...")
                listener_process.kill()
                try:
                    listener_process.wait(timeout=5)
                    logger.warning("Listener process killed")
                except subprocess.TimeoutExpired:
                    logger.error("Failed to kill listener process")
            except Exception as e:
                logger.error(f"Error terminating listener process: {e}")
    
    # Record end time
    end_time = time.time()
    
    logger.info("Simulation completed.")
    
    # Display performance metrics if requested
    if args.performance_metrics or args.summary:
        # Always show performance metrics if summary is requested
        metrics = display_performance_metrics(
            args.input_dir, 
            args.output_dir, 
            start_time, 
            end_time,
            expected_count=args.num_results
        )
    
    # Display Parquet summary if requested
    if args.summary:
        display_parquet_summary(args.output_dir)
        
    return 0


if __name__ == "__main__":
    main()
