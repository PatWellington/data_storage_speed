#!/usr/bin/env python3
"""
Result Listener Script

This script continuously monitors a specified directory for new JSON files,
validates them, and writes the data to related Parquet files with proper schemas.
"""

import os
import time
import json
import logging
import signal
import sys
import atexit
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Set
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, validator, Field
import shutil
import socket
from pathlib import Path


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("result_listener.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ResultListener")


# Pydantic models for validation
class Student(BaseModel):
    student_id: str
    first_name: str
    last_name: str
    age: int
    gender: str
    year: int
    gpa: float
    address: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    enrollment_date: Optional[str] = None
    
    @validator('age')
    def age_must_be_valid(cls, v):
        if v < 5 or v > 25:
            raise ValueError('Age must be between 5 and 25')
        return v
    
    @validator('year')
    def year_must_be_valid(cls, v):
        if v < 1 or v > 12:
            raise ValueError('Year must be between 1 and 12')
        return v
    
    @validator('gpa')
    def gpa_must_be_valid(cls, v):
        if v < 0.0 or v > 4.0:
            raise ValueError('GPA must be between 0.0 and 4.0')
        return v


class Teacher(BaseModel):
    teacher_id: str
    first_name: str
    last_name: str
    department: str
    email: str
    hire_date: Optional[str] = None
    salary: Optional[float] = None
    office_number: Optional[str] = None
    phone: Optional[str] = None
    qualifications: Optional[List[str]] = None


class Class(BaseModel):
    class_id: str
    class_name: str
    room_number: str
    teacher_id: str
    schedule: str
    semester: str
    max_capacity: int
    current_enrollment: int
    
    @validator('current_enrollment')
    def enrollment_cannot_exceed_capacity(cls, v, values):
        if 'max_capacity' in values and v > values['max_capacity']:
            raise ValueError('Current enrollment cannot exceed maximum capacity')
        return v


class StudentClass(BaseModel):
    student_class_id: str
    student_id: str
    class_id: str
    grade: Optional[str] = None
    enrollment_date: Optional[str] = None


class SchoolData(BaseModel):
    students: Optional[List[Student]] = []
    teachers: Optional[List[Teacher]] = []
    classes: Optional[List[Class]] = []
    student_classes: Optional[List[StudentClass]] = []


# Status tracking for safe shutdown
running_instance = None

def signal_handler(sig, frame):
    """Handle termination signals for clean shutdown."""
    logger.info(f"Received signal {sig}. Initiating safe shutdown...")
    if running_instance:
        running_instance.stop()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

class ResultListener:
    def __init__(
        self, 
        input_dir: str, 
        output_dir: str,
        processed_dir: str = None,
        check_interval: float = 1.0,
        max_retries: int = 3,
    ):
        """
        Initialize the ResultListener to monitor and process JSON files.
        
        Args:
            input_dir: Directory to monitor for new JSON files
            output_dir: Directory to save Parquet files
            processed_dir: Directory to move processed files (if None, files are deleted)
            check_interval: Time interval (seconds) between directory checks
            max_retries: Maximum number of retries for processing a file
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.processed_dir = Path(processed_dir) if processed_dir else None
        self.check_interval = check_interval
        self.max_retries = max_retries
        
        # Create necessary directories
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.processed_dir:
            self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up Parquet file paths
        self.students_parquet = self.output_dir / "students.parquet"
        self.teachers_parquet = self.output_dir / "teachers.parquet"
        self.classes_parquet = self.output_dir / "classes.parquet"
        self.student_classes_parquet = self.output_dir / "student_classes.parquet"
        
        # Initialize Parquet files if they don't exist
        self._initialize_parquet_files()
        
        logger.info(f"ResultListener initialized to monitor: {input_dir}")
        logger.info(f"Parquet files will be saved to: {output_dir}")
    
    def _initialize_parquet_files(self):
        """Initialize empty Parquet files with proper schemas if they don't exist."""
        
        # Define schemas
        self._create_empty_parquet_if_not_exists(
            self.students_parquet,
            pa.schema([
                ('student_id', pa.string()),
                ('first_name', pa.string()),
                ('last_name', pa.string()),
                ('age', pa.int32()),
                ('gender', pa.string()),
                ('year', pa.int32()),
                ('gpa', pa.float32()),
                ('address', pa.string()),
                ('email', pa.string()),
                ('phone', pa.string()),
                ('enrollment_date', pa.string())
            ])
        )
        
        self._create_empty_parquet_if_not_exists(
            self.teachers_parquet,
            pa.schema([
                ('teacher_id', pa.string()),
                ('first_name', pa.string()),
                ('last_name', pa.string()),
                ('department', pa.string()),
                ('email', pa.string()),
                ('hire_date', pa.string()),
                ('salary', pa.float32()),
                ('office_number', pa.string()),
                ('phone', pa.string()),
                ('qualifications', pa.list_(pa.string()))
            ])
        )
        
        self._create_empty_parquet_if_not_exists(
            self.classes_parquet,
            pa.schema([
                ('class_id', pa.string()),
                ('class_name', pa.string()),
                ('room_number', pa.string()),
                ('teacher_id', pa.string()),
                ('schedule', pa.string()),
                ('semester', pa.string()),
                ('max_capacity', pa.int32()),
                ('current_enrollment', pa.int32())
            ])
        )
        
        self._create_empty_parquet_if_not_exists(
            self.student_classes_parquet,
            pa.schema([
                ('student_class_id', pa.string()),
                ('student_id', pa.string()),
                ('class_id', pa.string()),
                ('grade', pa.string()),
                ('enrollment_date', pa.string())
            ])
        )
    
    def _create_empty_parquet_if_not_exists(self, path: Path, schema: pa.Schema):
        """Create an empty Parquet file with the given schema if it doesn't exist."""
        if not path.exists():
            logger.info(f"Creating empty Parquet file: {path}")
            empty_table = pa.Table.from_arrays(
                [[] for _ in schema.names],
                schema=schema
            )
            pq.write_table(empty_table, path)
    
    def _read_parquet_to_df(self, path: Path) -> pd.DataFrame:
        """Read a Parquet file into a pandas DataFrame."""
        try:
            return pd.read_parquet(path)
        except Exception as e:
            logger.error(f"Error reading {path}: {e}")
            return pd.DataFrame()
    
    def _update_parquet_file(self, path: Path, new_data: pd.DataFrame, key_column: str):
        """
        Update a Parquet file with new data atomically, using file locking to prevent race conditions.
        
        Args:
            path: Path to the Parquet file
            new_data: DataFrame with new data to add
            key_column: Column name to use as primary key for duplicate handling
        """
        if new_data.empty:
            logger.debug(f"No new data to add to {path}")
            return
        
        # Create lock file path for synchronization
        lock_path = path.with_suffix('.lock')
        temp_path = path.with_suffix('.tmp')
        
        try:
            # Create a lock file and obtain an exclusive lock
            with open(lock_path, 'w') as lock_file:
                # Try to get an exclusive lock on the file
                try:
                    # Use OS-specific locking mechanism
                    # On Windows, we can't use fcntl, so we'll rely on the file existence as a lock
                    # On POSIX systems, this would use fcntl.flock(lock_file, fcntl.LOCK_EX)
                    
                    # Read existing data
                    existing_data = self._read_parquet_to_df(path)
                    
                    if existing_data.empty:
                        # If no existing data, write directly to a temp file
                        pq.write_table(pa.Table.from_pandas(new_data), temp_path)
                        # Atomic rename
                        temp_path.replace(path)
                        logger.info(f"Added {len(new_data)} new records to {path}")
                        return
                    
                    # Combine existing and new data, removing duplicates based on key column
                    # If duplicate keys exist, the new data takes precedence (keep='last')
                    combined_data = pd.concat([existing_data, new_data]).drop_duplicates(
                        subset=[key_column], keep='last'
                    ).reset_index(drop=True)
                    
                    # Write to a temporary file first
                    pq.write_table(pa.Table.from_pandas(combined_data), temp_path)
                    
                    # Atomic rename operation to replace the original file
                    temp_path.replace(path)
                    
                    # Calculate how many records were actually added (avoiding duplicates)
                    records_added = len(combined_data) - len(existing_data)
                    logger.info(f"Added {records_added} new records to {path} (atomically)")
                    
                finally:
                    # The lock is released when the file is closed
                    pass
        finally:
            # Clean up the lock file
            try:
                if lock_path.exists():
                    lock_path.unlink()
                if temp_path.exists():
                    temp_path.unlink()
            except Exception as e:
                logger.warning(f"Error cleaning up temporary files: {e}")
                # Continue execution - this is not critical
    
    def process_file(self, file_path: Path) -> bool:
        """
        Process a single JSON file and update Parquet files.
        
        Args:
            file_path: Path to the JSON file to process
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        logger.info(f"Processing file: {file_path}")
        
        # Create an in-progress lock file to prevent partial processing
        process_lock_file = file_path.with_suffix('.inprogress')
        
        try:
            # Create process lock
            with open(process_lock_file, 'w') as f:
                f.write(f"Processing started at {datetime.now().isoformat()}\n")
            
            # Verify the file still exists (it might have been processed by another instance)
            if not file_path.exists():
                logger.warning(f"File {file_path} no longer exists, skipping processing")
                return False
                
            # Read the JSON file with error handling for incomplete/corrupted files
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {file_path}: {e}")
                return False
            
            # Validate data using Pydantic model
            try:
                validated_data = SchoolData(**data)
            except Exception as e:
                logger.error(f"Data validation failed for {file_path}: {e}")
                return False
            
            # Transaction approach - only commit if all updates succeed
            transaction_success = True
            
            # Process students
            if validated_data.students:
                try:
                    students_df = pd.DataFrame([student.dict() for student in validated_data.students])
                    self._update_parquet_file(self.students_parquet, students_df, 'student_id')
                except Exception as e:
                    logger.error(f"Error processing students: {e}")
                    transaction_success = False
            
            # Process teachers
            if validated_data.teachers and transaction_success:
                try:
                    teachers_df = pd.DataFrame([teacher.dict() for teacher in validated_data.teachers])
                    self._update_parquet_file(self.teachers_parquet, teachers_df, 'teacher_id')
                except Exception as e:
                    logger.error(f"Error processing teachers: {e}")
                    transaction_success = False
            
            # Process classes
            if validated_data.classes and transaction_success:
                try:
                    classes_df = pd.DataFrame([cls.dict() for cls in validated_data.classes])
                    self._update_parquet_file(self.classes_parquet, classes_df, 'class_id')
                except Exception as e:
                    logger.error(f"Error processing classes: {e}")
                    transaction_success = False
            
            # Process student-class relationships
            if validated_data.student_classes and transaction_success:
                try:
                    student_classes_df = pd.DataFrame([sc.dict() for sc in validated_data.student_classes])
                    self._update_parquet_file(self.student_classes_parquet, student_classes_df, 'student_class_id')
                except Exception as e:
                    logger.error(f"Error processing student-class relationships: {e}")
                    transaction_success = False
            
            return transaction_success
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return False
            
        finally:
            # Clean up the in-progress lock file
            if process_lock_file.exists():
                try:
                    process_lock_file.unlink()
                except Exception as e:
                    logger.warning(f"Failed to remove process lock file: {e}")
    
    def stop(self):
        """
        Stop the listener gracefully, completing any in-progress operations.
        """
        self.running = False
        logger.info("Listener shutdown requested - will complete current operations before stopping")
        
        # Create a status file to indicate proper shutdown
        status_file = self.output_dir / "listener_shutdown.status"
        with open(status_file, 'w') as f:
            f.write(f"Shutdown requested at: {datetime.now().isoformat()}\n")
            f.write(f"Hostname: {socket.gethostname()}\n")
            f.write(f"PID: {os.getpid()}\n")
    
    def run(self):
        """
        Start the continuous monitoring and processing loop.
        """
        global running_instance
        running_instance = self
        self.running = True
        self.start_time = datetime.now()
        self.processed_count = 0
        self.failed_count = 0
        self.skipped_count = 0
        
        # Create status file
        status_file = self.output_dir / "listener_status.json"
        
        logger.info("Starting continuous monitoring for new JSON files...")
        
        # Track completion marker
        completion_marker_path = self.input_dir / "GENERATION_COMPLETE"
        completion_detected = False
        last_status_update = datetime.now() - timedelta(seconds=10)  # Force initial update
        last_status_report = datetime.now() - timedelta(seconds=60)  # Force initial report
        
        # Set of files currently being processed by other instances
        currently_processing = set()
        
        while self.running:
            try:
                # Periodic status update (every 5 seconds)
                now = datetime.now()
                if (now - last_status_update).total_seconds() >= 5:
                    self._update_status_file(status_file)
                    last_status_update = now
                
                # Periodic status report (every 60 seconds)
                if (now - last_status_report).total_seconds() >= 60:
                    self._report_status()
                    last_status_report = now
                
                # Get list of JSON files in the input directory, ignoring temporary files
                json_files = [f for f in list(self.input_dir.glob("*.json")) 
                             if not f.name.endswith('.tmp') and 
                                not f.name.endswith('.processing') and
                                not f.name.endswith('.inprogress')]
                
                if json_files:
                    logger.debug(f"Found {len(json_files)} JSON files to process")
                
                # Reset the processing set if it gets too large (cleanup for stale markers)
                if len(currently_processing) > 1000:  # Arbitrary limit to prevent memory issues
                    currently_processing = set()
                
                for file_path in json_files:
                    # Skip if shutdown was requested
                    if not self.running:
                        break
                        
                    # Create a processing marker to prevent duplicate processing
                    processing_marker = file_path.with_suffix('.processing')
                    
                    # Skip if already being processed by another instance
                    if processing_marker.exists() or str(file_path) in currently_processing:
                        if str(file_path) not in currently_processing:
                            currently_processing.add(str(file_path))
                            self.skipped_count += 1
                        logger.debug(f"Skipping {file_path} - already being processed")
                        continue
                    
                    # Add to currently processing set
                    currently_processing.add(str(file_path))
                    
                    try:
                        # Create processing marker with metadata
                        with open(processing_marker, 'w') as f:
                            f.write(f"Processing started: {datetime.now().isoformat()}\n")
                            f.write(f"Host: {socket.gethostname()}\n")
                            f.write(f"PID: {os.getpid()}\n")
                        
                        # Try to process the file with retries
                        success = False
                        for attempt in range(self.max_retries):
                            if self.process_file(file_path):
                                success = True
                                break
                            else:
                                logger.warning(f"Retry {attempt + 1}/{self.max_retries} for {file_path}")
                                time.sleep(1)  # Wait before retrying
                        
                        # Handle the processed file
                        if success:
                            self.processed_count += 1
                            if self.processed_dir:
                                # Move to processed directory atomically
                                dest_path = self.processed_dir / file_path.name
                                temp_dest_path = dest_path.with_suffix('.tmp')
                                
                                # Create the processed directory if it doesn't exist
                                self.processed_dir.mkdir(parents=True, exist_ok=True)
                                
                                # Copy to temp location first
                                shutil.copy2(str(file_path), str(temp_dest_path))
                                # Atomic rename
                                temp_dest_path.replace(dest_path)
                                # Remove original only after successful copy
                                file_path.unlink()
                                
                                logger.info(f"Moved processed file to {dest_path} (atomically)")
                            else:
                                # Delete the file
                                file_path.unlink()
                                logger.info(f"Deleted processed file {file_path}")
                        else:
                            self.failed_count += 1
                            logger.error(f"Failed to process {file_path} after {self.max_retries} attempts")
                    
                    except Exception as e:
                        self.failed_count += 1
                        logger.error(f"Unexpected error processing {file_path}: {e}")
                    
                    finally:
                        # Always clean up the processing marker and remove from tracking set
                        if processing_marker.exists():
                            try:
                                processing_marker.unlink()
                            except Exception as e:
                                logger.warning(f"Failed to remove processing marker: {e}")
                        
                        # Remove from the tracking set
                        currently_processing.discard(str(file_path))
                
                # Check for completion marker
                if completion_marker_path.exists() and not completion_detected:
                    completion_detected = True
                    logger.info("Generator completion marker found")
                    
                    # Log additional details from the marker
                    try:
                        with open(completion_marker_path, 'r') as f:
                            marker_contents = f.read()
                            logger.info(f"Completion marker contents: {marker_contents}")
                    except Exception:
                        pass
                
                # Check if we've completed processing after completion marker was detected
                if completion_detected:
                    remaining_files = len([f for f in list(self.input_dir.glob("*.json")) 
                                          if not f.name.endswith('.tmp') and
                                             not f.name.endswith('.processing') and
                                             not f.name.endswith('.inprogress')])
                    
                    if remaining_files == 0:
                        logger.info("All files processed after generator completion")
                        # We don't exit here - just log the info and continue monitoring
                
                # Sleep before the next check
                time.sleep(self.check_interval)
            
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Unexpected error in monitoring loop: {e}")
                time.sleep(self.check_interval)  # Sleep before retry
        
        # Final status update before exit
        self._update_status_file(status_file, final=True)
        self._report_status(final=True)
        logger.info("Listener shutdown complete")
    
    def _update_status_file(self, status_file: Path, final: bool = False):
        """
        Update the status file with current processing statistics.
        """
        status = {
            "timestamp": datetime.now().isoformat(),
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "running_since": self.start_time.isoformat(),
            "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
            "processed_count": self.processed_count,
            "failed_count": self.failed_count,
            "skipped_count": self.skipped_count,
            "status": "shutting_down" if final else "running"
        }
        
        # Write to temp file first, then rename for atomic update
        temp_status_file = status_file.with_suffix('.tmp')
        with open(temp_status_file, 'w') as f:
            json.dump(status, f, indent=2)
        
        # Atomic rename
        temp_status_file.replace(status_file)
    
    def _report_status(self, final: bool = False):
        """
        Log a status report with current statistics.
        """
        uptime = datetime.now() - self.start_time
        uptime_str = f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds // 60) % 60}m {uptime.seconds % 60}s"
        
        if final:
            logger.info(f"=== FINAL STATUS REPORT ===")
        else:
            logger.info(f"=== PERIODIC STATUS REPORT ===")
            
        logger.info(f"Uptime: {uptime_str}")
        logger.info(f"Files processed successfully: {self.processed_count}")
        logger.info(f"Files failed: {self.failed_count}")
        logger.info(f"Files skipped (already processing): {self.skipped_count}")
        
        if final:
            logger.info(f"Listener shutting down")
        logger.info(f"============================")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor a directory for JSON files and process them into Parquet files")
    parser.add_argument("--input-dir", default="./input", help="Directory to monitor for JSON files")
    parser.add_argument("--output-dir", default="./output", help="Directory to save Parquet files")
    parser.add_argument("--processed-dir", default=None, help="Directory to move processed files (if not specified, files are deleted)")
    parser.add_argument("--interval", type=float, default=1.0, help="Check interval in seconds")
    parser.add_argument("--max-retries", type=int, default=3, help="Maximum retries for processing a file")
    
    args = parser.parse_args()
    
    listener = ResultListener(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        processed_dir=args.processed_dir,
        check_interval=args.interval,
        max_retries=args.max_retries
    )
    
    listener.run()


if __name__ == "__main__":
    main()
