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
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, validator, Field
import shutil
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
        Update a Parquet file with new data, handling duplicates based on key column.
        
        Args:
            path: Path to the Parquet file
            new_data: DataFrame with new data to add
            key_column: Column name to use as primary key for duplicate handling
        """
        if new_data.empty:
            logger.debug(f"No new data to add to {path}")
            return
            
        # Read existing data
        existing_data = self._read_parquet_to_df(path)
        
        if existing_data.empty:
            # If no existing data, just write the new data
            pq.write_table(pa.Table.from_pandas(new_data), path)
            logger.info(f"Added {len(new_data)} new records to {path}")
            return
        
        # Combine existing and new data, removing duplicates based on key column
        # If duplicate keys exist, the new data takes precedence (keep='last')
        combined_data = pd.concat([existing_data, new_data]).drop_duplicates(
            subset=[key_column], keep='last'
        ).reset_index(drop=True)
        
        # Write the combined data back to the Parquet file
        pq.write_table(pa.Table.from_pandas(combined_data), path)
        
        # Calculate how many records were actually added (avoiding duplicates)
        records_added = len(combined_data) - len(existing_data)
        logger.info(f"Added {records_added} new records to {path}")
    
    def process_file(self, file_path: Path) -> bool:
        """
        Process a single JSON file and update Parquet files.
        
        Args:
            file_path: Path to the JSON file to process
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        logger.info(f"Processing file: {file_path}")
        
        try:
            # Read the JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Validate data using Pydantic model
            validated_data = SchoolData(**data)
            
            # Process students
            if validated_data.students:
                students_df = pd.DataFrame([student.dict() for student in validated_data.students])
                self._update_parquet_file(self.students_parquet, students_df, 'student_id')
            
            # Process teachers
            if validated_data.teachers:
                teachers_df = pd.DataFrame([teacher.dict() for teacher in validated_data.teachers])
                self._update_parquet_file(self.teachers_parquet, teachers_df, 'teacher_id')
            
            # Process classes
            if validated_data.classes:
                classes_df = pd.DataFrame([cls.dict() for cls in validated_data.classes])
                self._update_parquet_file(self.classes_parquet, classes_df, 'class_id')
            
            # Process student-class relationships
            if validated_data.student_classes:
                student_classes_df = pd.DataFrame([sc.dict() for sc in validated_data.student_classes])
                self._update_parquet_file(self.student_classes_parquet, student_classes_df, 'student_class_id')
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return False
    
    def run(self):
        """
        Start the continuous monitoring and processing loop.
        """
        logger.info("Starting continuous monitoring for new JSON files...")
        
        while True:
            try:
                # Get list of JSON files in the input directory
                json_files = list(self.input_dir.glob("*.json"))
                
                if json_files:
                    logger.info(f"Found {len(json_files)} JSON files to process")
                
                for file_path in json_files:
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
                        if self.processed_dir:
                            # Move to processed directory
                            dest_path = self.processed_dir / file_path.name
                            shutil.move(str(file_path), str(dest_path))
                            logger.info(f"Moved processed file to {dest_path}")
                        else:
                            # Delete the file
                            file_path.unlink()
                            logger.info(f"Deleted processed file {file_path}")
                    else:
                        logger.error(f"Failed to process {file_path} after {self.max_retries} attempts")
                
                # Sleep before the next check
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("Received interrupt, shutting down...")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(self.check_interval)


def main():
    """Main entry point for the script."""
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
