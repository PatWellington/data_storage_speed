#!/usr/bin/env python3
"""
Adaptive Result Listener with Dynamic Schema Detection

This script automatically detects JSON data structures and creates appropriate
Parquet files with relationship mapping and schema documentation.
"""

import os
import time
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Set, Tuple
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
from pathlib import Path
from dataclasses import dataclass, field
from collections import defaultdict, Counter
import re
import threading


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("adaptive_listener.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AdaptiveListener")


@dataclass
class FieldMetadata:
    """Metadata about a detected field."""
    field_name: str
    data_types: Set[str] = field(default_factory=set)
    nullable: bool = False
    max_length: Optional[int] = None
    is_list: bool = False
    list_element_types: Set[str] = field(default_factory=set)
    sample_values: List[Any] = field(default_factory=list)
    foreign_key_candidate: Optional[str] = None  # Points to potential parent table
    
    def add_sample(self, value: Any):
        """Add a sample value and update metadata."""
        if value is None:
            self.nullable = True
            return
            
        if isinstance(value, list):
            self.is_list = True
            for item in value:
                if item is not None:
                    self.list_element_types.add(type(item).__name__)
        else:
            self.data_types.add(type(value).__name__)
            
            if isinstance(value, str):
                if self.max_length is None:
                    self.max_length = len(value)
                else:
                    self.max_length = max(self.max_length, len(value))
        
        # Keep limited samples
        if len(self.sample_values) < 10:
            self.sample_values.append(value)
    
    def get_arrow_type(self) -> pa.DataType:
        """Convert to Arrow data type."""
        if self.is_list:
            if 'str' in self.list_element_types:
                return pa.list_(pa.string())
            elif 'int' in self.list_element_types:
                return pa.list_(pa.int64())
            elif 'float' in self.list_element_types:
                return pa.list_(pa.float64())
            else:
                return pa.list_(pa.string())  # Default to string list
        
        # Handle primitive types
        if 'int' in self.data_types and 'float' not in self.data_types:
            return pa.int64()
        elif 'float' in self.data_types or ('int' in self.data_types and 'float' in self.data_types):
            return pa.float64()
        elif 'bool' in self.data_types:
            return pa.bool_()
        else:
            return pa.string()  # Default to string


@dataclass
class TableSchema:
    """Schema information for a detected table."""
    table_name: str
    fields: Dict[str, FieldMetadata] = field(default_factory=dict)
    primary_key: Optional[str] = None
    foreign_keys: Dict[str, str] = field(default_factory=dict)  # field_name -> referenced_table
    record_count: int = 0
    
    def add_record(self, record: Dict[str, Any]):
        """Add a record and update schema."""
        self.record_count += 1
        
        for field_name, value in record.items():
            if field_name not in self.fields:
                self.fields[field_name] = FieldMetadata(field_name=field_name)
            
            self.fields[field_name].add_sample(value)
    
    def detect_keys(self, all_schemas: Dict[str, 'TableSchema']):
        """Detect primary and foreign keys."""
        # Detect primary key (field ending with _id that's unique-ish)
        for field_name, metadata in self.fields.items():
            if field_name.endswith('_id') and not metadata.nullable:
                if self.primary_key is None:  # Take the first ID field as primary key
                    self.primary_key = field_name
                elif field_name == f"{self.table_name}_id":  # Prefer table_name_id pattern
                    self.primary_key = field_name
        
        # Detect foreign keys
        for field_name, metadata in self.fields.items():
            if field_name.endswith('_id') and field_name != self.primary_key:
                # Look for a table that might match this foreign key
                potential_table = field_name.replace('_id', '')
                
                # Try plural/singular variations
                potential_tables = [
                    potential_table,
                    potential_table + 's',
                    potential_table[:-1] if potential_table.endswith('s') else potential_table,
                ]
                
                for pot_table in potential_tables:
                    if pot_table in all_schemas:
                        self.foreign_keys[field_name] = pot_table
                        metadata.foreign_key_candidate = pot_table
                        break
    
    def to_arrow_schema(self) -> pa.Schema:
        """Convert to Arrow schema."""
        arrow_fields = []
        for field_name, metadata in self.fields.items():
            arrow_type = metadata.get_arrow_type()
            arrow_fields.append(pa.field(field_name, arrow_type, nullable=metadata.nullable))
        
        return pa.schema(arrow_fields)


class SchemaDetector:
    """Detects and evolves schemas from JSON data."""
    
    def __init__(self):
        self.schemas: Dict[str, TableSchema] = {}
        self.lock = threading.RLock()
        self.sample_limit = 100  # Number of samples to analyze for schema detection
        
    def analyze_json_structure(self, json_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Extract table-like structures from JSON."""
        tables = {}
        
        # Handle nested structure (e.g., {"data": {...}, "metadata": {...}})
        if 'data' in json_data and isinstance(json_data['data'], dict):
            json_data = json_data['data']
        
        for key, value in json_data.items():
            if isinstance(value, list) and value:
                # Check if it's a list of objects (table-like)
                if isinstance(value[0], dict):
                    tables[key] = value
                else:
                    # List of primitives - create a simple table
                    tables[key] = [{"value": item, "index": i} for i, item in enumerate(value)]
            elif isinstance(value, dict):
                # Single object - treat as a table with one record
                tables[key] = [value]
        
        return tables
    
    def update_schemas(self, json_data: Dict[str, Any]):
        """Update schemas based on new JSON data."""
        with self.lock:
            tables = self.analyze_json_structure(json_data)
            
            for table_name, records in tables.items():
                if table_name not in self.schemas:
                    self.schemas[table_name] = TableSchema(table_name=table_name)
                
                schema = self.schemas[table_name]
                
                # Analyze records (limit samples to avoid memory issues)
                sample_records = records[:self.sample_limit] if len(records) > self.sample_limit else records
                
                for record in sample_records:
                    schema.add_record(record)
            
            # After updating all schemas, detect relationships
            for schema in self.schemas.values():
                schema.detect_keys(self.schemas)
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get a summary of detected schemas."""
        with self.lock:
            summary = {
                "detection_timestamp": datetime.now().isoformat(),
                "total_tables": len(self.schemas),
                "tables": {}
            }
            
            for table_name, schema in self.schemas.items():
                table_info = {
                    "record_count": schema.record_count,
                    "field_count": len(schema.fields),
                    "primary_key": schema.primary_key,
                    "foreign_keys": schema.foreign_keys,
                    "fields": {}
                }
                
                for field_name, metadata in schema.fields.items():
                    table_info["fields"][field_name] = {
                        "data_types": list(metadata.data_types),
                        "nullable": metadata.nullable,
                        "is_list": metadata.is_list,
                        "max_length": metadata.max_length,
                        "foreign_key_to": metadata.foreign_key_candidate,
                        "sample_values": metadata.sample_values[:3]  # First 3 samples
                    }
                
                summary["tables"][table_name] = table_info
            
            return summary
    
    def get_relationship_graph(self) -> Dict[str, Any]:
        """Generate a relationship graph between tables."""
        with self.lock:
            relationships = []
            
            for table_name, schema in self.schemas.items():
                for fk_field, referenced_table in schema.foreign_keys.items():
                    relationships.append({
                        "from_table": table_name,
                        "to_table": referenced_table,
                        "foreign_key": fk_field,
                        "relationship_type": "one_to_many"  # Assumption
                    })
            
            return {
                "tables": list(self.schemas.keys()),
                "relationships": relationships,
                "graph_metadata": {
                    "total_tables": len(self.schemas),
                    "total_relationships": len(relationships),
                    "generation_time": datetime.now().isoformat()
                }
            }


class AdaptiveResultListener:
    """Adaptive listener that automatically handles any JSON structure."""
    
    def __init__(
        self, 
        input_dir: str, 
        output_dir: str,
        processed_dir: str = None,
        check_interval: float = 1.0,
        max_retries: int = 3,
        schema_detection_samples: int = 50
    ):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.processed_dir = Path(processed_dir) if processed_dir else None
        self.check_interval = check_interval
        self.max_retries = max_retries
        
        # Create directories
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.processed_dir:
            self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Schema detection
        self.schema_detector = SchemaDetector()
        self.schema_detection_complete = False
        self.schema_detection_samples = schema_detection_samples
        self.processed_files_count = 0
        
        # Track parquet files
        self.parquet_files: Dict[str, Path] = {}
        
        logger.info(f"Adaptive listener initialized")
        logger.info(f"Input: {input_dir}, Output: {output_dir}")
    
    def detect_schema_phase(self):
        """Run initial schema detection phase."""
        logger.info(f"Starting schema detection phase (analyzing {self.schema_detection_samples} files)")
        
        files_analyzed = 0
        json_files = list(self.input_dir.glob("*.json"))
        
        for file_path in json_files[:self.schema_detection_samples]:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                self.schema_detector.update_schemas(data)
                files_analyzed += 1
                
                if files_analyzed % 10 == 0:
                    logger.info(f"Analyzed {files_analyzed} files for schema detection")
                    
            except Exception as e:
                logger.warning(f"Error analyzing {file_path} for schema: {e}")
        
        self.schema_detection_complete = True
        
        # Log detected schemas
        schema_summary = self.schema_detector.get_schema_summary()
        logger.info(f"Schema detection complete. Detected {schema_summary['total_tables']} tables:")
        
        for table_name, table_info in schema_summary['tables'].items():
            logger.info(f"  - {table_name}: {table_info['field_count']} fields, "
                       f"PK: {table_info['primary_key']}, "
                       f"FKs: {list(table_info['foreign_keys'].keys())}")
        
        # Save schema documentation
        self.save_schema_documentation()
        
        # Initialize parquet files
        self.initialize_parquet_files()
    
    def save_schema_documentation(self):
        """Save schema and relationship documentation."""
        schema_summary = self.schema_detector.get_schema_summary()
        relationship_graph = self.schema_detector.get_relationship_graph()
        
        # Save detailed schema
        schema_file = self.output_dir / "schema_documentation.json"
        with open(schema_file, 'w') as f:
            json.dump(schema_summary, f, indent=2)
        
        # Save relationship graph
        relationships_file = self.output_dir / "table_relationships.json"
        with open(relationships_file, 'w') as f:
            json.dump(relationship_graph, f, indent=2)
        
        # Save human-readable documentation
        readme_file = self.output_dir / "DATA_STRUCTURE_README.md"
        self.generate_readme(schema_summary, relationship_graph, readme_file)
        
        logger.info(f"Schema documentation saved to {self.output_dir}")
    
    def generate_readme(self, schema_summary: Dict, relationship_graph: Dict, readme_path: Path):
        """Generate human-readable documentation."""
        content = f"""# Data Structure Documentation

Generated on: {schema_summary['detection_timestamp']}
Total Tables: {schema_summary['total_tables']}

## Table Overview

"""
        
        for table_name, table_info in schema_summary['tables'].items():
            content += f"### {table_name.title()}\n\n"
            content += f"- **Records**: {table_info['record_count']}\n"
            content += f"- **Primary Key**: {table_info['primary_key'] or 'None detected'}\n"
            
            if table_info['foreign_keys']:
                content += "- **Foreign Keys**:\n"
                for fk, ref_table in table_info['foreign_keys'].items():
                    content += f"  - `{fk}` → `{ref_table}`\n"
            
            content += "\n**Fields**:\n\n"
            content += "| Field | Type | Nullable | Description |\n"
            content += "|-------|------|----------|-------------|\n"
            
            for field_name, field_info in table_info['fields'].items():
                types_str = ", ".join(field_info['data_types'])
                if field_info['is_list']:
                    types_str = f"List[{types_str}]"
                
                nullable = "Yes" if field_info['nullable'] else "No"
                
                description = ""
                if field_info['foreign_key_to']:
                    description = f"References {field_info['foreign_key_to']}"
                elif field_name.endswith('_id'):
                    description = "Identifier field"
                elif 'date' in field_name.lower():
                    description = "Date field"
                
                content += f"| `{field_name}` | {types_str} | {nullable} | {description} |\n"
            
            content += "\n"
        
        content += "## Relationships\n\n"
        
        if relationship_graph['relationships']:
            content += "```\n"
            for rel in relationship_graph['relationships']:
                content += f"{rel['from_table']}.{rel['foreign_key']} -> {rel['to_table']}\n"
            content += "```\n"
        else:
            content += "No relationships detected.\n"
        
        content += f"\n## Files\n\n"
        content += f"- `schema_documentation.json`: Detailed schema information\n"
        content += f"- `table_relationships.json`: Relationship graph data\n"
        content += f"- `*.parquet`: Data files for each table\n"
        
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write(content)
    
    def initialize_parquet_files(self):
        """Initialize empty parquet files for each detected table."""
        for table_name, schema in self.schema_detector.schemas.items():
            parquet_path = self.output_dir / f"{table_name}.parquet"
            self.parquet_files[table_name] = parquet_path
            
            if not parquet_path.exists():
                # Create empty parquet file with proper schema
                arrow_schema = schema.to_arrow_schema()
                empty_table = pa.Table.from_arrays(
                    [[] for _ in arrow_schema.names],
                    schema=arrow_schema
                )
                pq.write_table(empty_table, parquet_path)
                logger.info(f"Created empty parquet file: {parquet_path}")
    
    def process_file(self, file_path: Path) -> bool:
        """Process a single JSON file."""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract table data
            tables = self.schema_detector.analyze_json_structure(data)
            
            # Process each table
            for table_name, records in tables.items():
                if table_name in self.parquet_files and records:
                    self.update_parquet_file(table_name, records)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return False
    
    def update_parquet_file(self, table_name: str, new_records: List[Dict[str, Any]]):
        """Update a parquet file with new records."""
        if not new_records:
            return
        
        parquet_path = self.parquet_files[table_name]
        schema = self.schema_detector.schemas[table_name]
        
        try:
            # Convert records to DataFrame
            df_new = pd.DataFrame(new_records)
            
            # Ensure all expected columns exist
            for field_name in schema.fields.keys():
                if field_name not in df_new.columns:
                    df_new[field_name] = None
            
            # Read existing data
            if parquet_path.exists():
                try:
                    df_existing = pd.read_parquet(parquet_path)
                except:
                    df_existing = pd.DataFrame()
            else:
                df_existing = pd.DataFrame()
            
            # Combine data
            if not df_existing.empty:
                # Handle potential duplicate keys
                if schema.primary_key and schema.primary_key in df_new.columns:
                    # Remove duplicates based on primary key, keeping new data
                    combined_df = pd.concat([df_existing, df_new]).drop_duplicates(
                        subset=[schema.primary_key], keep='last'
                    ).reset_index(drop=True)
                else:
                    combined_df = pd.concat([df_existing, df_new]).reset_index(drop=True)
            else:
                combined_df = df_new
            
            # Write back to parquet
            combined_df.to_parquet(parquet_path, index=False)
            
            records_added = len(df_new)
            logger.info(f"Added {records_added} records to {table_name}.parquet")
            
        except Exception as e:
            logger.error(f"Error updating {table_name}.parquet: {e}")
    
    def run(self):
        """Run the adaptive listener."""
        logger.info("Starting adaptive result listener...")
        
        # First phase: Schema detection
        if not self.schema_detection_complete:
            self.detect_schema_phase()
        
        # Second phase: Continuous processing
        logger.info("Starting continuous processing phase...")
        
        while True:
            try:
                json_files = list(self.input_dir.glob("*.json"))
                
                if json_files:
                    logger.info(f"Found {len(json_files)} JSON files to process")
                
                for file_path in json_files:
                    # Skip if we're still in schema detection and already processed
                    success = False
                    for attempt in range(self.max_retries):
                        if self.process_file(file_path):
                            success = True
                            break
                        else:
                            logger.warning(f"Retry {attempt + 1}/{self.max_retries} for {file_path}")
                            time.sleep(1)
                    
                    # Handle processed file
                    if success:
                        if self.processed_dir:
                            dest_path = self.processed_dir / file_path.name
                            shutil.move(str(file_path), str(dest_path))
                        else:
                            file_path.unlink()
                        
                        self.processed_files_count += 1
                        
                        # Periodically update documentation
                        if self.processed_files_count % 100 == 0:
                            self.save_schema_documentation()
                            logger.info(f"Updated documentation after processing {self.processed_files_count} files")
                    
                    else:
                        logger.error(f"Failed to process {file_path} after {self.max_retries} attempts")
                
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("Adaptive listener stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(self.check_interval)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Adaptive JSON to Parquet processor")
    parser.add_argument("--input-dir", default="./input", help="Directory to monitor for JSON files")
    parser.add_argument("--output-dir", default="./output", help="Directory to save Parquet files")
    parser.add_argument("--processed-dir", default=None, help="Directory to move processed files")
    parser.add_argument("--interval", type=float, default=1.0, help="Check interval in seconds")
    parser.add_argument("--max-retries", type=int, default=3, help="Maximum retries for processing")
    parser.add_argument("--schema-samples", type=int, default=50, help="Files to analyze for schema detection")
    
    args = parser.parse_args()
    
    listener = AdaptiveResultListener(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        processed_dir=args.processed_dir,
        check_interval=args.interval,
        max_retries=args.max_retries,
        schema_detection_samples=args.schema_samples
    )
    
    listener.run()


if __name__ == "__main__":
    main()
