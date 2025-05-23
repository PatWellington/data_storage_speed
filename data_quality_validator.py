#!/usr/bin/env python3
"""
Data Quality Validator with Referential Integrity Checks

This module provides comprehensive data quality validation including
referential integrity, capacity constraints, and relationship validation.
"""

import pandas as pd
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, Counter
import re


logger = logging.getLogger("DataQualityValidator")


@dataclass
class ValidationResult:
    """Result of a validation check."""
    check_name: str
    passed: bool
    message: str
    severity: str = "ERROR"  # ERROR, WARNING, INFO
    affected_records: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityReport:
    """Comprehensive data quality report."""
    timestamp: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    warnings: int
    errors: int
    results: List[ValidationResult] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    
    def add_result(self, result: ValidationResult):
        """Add a validation result."""
        self.results.append(result)
        self.total_checks += 1
        
        if result.passed:
            self.passed_checks += 1
        else:
            self.failed_checks += 1
            
        if result.severity == "ERROR":
            self.errors += 1
        elif result.severity == "WARNING":
            self.warnings += 1
    
    def get_score(self) -> float:
        """Get overall quality score (0-100)."""
        if self.total_checks == 0:
            return 100.0
        
        # Weight errors more heavily than warnings
        error_penalty = self.errors * 2
        warning_penalty = self.warnings * 0.5
        
        max_penalty = self.total_checks * 2
        actual_penalty = min(error_penalty + warning_penalty, max_penalty)
        
        return max(0.0, 100.0 - (actual_penalty / max_penalty * 100))


class DataQualityValidator:
    """Comprehensive data quality validator."""
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.tables: Dict[str, pd.DataFrame] = {}
        self.schema_info: Dict[str, Any] = {}
        self.relationships: List[Dict[str, str]] = []
        
        # Load schema information if available
        self.load_schema_info()
    
    def load_schema_info(self):
        """Load schema and relationship information."""
        try:
            schema_file = self.output_dir / "schema_documentation.json"
            if schema_file.exists():
                with open(schema_file, 'r') as f:
                    self.schema_info = json.load(f)
            
            relationships_file = self.output_dir / "table_relationships.json"
            if relationships_file.exists():
                with open(relationships_file, 'r') as f:
                    rel_data = json.load(f)
                    self.relationships = rel_data.get("relationships", [])
                    
        except Exception as e:
            logger.warning(f"Could not load schema info: {e}")
    
    def load_tables(self) -> bool:
        """Load all parquet tables."""
        try:
            parquet_files = list(self.output_dir.glob("*.parquet"))
            
            for file_path in parquet_files:
                table_name = file_path.stem
                try:
                    df = pd.read_parquet(file_path)
                    self.tables[table_name] = df
                    logger.info(f"Loaded table {table_name}: {len(df)} records")
                except Exception as e:
                    logger.error(f"Failed to load {file_path}: {e}")
                    return False
            
            logger.info(f"Loaded {len(self.tables)} tables for validation")
            return True
            
        except Exception as e:
            logger.error(f"Error loading tables: {e}")
            return False
    
    def validate_referential_integrity(self, report: QualityReport):
        """Validate referential integrity between tables."""
        logger.info("Validating referential integrity...")
        
        for relationship in self.relationships:
            from_table = relationship["from_table"]
            to_table = relationship["to_table"]
            foreign_key = relationship["foreign_key"]
            
            if from_table not in self.tables or to_table not in self.tables:
                report.add_result(ValidationResult(
                    check_name=f"Table Existence for {from_table} → {to_table}",
                    passed=False,
                    message=f"Missing table(s) for relationship validation",
                    severity="ERROR"
                ))
                continue
            
            from_df = self.tables[from_table]
            to_df = self.tables[to_table]
            
            if foreign_key not in from_df.columns:
                report.add_result(ValidationResult(
                    check_name=f"Foreign Key Column {foreign_key}",
                    passed=False,
                    message=f"Foreign key column {foreign_key} not found in {from_table}",
                    severity="ERROR"
                ))
                continue
            
            # Determine the primary key of the target table
            to_table_schema = self.schema_info.get("tables", {}).get(to_table, {})
            primary_key = to_table_schema.get("primary_key")
            
            if not primary_key or primary_key not in to_df.columns:
                # Try to infer primary key
                potential_keys = [col for col in to_df.columns if col.endswith('_id')]
                if potential_keys:
                    primary_key = potential_keys[0]  # Use first ID column
                else:
                    report.add_result(ValidationResult(
                        check_name=f"Primary Key Detection for {to_table}",
                        passed=False,
                        message=f"Cannot determine primary key for table {to_table}",
                        severity="WARNING"
                    ))
                    continue
            
            # Check for orphaned foreign keys
            foreign_values = from_df[foreign_key].dropna()
            primary_values = set(to_df[primary_key].dropna())
            
            orphaned = foreign_values[~foreign_values.isin(primary_values)]
            
            if len(orphaned) > 0:
                orphaned_records = from_df[from_df[foreign_key].isin(orphaned)]
                orphaned_ids = []
                
                # Try to get record identifiers
                if 'id' in from_df.columns:
                    orphaned_ids = orphaned_records['id'].tolist()
                elif f"{from_table}_id" in from_df.columns:
                    orphaned_ids = orphaned_records[f"{from_table}_id"].tolist()
                
                report.add_result(ValidationResult(
                    check_name=f"Referential Integrity: {from_table}.{foreign_key} → {to_table}.{primary_key}",
                    passed=False,
                    message=f"Found {len(orphaned)} orphaned foreign key references",
                    severity="ERROR",
                    affected_records=orphaned_ids[:10],  # Limit to first 10
                    details={
                        "orphaned_count": len(orphaned),
                        "orphaned_values": orphaned.unique().tolist()[:10],
                        "total_foreign_keys": len(foreign_values),
                        "total_primary_keys": len(primary_values)
                    }
                ))
            else:
                report.add_result(ValidationResult(
                    check_name=f"Referential Integrity: {from_table}.{foreign_key} → {to_table}.{primary_key}",
                    passed=True,
                    message=f"All foreign key references are valid",
                    severity="INFO",
                    details={
                        "total_references": len(foreign_values),
                        "unique_references": len(foreign_values.unique())
                    }
                ))
    
    def validate_capacity_constraints(self, report: QualityReport):
        """Validate class capacity constraints."""
        logger.info("Validating capacity constraints...")
        
        if "classes" not in self.tables or "student_classes" not in self.tables:
            report.add_result(ValidationResult(
                check_name="Capacity Validation Setup",
                passed=False,
                message="Missing classes or student_classes tables for capacity validation",
                severity="WARNING"
            ))
            return
        
        classes_df = self.tables["classes"]
        enrollments_df = self.tables["student_classes"]
        
        if "class_id" not in enrollments_df.columns or "max_capacity" not in classes_df.columns:
            report.add_result(ValidationResult(
                check_name="Capacity Validation Columns",
                passed=False,
                message="Missing required columns for capacity validation",
                severity="WARNING"
            ))
            return
        
        # Count enrollments per class
        enrollment_counts = enrollments_df["class_id"].value_counts()
        
        over_capacity = []
        
        for _, class_row in classes_df.iterrows():
            class_id = class_row.get("class_id")
            max_capacity = class_row.get("max_capacity", 0)
            current_enrollment = enrollment_counts.get(class_id, 0)
            
            if current_enrollment > max_capacity:
                over_capacity.append({
                    "class_id": class_id,
                    "current": current_enrollment,
                    "max": max_capacity,
                    "excess": current_enrollment - max_capacity
                })
        
        if over_capacity:
            report.add_result(ValidationResult(
                check_name="Class Capacity Constraints",
                passed=False,
                message=f"Found {len(over_capacity)} classes over capacity",
                severity="ERROR",
                affected_records=[item["class_id"] for item in over_capacity],
                details={
                    "over_capacity_classes": over_capacity,
                    "total_classes": len(classes_df),
                    "max_excess": max(item["excess"] for item in over_capacity)
                }
            ))
        else:
            report.add_result(ValidationResult(
                check_name="Class Capacity Constraints",
                passed=True,
                message="All classes are within capacity limits",
                severity="INFO",
                details={
                    "total_classes_checked": len(classes_df),
                    "total_enrollments": len(enrollments_df)
                }
            ))
    
    def validate_data_consistency(self, report: QualityReport):
        """Validate data consistency within and across tables."""
        logger.info("Validating data consistency...")
        
        for table_name, df in self.tables.items():
            # Check for duplicate primary keys
            table_schema = self.schema_info.get("tables", {}).get(table_name, {})
            primary_key = table_schema.get("primary_key")
            
            if primary_key and primary_key in df.columns:
                duplicates = df[df.duplicated(subset=[primary_key], keep=False)]
                
                if len(duplicates) > 0:
                    report.add_result(ValidationResult(
                        check_name=f"Primary Key Uniqueness: {table_name}.{primary_key}",
                        passed=False,
                        message=f"Found {len(duplicates)} duplicate primary key values",
                        severity="ERROR",
                        affected_records=duplicates[primary_key].tolist()[:10],
                        details={
                            "duplicate_count": len(duplicates),
                            "unique_duplicates": len(duplicates[primary_key].unique())
                        }
                    ))
                else:
                    report.add_result(ValidationResult(
                        check_name=f"Primary Key Uniqueness: {table_name}.{primary_key}",
                        passed=True,
                        message="All primary key values are unique",
                        severity="INFO"
                    ))
            
            # Check for null values in critical fields
            critical_fields = [col for col in df.columns if col.endswith('_id')]
            
            for field in critical_fields:
                null_count = df[field].isnull().sum()
                null_percentage = (null_count / len(df)) * 100 if len(df) > 0 else 0
                
                if null_percentage > 5:  # More than 5% nulls in ID fields is concerning
                    report.add_result(ValidationResult(
                        check_name=f"Null Values in Critical Field: {table_name}.{field}",
                        passed=False,
                        message=f"{null_percentage:.2f}% null values in critical field",
                        severity="WARNING" if null_percentage < 20 else "ERROR",
                        details={
                            "null_count": null_count,
                            "total_records": len(df),
                            "null_percentage": null_percentage
                        }
                    ))
    
    def validate_business_logic(self, report: QualityReport):
        """Validate business logic rules."""
        logger.info("Validating business logic...")
        
        # Student enrollment limits
        if "student_classes" in self.tables:
            enrollments_df = self.tables["student_classes"]
            
            if "student_id" in enrollments_df.columns:
                enrollment_counts = enrollments_df["student_id"].value_counts()
                over_enrolled = enrollment_counts[enrollment_counts > 8]  # Assume 8 is max reasonable
                
                if len(over_enrolled) > 0:
                    report.add_result(ValidationResult(
                        check_name="Student Enrollment Limits",
                        passed=False,
                        message=f"Found {len(over_enrolled)} students with excessive enrollments",
                        severity="WARNING",
                        affected_records=over_enrolled.index.tolist()[:10],
                        details={
                            "max_enrollments": over_enrolled.max(),
                            "avg_enrollments": enrollment_counts.mean(),
                            "students_over_limit": len(over_enrolled)
                        }
                    ))
                else:
                    report.add_result(ValidationResult(
                        check_name="Student Enrollment Limits",
                        passed=True,
                        message="All students have reasonable enrollment counts",
                        severity="INFO"
                    ))
        
        # Age and grade consistency for students
        if "students" in self.tables:
            students_df = self.tables["students"]
            
            if "age" in students_df.columns and "year" in students_df.columns:
                # Rough validation: age should correlate with grade year
                inconsistent = students_df[
                    ((students_df["age"] < 14) & (students_df["year"] > 9)) |
                    ((students_df["age"] > 19) & (students_df["year"] < 12))
                ]
                
                if len(inconsistent) > 0:
                    report.add_result(ValidationResult(
                        check_name="Age-Grade Consistency",
                        passed=False,
                        message=f"Found {len(inconsistent)} students with inconsistent age/grade",
                        severity="WARNING",
                        details={
                            "inconsistent_count": len(inconsistent),
                            "total_students": len(students_df)
                        }
                    ))
                else:
                    report.add_result(ValidationResult(
                        check_name="Age-Grade Consistency",
                        passed=True,
                        message="Student ages are consistent with grade levels",
                        severity="INFO"
                    ))
    
    def validate_data_distribution(self, report: QualityReport):
        """Validate data distribution and detect anomalies."""
        logger.info("Validating data distribution...")
        
        for table_name, df in self.tables.items():
            if len(df) == 0:
                report.add_result(ValidationResult(
                    check_name=f"Table Population: {table_name}",
                    passed=False,
                    message=f"Table {table_name} is empty",
                    severity="WARNING"
                ))
                continue
            
            # Check for reasonable data distribution
            numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
            
            for col in numeric_columns:
                if col.endswith('_id'):
                    continue  # Skip ID columns
                
                col_data = df[col].dropna()
                if len(col_data) == 0:
                    continue
                
                # Check for suspicious patterns
                unique_values = len(col_data.unique())
                total_values = len(col_data)
                
                if unique_values == 1:
                    report.add_result(ValidationResult(
                        check_name=f"Data Variation: {table_name}.{col}",
                        passed=False,
                        message=f"Column has no variation (all values identical)",
                        severity="WARNING",
                        details={"constant_value": col_data.iloc[0]}
                    ))
                elif unique_values / total_values < 0.1 and total_values > 20:
                    report.add_result(ValidationResult(
                        check_name=f"Data Variation: {table_name}.{col}",
                        passed=False,
                        message=f"Column has very low variation ({unique_values}/{total_values} unique)",
                        severity="WARNING",
                        details={
                            "unique_ratio": unique_values / total_values,
                            "most_common_values": col_data.value_counts().head(3).to_dict()
                        }
                    ))
    
    def run_comprehensive_validation(self) -> QualityReport:
        """Run all validation checks and return comprehensive report."""
        from datetime import datetime
        
        report = QualityReport(
            timestamp=datetime.now().isoformat(),
            total_checks=0,
            passed_checks=0,
            failed_checks=0,
            warnings=0,
            errors=0
        )
        
        # Load tables
        if not self.load_tables():
            report.add_result(ValidationResult(
                check_name="Table Loading",
                passed=False,
                message="Failed to load one or more tables",
                severity="ERROR"
            ))
            return report
        
        # Run validation checks
        self.validate_referential_integrity(report)
        self.validate_capacity_constraints(report)
        self.validate_data_consistency(report)
        self.validate_business_logic(report)
        self.validate_data_distribution(report)
        
        # Generate summary
        report.summary = {
            "tables_validated": len(self.tables),
            "total_records": sum(len(df) for df in self.tables.values()),
            "quality_score": report.get_score(),
            "critical_issues": len([r for r in report.results if not r.passed and r.severity == "ERROR"]),
            "warnings": len([r for r in report.results if not r.passed and r.severity == "WARNING"]),
            "relationships_validated": len(self.relationships)
        }
        
        return report
    
    def save_validation_report(self, report: QualityReport, filename: str = "data_quality_report.json"):
        """Save validation report to file."""
        report_data = {
            "metadata": {
                "timestamp": report.timestamp,
                "validator_version": "1.0",
                "quality_score": report.get_score()
            },
            "summary": {
                "total_checks": report.total_checks,
                "passed_checks": report.passed_checks,
                "failed_checks": report.failed_checks,
                "warnings": report.warnings,
                "errors": report.errors,
                "overall_status": "PASS" if report.errors == 0 else "FAIL",
                **report.summary
            },
            "detailed_results": [
                {
                    "check_name": result.check_name,
                    "passed": result.passed,
                    "message": result.message,
                    "severity": result.severity,
                    "affected_records_count": len(result.affected_records),
                    "affected_records_sample": result.affected_records[:5],
                    "details": result.details
                }
                for result in report.results
            ]
        }
        
        report_path = self.output_dir / filename
        with open(report_path, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        logger.info(f"Validation report saved to {report_path}")
        
        # Also create a human-readable summary
        self.create_readable_report(report, self.output_dir / "QUALITY_REPORT.md")
    
    def create_readable_report(self, report: QualityReport, output_path: Path):
        """Create a human-readable quality report."""
        content = f"""# Data Quality Report

Generated: {report.timestamp}
Overall Quality Score: **{report.get_score():.1f}/100**

## Summary

- **Total Checks**: {report.total_checks}
- **Passed**: {report.passed_checks}
- **Failed**: {report.failed_checks}
- **Warnings**: {report.warnings}
- **Errors**: {report.errors}
- **Overall Status**: {'PASS' if report.errors == 0 else 'FAIL'}

## Tables Validated

- **Tables**: {report.summary.get('tables_validated', 0)}
- **Total Records**: {report.summary.get('total_records', 0):,}
- **Relationships**: {report.summary.get('relationships_validated', 0)}

## Issues Found

"""
        
        # Group results by severity
        errors = [r for r in report.results if not r.passed and r.severity == "ERROR"]
        warnings = [r for r in report.results if not r.passed and r.severity == "WARNING"]
        
        if errors:
            content += "### Critical Issues (Errors)\n\n"
            for error in errors:
                content += f"- **{error.check_name}**: {error.message}\n"
                if error.details:
                    for key, value in error.details.items():
                        content += f"  - {key}: {value}\n"
                content += "\n"
        
        if warnings:
            content += "### Warnings\n\n"
            for warning in warnings:
                content += f"- **{warning.check_name}**: {warning.message}\n"
                if warning.details:
                    for key, value in warning.details.items():
                        content += f"  - {key}: {value}\n"
                content += "\n"
        
        # Successful checks
        successes = [r for r in report.results if r.passed]
        if successes:
            content += "### Passed Checks\n\n"
            for success in successes:
                content += f"- **{success.check_name}**: {success.message}\n"
        
        content += f"\n## Recommendations\n\n"
        
        if report.errors > 0:
            content += "**Critical**: Address all error-level issues before using this data in production.\n\n"
        
        if report.warnings > 0:
            content += "**Important**: Review and resolve warning-level issues to improve data quality.\n\n"
        
        if report.get_score() > 90:
            content += "**Excellent**: Data quality is very high. Minor issues only.\n\n"
        elif report.get_score() > 70:
            content += "**Good**: Data quality is acceptable but could be improved.\n\n"
        else:
            content += "**Poor**: Data quality needs significant improvement.\n\n"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)


def main():
    """Standalone validation runner."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate data quality in parquet files")
    parser.add_argument("--output-dir", default="./output", help="Directory containing parquet files")
    parser.add_argument("--report-file", default="data_quality_report.json", help="Output report filename")
    
    args = parser.parse_args()
    
    validator = DataQualityValidator(args.output_dir)
    report = validator.run_comprehensive_validation()
    validator.save_validation_report(report, args.report_file)
    
    print(f"Validation complete. Quality Score: {report.get_score():.1f}/100")
    print(f"Errors: {report.errors}, Warnings: {report.warnings}")
    
    return 0 if report.errors == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
