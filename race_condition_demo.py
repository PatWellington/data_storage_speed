#!/usr/bin/env python3
"""
Race Condition Demonstration Script

This script demonstrates the race condition problems in the original system
and shows how the fixed version eliminates them.
"""

import subprocess
import time
import json
import pandas as pd
import logging
from pathlib import Path
import shutil
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RaceConditionDemo")


def run_generator(script_name: str, output_dir: str, num_files: int = 20, clean_first: bool = True):
    """Run a generator script and return statistics."""
    output_path = Path(output_dir)
    
    if clean_first and output_path.exists():
        shutil.rmtree(output_path)
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Running {script_name} to generate {num_files} files in {output_dir}")
    
    # Run the generator
    cmd = [
        "python", script_name,
        "--output-dir", output_dir,
        "--num-iterations", str(num_files),
        "--rate", "2.0"  # Fast generation for demo
    ]
    
    if "fixed" in script_name:
        cmd.append("--clean-state")  # Clean state for fixed version
    
    start_time = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    end_time = time.time()
    
    if result.returncode != 0:
        logger.error(f"Error running {script_name}: {result.stderr}")
        return None
    
    # Analyze generated files
    json_files = list(output_path.glob("*.json"))
    
    return {
        "script": script_name,
        "files_generated": len(json_files),
        "duration": end_time - start_time,
        "output_dir": output_dir,
        "json_files": json_files
    }


def analyze_race_conditions(json_files: list) -> dict:
    """Analyze JSON files for race condition issues."""
    analysis = {
        "total_files": len(json_files),
        "total_entities": {"students": 0, "teachers": 0, "classes": 0, "student_classes": 0},
        "orphaned_references": [],
        "capacity_violations": [],
        "duplicate_ids": {},
        "missing_dependencies": [],
        "data_integrity_score": 100.0
    }
    
    all_teachers = set()
    all_students = set()
    all_classes = set()
    class_capacities = {}
    class_enrollments = {}
    
    # Collect all entities and check for issues
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            # Handle nested structure
            if 'data' in data:
                entities = data['data']
            else:
                entities = data
            
            # Process each entity type
            for entity_type, entity_list in entities.items():
                if not isinstance(entity_list, list):
                    continue
                
                analysis["total_entities"][entity_type] += len(entity_list)
                
                for entity in entity_list:
                    if entity_type == "teachers":
                        teacher_id = entity.get("teacher_id")
                        if teacher_id:
                            if teacher_id in all_teachers:
                                analysis["duplicate_ids"].setdefault("teachers", []).append(teacher_id)
                                analysis["data_integrity_score"] -= 1
                            all_teachers.add(teacher_id)
                    
                    elif entity_type == "students":
                        student_id = entity.get("student_id")
                        if student_id:
                            if student_id in all_students:
                                analysis["duplicate_ids"].setdefault("students", []).append(student_id)
                                analysis["data_integrity_score"] -= 1
                            all_students.add(student_id)
                    
                    elif entity_type == "classes":
                        class_id = entity.get("class_id")
                        teacher_id = entity.get("teacher_id")
                        max_capacity = entity.get("max_capacity", 0)
                        
                        if class_id:
                            if class_id in all_classes:
                                analysis["duplicate_ids"].setdefault("classes", []).append(class_id)
                                analysis["data_integrity_score"] -= 1
                            all_classes.add(class_id)
                            class_capacities[class_id] = max_capacity
                            class_enrollments[class_id] = 0
                        
                        # Check if teacher exists
                        if teacher_id and teacher_id not in all_teachers:
                            analysis["orphaned_references"].append({
                                "type": "class_teacher",
                                "class_id": class_id,
                                "missing_teacher_id": teacher_id,
                                "file": json_file.name
                            })
                            analysis["data_integrity_score"] -= 5
                    
                    elif entity_type == "student_classes":
                        student_id = entity.get("student_id")
                        class_id = entity.get("class_id")
                        
                        # Check if student exists
                        if student_id and student_id not in all_students:
                            analysis["orphaned_references"].append({
                                "type": "enrollment_student",
                                "enrollment_id": entity.get("student_class_id"),
                                "missing_student_id": student_id,
                                "file": json_file.name
                            })
                            analysis["data_integrity_score"] -= 5
                        
                        # Check if class exists
                        if class_id and class_id not in all_classes:
                            analysis["orphaned_references"].append({
                                "type": "enrollment_class",
                                "enrollment_id": entity.get("student_class_id"),
                                "missing_class_id": class_id,
                                "file": json_file.name
                            })
                            analysis["data_integrity_score"] -= 5
                        
                        # Track enrollments for capacity checking
                        if class_id in class_enrollments:
                            class_enrollments[class_id] += 1
                        
        except Exception as e:
            logger.warning(f"Error analyzing {json_file}: {e}")
            analysis["data_integrity_score"] -= 10
    
    # Check for capacity violations
    for class_id, enrollment_count in class_enrollments.items():
        max_capacity = class_capacities.get(class_id, 0)
        if enrollment_count > max_capacity:
            analysis["capacity_violations"].append({
                "class_id": class_id,
                "enrolled": enrollment_count,
                "capacity": max_capacity,
                "violation": enrollment_count - max_capacity
            })
            analysis["data_integrity_score"] -= 3
    
    # Ensure score doesn't go below 0
    analysis["data_integrity_score"] = max(0, analysis["data_integrity_score"])
    
    return analysis


def generate_comparison_report(original_analysis: dict, fixed_analysis: dict) -> str:
    """Generate a comprehensive comparison report."""
    
    report = f"""# Race Condition Analysis Report

Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}

## 📊 Summary Comparison

| Metric | Original System | Fixed System | Improvement |
|--------|----------------|--------------|-------------|
| **Data Integrity Score** | {original_analysis['data_integrity_score']:.1f}/100 | {fixed_analysis['data_integrity_score']:.1f}/100 | {fixed_analysis['data_integrity_score'] - original_analysis['data_integrity_score']:+.1f} |
| **Orphaned References** | {len(original_analysis['orphaned_references'])} | {len(fixed_analysis['orphaned_references'])} | {len(fixed_analysis['orphaned_references']) - len(original_analysis['orphaned_references']):+d} |
| **Capacity Violations** | {len(original_analysis['capacity_violations'])} | {len(fixed_analysis['capacity_violations'])} | {len(fixed_analysis['capacity_violations']) - len(original_analysis['capacity_violations']):+d} |
| **Duplicate IDs** | {sum(len(v) for v in original_analysis['duplicate_ids'].values())} | {sum(len(v) for v in fixed_analysis['duplicate_ids'].values())} | {sum(len(v) for v in fixed_analysis['duplicate_ids'].values()) - sum(len(v) for v in original_analysis['duplicate_ids'].values()):+d} |

## 🚨 Race Condition Issues Found

### Original System Problems

"""
    
    # Original system issues
    if original_analysis['orphaned_references']:
        report += f"#### ❌ Orphaned Foreign Key References: {len(original_analysis['orphaned_references'])}\n\n"
        report += "**Examples:**\n"
        for ref in original_analysis['orphaned_references'][:5]:  # Show first 5
            if ref['type'] == 'class_teacher':
                report += f"- Class `{ref['class_id']}` references non-existent teacher `{ref['missing_teacher_id']}` (in {ref['file']})\n"
            elif ref['type'] == 'enrollment_student':
                report += f"- Enrollment `{ref['enrollment_id']}` references non-existent student `{ref['missing_student_id']}` (in {ref['file']})\n"
            elif ref['type'] == 'enrollment_class':
                report += f"- Enrollment `{ref['enrollment_id']}` references non-existent class `{ref['missing_class_id']}` (in {ref['file']})\n"
        if len(original_analysis['orphaned_references']) > 5:
            report += f"- ... and {len(original_analysis['orphaned_references']) - 5} more\n"
        report += "\n"
    
    if original_analysis['capacity_violations']:
        report += f"#### ❌ Capacity Violations: {len(original_analysis['capacity_violations'])}\n\n"
        report += "**Examples:**\n"
        for violation in original_analysis['capacity_violations'][:5]:
            report += f"- Class `{violation['class_id']}`: {violation['enrolled']} enrolled > {violation['capacity']} capacity (+{violation['violation']} over)\n"
        if len(original_analysis['capacity_violations']) > 5:
            report += f"- ... and {len(original_analysis['capacity_violations']) - 5} more\n"
        report += "\n"
    
    if original_analysis['duplicate_ids']:
        report += "#### ❌ Duplicate ID Issues:\n\n"
        for entity_type, duplicates in original_analysis['duplicate_ids'].items():
            if duplicates:
                report += f"- **{entity_type}**: {len(duplicates)} duplicates (e.g., {', '.join(duplicates[:3])})\n"
        report += "\n"
    
    # Fixed system results
    report += "### Fixed System Results\n\n"
    
    if not fixed_analysis['orphaned_references'] and not fixed_analysis['capacity_violations'] and not any(fixed_analysis['duplicate_ids'].values()):
        report += "✅ **NO RACE CONDITIONS DETECTED!**\n\n"
        report += "- Zero orphaned foreign key references\n"
        report += "- Zero capacity violations\n" 
        report += "- Zero duplicate IDs\n"
        report += "- Perfect referential integrity maintained\n\n"
    else:
        report += "⚠️ **Some issues found in fixed system:**\n\n"
        if fixed_analysis['orphaned_references']:
            report += f"- {len(fixed_analysis['orphaned_references'])} orphaned references\n"
        if fixed_analysis['capacity_violations']:
            report += f"- {len(fixed_analysis['capacity_violations'])} capacity violations\n"
    
    # Technical details
    report += f"""## 🔧 Technical Analysis

### Root Cause of Race Conditions (Original System)

1. **Lost State Between Instances**: Each generator instance starts with empty dictionaries
2. **No Persistent ID Tracking**: ID counters reset, causing duplicate IDs
3. **Missing Dependency Validation**: Entities reference others that don't exist yet
4. **No Capacity Management**: Class enrollment tracking is lost between runs

### How the Fix Works

1. **Persistent State Management**: State saved to disk with file locking
2. **Dependency Ordering**: Teachers → Classes → Students → Enrollments
3. **Referential Integrity**: Validates all foreign keys before creation
4. **Capacity Constraints**: Tracks and enforces class capacity limits
5. **Thread Safety**: File locking prevents concurrent access issues

## 📈 Entity Statistics

### Original System
- **Students**: {original_analysis['total_entities']['students']}
- **Teachers**: {original_analysis['total_entities']['teachers']} 
- **Classes**: {original_analysis['total_entities']['classes']}
- **Enrollments**: {original_analysis['total_entities']['student_classes']}

### Fixed System  
- **Students**: {fixed_analysis['total_entities']['students']}
- **Teachers**: {fixed_analysis['total_entities']['teachers']}
- **Classes**: {fixed_analysis['total_entities']['classes']} 
- **Enrollments**: {fixed_analysis['total_entities']['student_classes']}

## 🎯 Recommendations

### Immediate Actions
1. **Migrate to Fixed System**: Replace `json_generator.py` with `json_generator_fixed.py`
2. **Validate Existing Data**: Run integrity checks on previously generated data
3. **Implement Monitoring**: Set up automated race condition detection

### Long-term Solutions
1. **Use Enhanced System**: Migrate to `enhanced_json_generator.py` for best results
2. **Continuous Integration**: Include race condition tests in CI/CD pipeline
3. **Data Quality Metrics**: Monitor integrity scores over time

---

*This analysis demonstrates why the enhanced dependency-managed system is critical for production use.*
"""
    
    return report


def main():
    """Run the race condition demonstration."""
    logger.info("Starting Race Condition Demonstration")
    
    # Test original system (with race conditions)
    logger.info("=" * 60)
    logger.info("Testing ORIGINAL system (with race conditions)")
    logger.info("=" * 60)
    
    original_results = run_generator(
        script_name="json_generator.py",
        output_dir="./race_test_original",
        num_files=30,
        clean_first=True
    )
    
    if not original_results:
        logger.error("Failed to run original generator")
        return
    
    # Test fixed system (race conditions eliminated)
    logger.info("=" * 60)  
    logger.info("Testing FIXED system (race conditions eliminated)")
    logger.info("=" * 60)
    
    fixed_results = run_generator(
        script_name="json_generator_fixed.py",
        output_dir="./race_test_fixed",
        num_files=30,
        clean_first=True
    )
    
    if not fixed_results:
        logger.error("Failed to run fixed generator")
        return
    
    # Analyze both results
    logger.info("Analyzing race conditions in both systems...")
    
    original_analysis = analyze_race_conditions(original_results['json_files'])
    fixed_analysis = analyze_race_conditions(fixed_results['json_files'])
    
    # Generate comparison report
    report = generate_comparison_report(original_analysis, fixed_analysis)
    
    # Save report
    report_file = Path("RACE_CONDITION_ANALYSIS.md")
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    # Display summary
    logger.info("=" * 60)
    logger.info("RACE CONDITION ANALYSIS COMPLETE")
    logger.info("=" * 60)
    
    logger.info(f"Original System Integrity Score: {original_analysis['data_integrity_score']:.1f}/100")
    logger.info(f"Fixed System Integrity Score: {fixed_analysis['data_integrity_score']:.1f}/100")
    logger.info(f"Improvement: {fixed_analysis['data_integrity_score'] - original_analysis['data_integrity_score']:+.1f} points")
    
    logger.info(f"\nOrphaned References:")
    logger.info(f"  Original: {len(original_analysis['orphaned_references'])}")
    logger.info(f"  Fixed: {len(fixed_analysis['orphaned_references'])}")
    
    logger.info(f"\nCapacity Violations:")
    logger.info(f"  Original: {len(original_analysis['capacity_violations'])}")
    logger.info(f"  Fixed: {len(fixed_analysis['capacity_violations'])}")
    
    logger.info(f"\nDetailed report saved to: {report_file.absolute()}")
    
    if fixed_analysis['data_integrity_score'] >= 95:
        logger.info("✅ RACE CONDITIONS SUCCESSFULLY ELIMINATED!")
    else:
        logger.warning("⚠️ Some issues remain in the fixed system")


if __name__ == "__main__":
    main()
