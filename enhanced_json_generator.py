#!/usr/bin/env python3
"""
Enhanced JSON Generator with Dependency Management

This script generates realistic school registry data with proper dependency ordering
and referential integrity to prevent race conditions.
"""

import os
import json
import time
import random
import uuid
import argparse
import logging
from datetime import datetime, timedelta
import numpy as np
from typing import List, Dict, Any, Optional, Tuple, Set
from pathlib import Path
from dataclasses import dataclass, field
from collections import defaultdict
import threading
import queue


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("json_generator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("EnhancedJSONGenerator")


@dataclass
class GenerationConfig:
    """Configuration for data generation."""
    generation_rate: float = 0.33
    batch_size_range: Tuple[int, int] = (1, 10)
    dependency_batch_size: int = 50  # Generate dependencies in larger batches
    validation_enabled: bool = True
    enforce_referential_integrity: bool = True
    max_enrollments_per_student: int = 5
    class_capacity_utilization: float = 0.8  # Don't fill classes beyond 80%


class GlobalStateManager:
    """Manages global state to ensure referential integrity across batches."""
    
    def __init__(self):
        self.lock = threading.RLock()
        self.existing_students: Set[str] = set()
        self.existing_teachers: Set[str] = set()
        self.existing_classes: Set[str] = set()
        self.class_capacities: Dict[str, int] = {}
        self.class_enrollments: Dict[str, int] = defaultdict(int)
        self.student_enrollments: Dict[str, int] = defaultdict(int)
        self.teacher_departments: Dict[str, str] = {}
        
        # Track pending entities (generated but not yet persisted)
        self.pending_students: Set[str] = set()
        self.pending_teachers: Set[str] = set()
        self.pending_classes: Set[str] = set()
        
    def register_teacher(self, teacher_id: str, department: str):
        """Register a new teacher."""
        with self.lock:
            self.existing_teachers.add(teacher_id)
            self.teacher_departments[teacher_id] = department
            self.pending_teachers.discard(teacher_id)
    
    def register_student(self, student_id: str):
        """Register a new student."""
        with self.lock:
            self.existing_students.add(student_id)
            self.pending_students.discard(student_id)
    
    def register_class(self, class_id: str, capacity: int):
        """Register a new class."""
        with self.lock:
            self.existing_classes.add(class_id)
            self.class_capacities[class_id] = capacity
            self.class_enrollments[class_id] = 0
            self.pending_classes.discard(class_id)
    
    def can_enroll_student(self, student_id: str, class_id: str, max_enrollments: int = 5) -> bool:
        """Check if a student can be enrolled in a class."""
        with self.lock:
            # Check if student and class exist
            if student_id not in self.existing_students or class_id not in self.existing_classes:
                return False
            
            # Check class capacity
            current_enrollment = self.class_enrollments[class_id]
            max_capacity = self.class_capacities.get(class_id, 0)
            if current_enrollment >= max_capacity:
                return False
            
            # Check student enrollment limit
            student_enrollment_count = self.student_enrollments[student_id]
            if student_enrollment_count >= max_enrollments:
                return False
            
            return True
    
    def enroll_student(self, student_id: str, class_id: str) -> bool:
        """Attempt to enroll a student in a class."""
        with self.lock:
            if self.can_enroll_student(student_id, class_id):
                self.class_enrollments[class_id] += 1
                self.student_enrollments[student_id] += 1
                return True
            return False
    
    def get_available_teachers(self) -> List[str]:
        """Get list of available teachers."""
        with self.lock:
            return list(self.existing_teachers)
    
    def get_available_students(self) -> List[str]:
        """Get list of available students."""
        with self.lock:
            return list(self.existing_students)
    
    def get_available_classes_for_enrollment(self, utilization_limit: float = 0.8) -> List[str]:
        """Get classes that can accept new enrollments."""
        with self.lock:
            available = []
            for class_id in self.existing_classes:
                current = self.class_enrollments[class_id]
                capacity = self.class_capacities[class_id]
                if current < (capacity * utilization_limit):
                    available.append(class_id)
            return available
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        with self.lock:
            total_enrollments = sum(self.class_enrollments.values())
            total_capacity = sum(self.class_capacities.values())
            
            return {
                'students': len(self.existing_students),
                'teachers': len(self.existing_teachers),
                'classes': len(self.existing_classes),
                'total_enrollments': total_enrollments,
                'total_capacity': total_capacity,
                'capacity_utilization': total_enrollments / max(total_capacity, 1),
                'avg_enrollments_per_student': total_enrollments / max(len(self.existing_students), 1)
            }


class EnhancedSchoolDataGenerator:
    """Enhanced generator with dependency management and data quality controls."""
    
    # Class constants (same as before)
    FIRST_NAMES = [
        "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia", "James", 
        "Isabella", "Logan", "Charlotte", "Benjamin", "Amelia", "Mason", "Mia", 
        "Elijah", "Harper", "Oliver", "Evelyn", "Jacob", "Abigail", "Lucas", 
        "Emily", "Michael", "Elizabeth", "Alexander", "Sofia", "Ethan", "Avery", 
        "Daniel", "Ella", "Matthew", "Scarlett", "Aiden", "Grace", "Chloe", 
        "Victoria", "Joseph", "Riley", "Jackson", "Aria", "Samuel", "Lily", 
        "Sebastian", "Aubrey", "David", "Zoey", "Carter", "Hannah", "Wyatt"
    ]
    
    LAST_NAMES = [
        "Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", 
        "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", 
        "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", 
        "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez"
    ]
    
    DEPARTMENTS = [
        "Mathematics", "English", "Science", "History", "Computer Science", 
        "Physical Education", "Art", "Music", "Foreign Languages", "Social Studies"
    ]
    
    SUBJECTS = {
        "Mathematics": ["Algebra I", "Algebra II", "Geometry", "Calculus", "Statistics"],
        "English": ["English Literature", "Creative Writing", "Grammar", "Composition"],
        "Science": ["General Science", "Biology", "Chemistry", "Physics"],
        "History": ["World History", "U.S. History", "Ancient Civilizations"],
        "Computer Science": ["Programming Basics", "Web Development", "Data Structures"],
        "Physical Education": ["Team Sports", "Individual Sports", "Fitness"],
        "Art": ["Drawing", "Painting", "Sculpture", "Art History"],
        "Music": ["Band", "Orchestra", "Choir", "Music Theory"],
        "Foreign Languages": ["Spanish", "French", "German", "Chinese"],
        "Social Studies": ["Civics", "Economics", "Sociology", "Psychology"]
    }
    
    GRADES = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-", "F"]
    GRADE_WEIGHTS = [5, 10, 10, 10, 15, 10, 10, 10, 5, 5, 5, 3, 2]
    
    def __init__(
        self, 
        output_dir: str,
        state_manager: GlobalStateManager,
        config: GenerationConfig,
        seed: Optional[int] = None
    ):
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.state_manager = state_manager
        self.config = config
        
        # ID counters
        self.student_counter = 1000
        self.teacher_counter = 100
        self.class_counter = 2000
        self.enrollment_counter = 5000
        
        logger.info(f"Enhanced generator initialized with dependency management")
    
    def generate_teacher(self) -> Dict[str, Any]:
        """Generate a teacher with proper state registration."""
        teacher_id = f"T{self.teacher_counter}"
        self.teacher_counter += 1
        
        first_name = random.choice(self.FIRST_NAMES)
        last_name = random.choice(self.LAST_NAMES)
        department = random.choice(self.DEPARTMENTS)
        
        teacher = {
            "teacher_id": teacher_id,
            "first_name": first_name,
            "last_name": last_name,
            "department": department,
            "email": f"{first_name.lower()}.{last_name.lower()}@faculty.school.edu",
            "hire_date": (datetime.now() - timedelta(days=random.randint(0, 20*365))).strftime("%Y-%m-%d"),
            "salary": round(random.uniform(40000, 90000), 2),
            "office_number": f"{random.choice(['A', 'B', 'C', 'D'])}-{random.randint(100, 399)}",
            "phone": f"({random.randint(100, 999)}) {random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "qualifications": random.sample([
                "B.A.", "B.S.", "M.A.", "M.S.", "Ph.D.", "Ed.D.", 
                "Teaching Certificate", "Department Chair", "Published Author"
            ], random.randint(1, 4))
        }
        
        # Register with state manager
        self.state_manager.register_teacher(teacher_id, department)
        
        return teacher
    
    def generate_student(self) -> Dict[str, Any]:
        """Generate a student with proper state registration."""
        student_id = f"S{self.student_counter}"
        self.student_counter += 1
        
        first_name = random.choice(self.FIRST_NAMES)
        last_name = random.choice(self.LAST_NAMES)
        
        student = {
            "student_id": student_id,
            "first_name": first_name,
            "last_name": last_name,
            "age": random.randint(14, 19),
            "gender": random.choice(["Male", "Female", "Non-binary"]),
            "year": random.randint(9, 12),
            "gpa": round(random.uniform(1.5, 4.0), 2),
            "address": f"{random.randint(100, 9999)} {random.choice(['Main St', 'Oak Ave', 'Maple Dr'])}, {random.choice(['Springfield', 'Riverside', 'Fairview'])}, {random.choice(['CA', 'NY', 'TX'])} {random.randint(10000, 99999)}",
            "email": f"{first_name.lower()}.{last_name.lower()}@{random.choice(['school.edu', 'student.edu'])}",
            "phone": f"({random.randint(100, 999)}) {random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "enrollment_date": (datetime.now() - timedelta(days=random.randint(0, 4*365))).strftime("%Y-%m-%d")
        }
        
        # Register with state manager
        self.state_manager.register_student(student_id)
        
        return student
    
    def generate_class(self) -> Optional[Dict[str, Any]]:
        """Generate a class if teachers are available."""
        available_teachers = self.state_manager.get_available_teachers()
        if not available_teachers:
            return None
        
        class_id = f"C{self.class_counter}"
        self.class_counter += 1
        
        teacher_id = random.choice(available_teachers)
        department = self.state_manager.teacher_departments.get(teacher_id, "General")
        
        # Select subject based on teacher's department
        if department in self.SUBJECTS:
            class_name = random.choice(self.SUBJECTS[department])
        else:
            class_name = f"{department} {random.randint(101, 499)}"
        
        max_capacity = random.randint(20, 35)
        
        class_data = {
            "class_id": class_id,
            "class_name": class_name,
            "room_number": f"{random.choice(['A', 'B', 'C', 'D'])}{random.randint(100, 300)}",
            "teacher_id": teacher_id,
            "schedule": f"{random.choice(['Monday/Wednesday/Friday', 'Tuesday/Thursday'])} {random.choice(['8:00 AM - 9:30 AM', '9:45 AM - 11:15 AM', '11:30 AM - 1:00 PM'])}",
            "semester": random.choice(["Fall 2024", "Spring 2025", "Summer 2025"]),
            "max_capacity": max_capacity,
            "current_enrollment": 0
        }
        
        # Register with state manager
        self.state_manager.register_class(class_id, max_capacity)
        
        return class_data
    
    def generate_enrollment(self) -> Optional[Dict[str, Any]]:
        """Generate a student enrollment if valid combinations exist."""
        available_students = self.state_manager.get_available_students()
        available_classes = self.state_manager.get_available_classes_for_enrollment(
            self.config.class_capacity_utilization
        )
        
        if not available_students or not available_classes:
            return None
        
        # Try to find a valid student-class combination
        max_attempts = 20
        for _ in range(max_attempts):
            student_id = random.choice(available_students)
            class_id = random.choice(available_classes)
            
            if self.state_manager.can_enroll_student(student_id, class_id, self.config.max_enrollments_per_student):
                enrollment_id = f"SC{self.enrollment_counter}"
                self.enrollment_counter += 1
                
                # Actually perform the enrollment
                if self.state_manager.enroll_student(student_id, class_id):
                    enrollment = {
                        "student_class_id": enrollment_id,
                        "student_id": student_id,
                        "class_id": class_id,
                        "grade": random.choices(self.GRADES, weights=self.GRADE_WEIGHTS)[0] if random.random() < 0.8 else None,
                        "enrollment_date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
                    }
                    return enrollment
        
        return None
    
    def generate_dependency_aware_batch(self) -> Dict[str, List[Dict[str, Any]]]:
        """Generate a batch with proper dependency ordering."""
        batch = {
            "teachers": [],
            "students": [],
            "classes": [],
            "student_classes": []
        }
        
        stats = self.state_manager.get_stats()
        
        # Determine what to generate based on current state and needs
        need_teachers = stats['teachers'] < 10 or random.random() < 0.1
        need_students = stats['students'] < 50 or random.random() < 0.3
        need_classes = stats['classes'] < 20 or random.random() < 0.2
        need_enrollments = stats['capacity_utilization'] < 0.5 or random.random() < 0.4
        
        # Generate teachers first (no dependencies)
        if need_teachers:
            for _ in range(random.randint(1, 3)):
                teacher = self.generate_teacher()
                batch["teachers"].append(teacher)
        
        # Generate students (no dependencies)
        if need_students:
            for _ in range(random.randint(1, 8)):
                student = self.generate_student()
                batch["students"].append(student)
        
        # Generate classes (depends on teachers)
        if need_classes:
            for _ in range(random.randint(1, 4)):
                class_data = self.generate_class()
                if class_data:
                    batch["classes"].append(class_data)
        
        # Generate enrollments (depends on students and classes)
        if need_enrollments:
            max_enrollments = random.randint(5, 15)
            successful_enrollments = 0
            
            for _ in range(max_enrollments):
                enrollment = self.generate_enrollment()
                if enrollment:
                    batch["student_classes"].append(enrollment)
                    successful_enrollments += 1
                else:
                    break  # No more valid enrollments possible
        
        return batch
    
    def save_json(self, data: Dict[str, Any], filename: Optional[str] = None) -> str:
        """Save data to a JSON file with metadata."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"school_data_{timestamp}.json"
        
        # Add metadata to the JSON
        data_with_metadata = {
            "metadata": {
                "generation_timestamp": datetime.now().isoformat(),
                "generator_version": "2.0",
                "dependency_managed": True,
                "entity_counts": {
                    "teachers": len(data.get("teachers", [])),
                    "students": len(data.get("students", [])),
                    "classes": len(data.get("classes", [])),
                    "student_classes": len(data.get("student_classes", []))
                }
            },
            "data": data
        }
        
        file_path = self.output_dir / filename
        
        with open(file_path, 'w') as f:
            json.dump(data_with_metadata, f, indent=2)
        
        logger.info(f"Saved dependency-managed data to {file_path}")
        return str(file_path)


class EnhancedContinuousGenerator:
    """Enhanced continuous generator with dependency management."""
    
    def __init__(
        self,
        output_dir: str,
        config: GenerationConfig,
        seed: Optional[int] = None
    ):
        self.state_manager = GlobalStateManager()
        self.generator = EnhancedSchoolDataGenerator(
            output_dir=output_dir,
            state_manager=self.state_manager,
            config=config,
            seed=seed
        )
        self.config = config
        
        # Convert rate to interval
        self.avg_interval = 1.0 / config.generation_rate if config.generation_rate > 0 else 3.0
        
        logger.info(f"Enhanced continuous generator initialized with rate: {config.generation_rate} files/second")
    
    def get_next_interval(self) -> float:
        """Get next interval with some randomness."""
        jitter = random.uniform(0.8, 1.2)  # ±20% jitter
        return self.avg_interval * jitter
    
    def run(self, num_iterations: Optional[int] = None):
        """Run the enhanced continuous generator."""
        iteration = 0
        running = True
        
        logger.info(f"Starting enhanced generation (iterations: {'infinite' if num_iterations is None else num_iterations})")
        
        try:
            while running:
                # Generate dependency-aware batch
                batch = self.generator.generate_dependency_aware_batch()
                
                # Calculate statistics
                total_entities = sum(len(entities) for entities in batch.values())
                stats = self.state_manager.get_stats()
                
                # Log generation info
                logger.info(f"Generated batch #{iteration + 1}: {total_entities} entities, "
                           f"Global stats: {stats['students']} students, {stats['teachers']} teachers, "
                           f"{stats['classes']} classes, {stats['total_enrollments']} enrollments, "
                           f"Capacity utilization: {stats['capacity_utilization']:.2%}")
                
                # Save to file
                self.generator.save_json(batch)
                
                # Update iteration counter and check if we should stop
                iteration += 1
                if num_iterations is not None and iteration >= num_iterations:
                    running = False
                else:
                    # Wait for next interval
                    interval = self.get_next_interval()
                    time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Enhanced generator stopped by user")
        
        final_stats = self.state_manager.get_stats()
        logger.info(f"Final statistics: {final_stats}")
        logger.info(f"Enhanced generator completed after {iteration} iterations")


def main():
    """Main entry point for enhanced JSON generator."""
    parser = argparse.ArgumentParser(description="Generate dependency-managed school registry data")
    
    parser.add_argument("--output-dir", default="./input", help="Directory to save generated JSON files")
    parser.add_argument("--num-iterations", type=int, default=None, help="Number of files to generate")
    parser.add_argument("--rate", type=float, default=0.33, help="Files per second")
    parser.add_argument("--max-enrollments", type=int, default=5, help="Max enrollments per student")
    parser.add_argument("--capacity-utilization", type=float, default=0.8, help="Max class capacity utilization")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    
    args = parser.parse_args()
    
    # Create configuration
    config = GenerationConfig(
        generation_rate=args.rate,
        max_enrollments_per_student=args.max_enrollments,
        class_capacity_utilization=args.capacity_utilization
    )
    
    # Create and run generator
    generator = EnhancedContinuousGenerator(
        output_dir=args.output_dir,
        config=config,
        seed=args.seed
    )
    
    generator.run(num_iterations=args.num_iterations)


if __name__ == "__main__":
    main()
