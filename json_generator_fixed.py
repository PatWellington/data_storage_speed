#!/usr/bin/env python3
"""
FIXED JSON Generator Script - Race Condition Free

This is the FIXED version of the original json_generator.py that eliminates race conditions
by implementing proper dependency management and state persistence.
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
import pickle
import fcntl  # For file locking
import threading
from collections import defaultdict


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("json_generator_fixed.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("JSONGeneratorFixed")


class PersistentState:
    """Manages persistent state across generator instances to prevent race conditions."""
    
    def __init__(self, state_file: str = "./generator_state.pkl"):
        self.state_file = Path(state_file)
        self.lock = threading.RLock()
        
        # Initialize default state
        self.state = {
            "students": set(),  # Set of existing student IDs
            "teachers": set(),  # Set of existing teacher IDs
            "classes": set(),   # Set of existing class IDs
            "teacher_departments": {},  # teacher_id -> department
            "class_capacities": {},     # class_id -> max_capacity
            "class_enrollments": defaultdict(int),  # class_id -> current_enrollment
            "student_enrollments": defaultdict(int),  # student_id -> enrollment_count
            "id_counters": {
                "student": 1000,
                "teacher": 100,
                "class": 2000,
                "student_class": 5000
            }
        }
        
        # Load existing state if available
        self.load_state()
    
    def load_state(self):
        """Load state from disk with file locking."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'rb') as f:
                    # Use file locking for thread safety
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                    saved_state = pickle.load(f)
                    self.state.update(saved_state)
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                logger.info(f"Loaded state: {len(self.state['students'])} students, {len(self.state['teachers'])} teachers, {len(self.state['classes'])} classes")
            except Exception as e:
                logger.warning(f"Could not load state file: {e}")
    
    def save_state(self):
        """Save state to disk with file locking."""
        try:
            with open(self.state_file, 'wb') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                pickle.dump(self.state, f)
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception as e:
            logger.error(f"Could not save state file: {e}")
    
    def get_next_id(self, entity_type: str) -> str:
        """Get next unique ID for entity type."""
        with self.lock:
            counter = self.state["id_counters"][entity_type]
            self.state["id_counters"][entity_type] += 1
            
            if entity_type == "student":
                return f"S{counter}"
            elif entity_type == "teacher":
                return f"T{counter}"
            elif entity_type == "class":
                return f"C{counter}"
            elif entity_type == "student_class":
                return f"SC{counter}"
    
    def register_teacher(self, teacher_id: str, department: str):
        """Register a new teacher."""
        with self.lock:
            self.state["teachers"].add(teacher_id)
            self.state["teacher_departments"][teacher_id] = department
    
    def register_student(self, student_id: str):
        """Register a new student."""
        with self.lock:
            self.state["students"].add(student_id)
    
    def register_class(self, class_id: str, capacity: int):
        """Register a new class."""
        with self.lock:
            self.state["classes"].add(class_id)
            self.state["class_capacities"][class_id] = capacity
            self.state["class_enrollments"][class_id] = 0
    
    def can_enroll_student(self, student_id: str, class_id: str, max_enrollments: int = 5) -> bool:
        """Check if a student can be enrolled in a class."""
        with self.lock:
            # Check if student and class exist
            if student_id not in self.state["students"] or class_id not in self.state["classes"]:
                return False
            
            # Check class capacity (80% utilization limit)
            current_enrollment = self.state["class_enrollments"][class_id]
            max_capacity = self.state["class_capacities"].get(class_id, 0)
            if current_enrollment >= (max_capacity * 0.8):
                return False
            
            # Check student enrollment limit
            student_enrollment_count = self.state["student_enrollments"][student_id]
            if student_enrollment_count >= max_enrollments:
                return False
            
            return True
    
    def enroll_student(self, student_id: str, class_id: str) -> bool:
        """Attempt to enroll a student in a class."""
        with self.lock:
            if self.can_enroll_student(student_id, class_id):
                self.state["class_enrollments"][class_id] += 1
                self.state["student_enrollments"][student_id] += 1
                return True
            return False
    
    def get_available_teachers(self) -> List[str]:
        """Get list of available teachers."""
        with self.lock:
            return list(self.state["teachers"])
    
    def get_available_students(self) -> List[str]:
        """Get list of available students."""
        with self.lock:
            return list(self.state["students"])
    
    def get_available_classes_for_enrollment(self) -> List[str]:
        """Get classes that can accept new enrollments."""
        with self.lock:
            available = []
            for class_id in self.state["classes"]:
                current = self.state["class_enrollments"][class_id]
                capacity = self.state["class_capacities"][class_id]
                if current < (capacity * 0.8):  # 80% utilization limit
                    available.append(class_id)
            return available
    
    def get_teacher_department(self, teacher_id: str) -> str:
        """Get teacher's department."""
        with self.lock:
            return self.state["teacher_departments"].get(teacher_id, "General")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        with self.lock:
            total_enrollments = sum(self.state["class_enrollments"].values())
            total_capacity = sum(self.state["class_capacities"].values())
            
            return {
                'students': len(self.state["students"]),
                'teachers': len(self.state["teachers"]),
                'classes': len(self.state["classes"]),
                'total_enrollments': total_enrollments,
                'total_capacity': total_capacity,
                'capacity_utilization': total_enrollments / max(total_capacity, 1),
                'avg_enrollments_per_student': total_enrollments / max(len(self.state["students"]), 1)
            }


class FixedSchoolDataGenerator:
    """FIXED generator that eliminates race conditions through persistent state management."""
    
    # Same constants as original
    FIRST_NAMES = [
        "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia", "James", 
        "Isabella", "Logan", "Charlotte", "Benjamin", "Amelia", "Mason", "Mia", 
        "Elijah", "Harper", "Oliver", "Evelyn", "Jacob", "Abigail", "Lucas", 
        "Emily", "Michael", "Elizabeth", "Alexander", "Sofia", "Ethan", "Avery", 
        "Daniel", "Ella", "Matthew", "Scarlett", "Aiden", "Grace", "Henry", 
        "Chloe", "Joseph", "Victoria", "Jackson", "Riley", "Samuel", "Aria", 
        "Sebastian", "Lily", "David", "Aubrey", "Carter", "Zoey", "Wyatt", "Hannah"
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
    
    def __init__(self, output_dir: str, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Use persistent state to avoid race conditions
        self.state = PersistentState()
        
        logger.info(f"FIXED SchoolDataGenerator initialized with persistent state")
    
    def generate_teacher(self) -> Dict[str, Any]:
        """Generate teacher with proper state registration."""
        teacher_id = self.state.get_next_id("teacher")
        
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
        
        # Register with persistent state
        self.state.register_teacher(teacher_id, department)
        
        return teacher
    
    def generate_student(self) -> Dict[str, Any]:
        """Generate student with proper state registration."""
        student_id = self.state.get_next_id("student")
        
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
        
        # Register with persistent state
        self.state.register_student(student_id)
        
        return student
    
    def generate_class(self) -> Optional[Dict[str, Any]]:
        """Generate class with proper dependency checking."""
        available_teachers = self.state.get_available_teachers()
        if not available_teachers:
            return None  # Cannot create class without teachers
        
        class_id = self.state.get_next_id("class")
        teacher_id = random.choice(available_teachers)
        department = self.state.get_teacher_department(teacher_id)
        
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
        
        # Register with persistent state
        self.state.register_class(class_id, max_capacity)
        
        return class_data
    
    def generate_enrollment(self) -> Optional[Dict[str, Any]]:
        """Generate enrollment with proper validation."""
        available_students = self.state.get_available_students()
        available_classes = self.state.get_available_classes_for_enrollment()
        
        if not available_students or not available_classes:
            return None  # Cannot create enrollment
        
        # Try to find a valid combination
        max_attempts = 20
        for _ in range(max_attempts):
            student_id = random.choice(available_students)
            class_id = random.choice(available_classes)
            
            if self.state.can_enroll_student(student_id, class_id):
                enrollment_id = self.state.get_next_id("student_class")
                
                # Actually perform the enrollment
                if self.state.enroll_student(student_id, class_id):
                    enrollment = {
                        "student_class_id": enrollment_id,
                        "student_id": student_id,
                        "class_id": class_id,
                        "grade": random.choices(self.GRADES, weights=self.GRADE_WEIGHTS)[0] if random.random() < 0.8 else None,
                        "enrollment_date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
                    }
                    return enrollment
        
        return None  # Could not find valid enrollment
    
    def generate_dependency_aware_batch(self) -> Dict[str, List[Dict[str, Any]]]:
        """Generate batch with proper dependency ordering."""
        batch = {
            "teachers": [],
            "students": [],
            "classes": [],
            "student_classes": []
        }
        
        stats = self.state.get_stats()
        
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
        """Save data to JSON file with metadata and state persistence."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"school_data_{timestamp}.json"
        
        # Add metadata indicating this is from the FIXED generator
        data_with_metadata = {
            "metadata": {
                "generation_timestamp": datetime.now().isoformat(),
                "generator_version": "1.0-FIXED",
                "race_condition_free": True,
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
        
        # Save persistent state after each file generation
        self.state.save_state()
        
        logger.info(f"Saved race-condition-free data to {file_path}")
        return str(file_path)


class FixedContinuousGenerator:
    """FIXED continuous generator with race condition elimination."""
    
    def __init__(
        self,
        output_dir: str,
        rate: float = 0.33,
        std_dev: float = 0.1,
        min_interval: float = 0.1,
        min_size: int = 1,
        max_size: int = 10,
        seed: Optional[int] = None
    ):
        self.generator = FixedSchoolDataGenerator(output_dir=output_dir, seed=seed)
        
        if rate <= 0:
            raise ValueError("Rate must be positive")
        
        self.avg_interval = 1.0 / rate
        self.std_dev = std_dev
        self.min_interval = min_interval
        self.min_size = min_size
        self.max_size = max_size
        
        logger.info(f"FIXED continuous generator initialized with rate: {rate} files/second (avg interval: {self.avg_interval:.2f}s)")
    
    def _get_next_interval(self) -> float:
        """Get the next random interval using Gaussian distribution."""
        interval = max(
            self.min_interval,
            np.random.normal(self.avg_interval, self.std_dev)
        )
        return interval
    
    def run(self, num_iterations: Optional[int] = None):
        """Run the FIXED continuous generator."""
        iteration = 0
        running = True
        
        logger.info(f"Starting FIXED continuous generation (iterations: {'infinite' if num_iterations is None else num_iterations})")
        
        try:
            while running:
                # Generate dependency-aware batch
                batch = self.generator.generate_dependency_aware_batch()
                
                # Calculate statistics
                total_entities = sum(len(entities) for entities in batch.values())
                stats = self.generator.state.get_stats()
                
                # Log generation info
                logger.info(f"Generated FIXED batch #{iteration + 1}: {total_entities} entities, "
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
                    interval = self._get_next_interval()
                    time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("FIXED generator stopped by user")
        
        final_stats = self.generator.state.get_stats()
        logger.info(f"FIXED generator final statistics: {final_stats}")
        logger.info(f"FIXED generator completed after {iteration} iterations")


def main():
    """Main entry point for the FIXED script."""
    parser = argparse.ArgumentParser(description="Generate FIXED school registry data (race condition free)")
    
    parser.add_argument("--output-dir", default="./input_fixed", help="Directory to save generated JSON files")
    parser.add_argument("--num-iterations", type=int, default=None, help="Number of files to generate (default: infinite)")
    parser.add_argument("--rate", type=float, default=0.33, help="Number of files to generate per second")
    parser.add_argument("--std-dev", type=float, default=0.1, help="Standard deviation for rate variability")
    parser.add_argument("--min-interval", type=float, default=0.1, help="Minimum time between generations (seconds)")
    parser.add_argument("--min-size", type=int, default=1, help="Minimum number of entities per batch")
    parser.add_argument("--max-size", type=int, default=10, help="Maximum number of entities per batch")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument("--clean-state", action="store_true", help="Clean persistent state before starting")
    
    args = parser.parse_args()
    
    # Clean state if requested
    if args.clean_state:
        state_file = Path("./generator_state.pkl")
        if state_file.exists():
            state_file.unlink()
            logger.info("Cleaned persistent state")
    
    # Create FIXED continuous generator and run
    generator = FixedContinuousGenerator(
        output_dir=args.output_dir,
        rate=args.rate,
        std_dev=args.std_dev,
        min_interval=args.min_interval,
        min_size=args.min_size,
        max_size=args.max_size,
        seed=args.seed
    )
    
    generator.run(num_iterations=args.num_iterations)


if __name__ == "__main__":
    main()
