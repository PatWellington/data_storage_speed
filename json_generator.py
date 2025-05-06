#!/usr/bin/env python3
"""
JSON Generator Script

This script generates realistic school registry data in JSON format,
simulating data that might be returned from distributed computing jobs.
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
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("json_generator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("JSONGenerator")


class SchoolDataGenerator:
    """Generator for realistic school registry data in JSON format."""
    
    # Common data for generation
    FIRST_NAMES = [
        "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia", "James", 
        "Isabella", "Logan", "Charlotte", "Benjamin", "Amelia", "Mason", "Mia", 
        "Elijah", "Harper", "Oliver", "Evelyn", "Jacob", "Abigail", "Lucas", 
        "Emily", "Michael", "Elizabeth", "Alexander", "Sofia", "Ethan", "Avery", 
        "Daniel", "Ella", "Matthew", "Scarlett", "Aiden", "Grace", "Henry", 
        "Chloe", "Joseph", "Victoria", "Jackson", "Riley", "Samuel", "Aria", 
        "Sebastian", "Lily", "David", "Aubrey", "Carter", "Zoey", "Wyatt", "Hannah",
        "John", "Maria", "Owen", "Layla", "Dylan", "Brooklyn", "Luke", "Zoe", 
        "Gabriel", "Penelope", "Anthony", "Leah", "Isaac", "Audrey", "Grayson", 
        "Savannah", "Julian", "Bella", "Christopher", "Maya", "Joshua", "Sydney", 
        "Andrew", "Alice", "Lincoln", "Skylar", "Theodore", "Madelyn", "Ryan", 
        "Stella", "Nathan", "Julia", "Adam", "Jayden", "Ian", "Wei", "Mohammad", 
        "Aisha", "Juan", "Maria", "Hiroshi", "Yuki", "Chen", "Li", "Raj", "Priya"
    ]
    
    LAST_NAMES = [
        "Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", 
        "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", 
        "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", 
        "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez", 
        "King", "Wright", "Lopez", "Hill", "Scott", "Green", "Adams", "Baker", 
        "Gonzalez", "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Turner", 
        "Phillips", "Campbell", "Parker", "Evans", "Edwards", "Collins", "Stewart", 
        "Sanchez", "Morris", "Rogers", "Reed", "Cook", "Morgan", "Bell", "Murphy", 
        "Bailey", "Rivera", "Cooper", "Richardson", "Cox", "Howard", "Ward", 
        "Torres", "Peterson", "Gray", "Ramirez", "James", "Watson", "Brooks", 
        "Kelly", "Sanders", "Price", "Bennett", "Wood", "Barnes", "Ross", "Henderson", 
        "Coleman", "Jenkins", "Perry", "Powell", "Long", "Patterson", "Hughes", 
        "Flores", "Washington", "Butler", "Simmons", "Foster", "Gonzales", "Bryant", 
        "Alexander", "Russell", "Griffin", "Diaz", "Hayes", "Kim", "Wang", "Patel",
        "Singh", "Ali", "Chen", "Wong", "Liu", "Gupta", "Sharma", "Kumar", "Nakamura"
    ]
    
    DEPARTMENTS = [
        "Mathematics", "English", "Science", "History", "Computer Science", 
        "Physical Education", "Art", "Music", "Foreign Languages", "Social Studies",
        "Economics", "Biology", "Chemistry", "Physics", "Geography", "Psychology"
    ]
    
    SUBJECTS = {
        "Mathematics": ["Algebra I", "Algebra II", "Geometry", "Calculus", "Statistics", "Pre-Calculus"],
        "English": ["English Literature", "Creative Writing", "Grammar", "Composition", "Public Speaking"],
        "Science": ["General Science", "Biology", "Chemistry", "Physics", "Environmental Science"],
        "History": ["World History", "U.S. History", "Ancient Civilizations", "Modern History"],
        "Computer Science": ["Programming Basics", "Web Development", "Data Structures", "Algorithms", "Computer Architecture"],
        "Physical Education": ["Team Sports", "Individual Sports", "Fitness", "Health"],
        "Art": ["Drawing", "Painting", "Sculpture", "Art History", "Digital Art"],
        "Music": ["Band", "Orchestra", "Choir", "Music Theory", "Music History"],
        "Foreign Languages": ["Spanish", "French", "German", "Chinese", "Japanese", "Latin"],
        "Social Studies": ["Civics", "Economics", "Sociology", "Psychology", "Anthropology"],
        "Economics": ["Microeconomics", "Macroeconomics", "International Economics", "Business Economics"],
        "Biology": ["Cell Biology", "Genetics", "Ecology", "Human Anatomy", "Zoology"],
        "Chemistry": ["Organic Chemistry", "Inorganic Chemistry", "Biochemistry", "Physical Chemistry"],
        "Physics": ["Mechanics", "Thermodynamics", "Electromagnetism", "Quantum Physics"],
        "Geography": ["Physical Geography", "Human Geography", "Cartography", "Geographic Information Systems"],
        "Psychology": ["Intro to Psychology", "Developmental Psychology", "Abnormal Psychology", "Social Psychology"]
    }
    
    GRADES = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-", "F"]
    GRADE_WEIGHTS = [5, 10, 10, 10, 15, 10, 10, 10, 5, 5, 5, 3, 2]  # Weighted distribution
    
    SCHEDULE_TIMES = [
        "8:00 AM - 9:30 AM", 
        "9:45 AM - 11:15 AM", 
        "11:30 AM - 1:00 PM", 
        "1:15 PM - 2:45 PM", 
        "3:00 PM - 4:30 PM"
    ]
    
    SCHEDULE_DAYS = [
        "Monday/Wednesday/Friday", 
        "Tuesday/Thursday", 
        "Monday/Wednesday", 
        "Tuesday/Thursday/Friday",
        "Monday/Friday"
    ]
    
    SEMESTERS = ["Fall 2024", "Spring 2025", "Summer 2025"]
    
    def __init__(
        self, 
        output_dir: str,
        seed: Optional[int] = None,
        start_ids: Optional[Dict[str, int]] = None
    ):
        """
        Initialize the school data generator.
        
        Args:
            output_dir: Directory where JSON files will be saved
            seed: Random seed for reproducibility (if None, use random seed)
            start_ids: Starting ID numbers for each entity type
        """
        # Set random seed if provided
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize ID counters (used to ensure unique IDs)
        self.start_ids = start_ids or {
            "student": 1000,
            "teacher": 100,
            "class": 2000,
            "student_class": 5000
        }
        
        # Keep track of generated data to maintain relationships
        self.students = {}  # student_id -> student data
        self.teachers = {}  # teacher_id -> teacher data
        self.classes = {}   # class_id -> class data
        self.student_classes = {}  # student_class_id -> student_class data
        
        logger.info(f"SchoolDataGenerator initialized to output to: {output_dir}")
    
    def generate_student(self) -> Dict[str, Any]:
        """Generate random student data."""
        student_id = f"S{self.start_ids['student']}"
        self.start_ids['student'] += 1
        
        first_name = random.choice(self.FIRST_NAMES)
        last_name = random.choice(self.LAST_NAMES)
        age = random.randint(14, 19)  # High school age range
        gender = random.choice(["Male", "Female", "Non-binary"])
        year = random.randint(9, 12)  # Grades 9-12 (high school)
        gpa = round(random.uniform(1.5, 4.0), 2)
        
        # Generate email with some variation
        email_domain = random.choice(["school.edu", "student.edu", "learn.org", "academy.net"])
        email_style = random.choice([
            lambda f, l: f"{f.lower()}.{l.lower()}@{email_domain}",
            lambda f, l: f"{f.lower()}{l.lower()[0]}@{email_domain}",
            lambda f, l: f"{f.lower()[0]}{l.lower()}@{email_domain}"
        ])
        email = email_style(first_name, last_name)
        
        # Generate phone with realistic format
        phone = f"({random.randint(100, 999)}) {random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # Generate address
        street_num = random.randint(100, 9999)
        street_names = ["Main St", "Oak Ave", "Maple Dr", "Park Blvd", "Washington St", "Cedar Ln"]
        city_names = ["Springfield", "Riverside", "Fairview", "Georgetown", "Kingston", "Millville"]
        states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
        address = f"{street_num} {random.choice(street_names)}, {random.choice(city_names)}, {random.choice(states)} {random.randint(10000, 99999)}"
        
        # Generate enrollment date (within the last 4 years)
        days_ago = random.randint(0, 4*365)
        enrollment_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        
        student = {
            "student_id": student_id,
            "first_name": first_name,
            "last_name": last_name,
            "age": age,
            "gender": gender,
            "year": year,
            "gpa": gpa,
            "address": address,
            "email": email,
            "phone": phone,
            "enrollment_date": enrollment_date
        }
        
        self.students[student_id] = student
        return student
    
    def generate_teacher(self) -> Dict[str, Any]:
        """Generate random teacher data."""
        teacher_id = f"T{self.start_ids['teacher']}"
        self.start_ids['teacher'] += 1
        
        first_name = random.choice(self.FIRST_NAMES)
        last_name = random.choice(self.LAST_NAMES)
        department = random.choice(self.DEPARTMENTS)
        
        # Generate email with professional format
        email_domain = "faculty.school.edu"
        email = f"{first_name.lower()}.{last_name.lower()}@{email_domain}"
        
        # Generate office number
        building = random.choice(["A", "B", "C", "D"])
        office_number = f"{building}-{random.randint(100, 399)}"
        
        # Generate phone with realistic format
        phone = f"({random.randint(100, 999)}) {random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # Generate hire date (within the last 20 years)
        days_ago = random.randint(0, 20*365)
        hire_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        
        # Generate salary (typical teacher range)
        salary = round(random.uniform(40000, 90000), 2)
        
        # Generate qualifications
        num_qualifications = random.randint(1, 4)
        qualification_options = [
            "B.A.", "B.S.", "M.A.", "M.S.", "Ph.D.", "Ed.D.", 
            "Teaching Certificate", "Department Chair", "Published Author",
            "National Board Certified", "Distinguished Educator Award"
        ]
        qualifications = random.sample(qualification_options, num_qualifications)
        
        teacher = {
            "teacher_id": teacher_id,
            "first_name": first_name,
            "last_name": last_name,
            "department": department,
            "email": email,
            "hire_date": hire_date,
            "salary": salary,
            "office_number": office_number,
            "phone": phone,
            "qualifications": qualifications
        }
        
        self.teachers[teacher_id] = teacher
        return teacher
    
    def generate_class(self, teacher_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate random class data.
        
        Args:
            teacher_id: Specific teacher ID to assign (if None, choose randomly)
        """
        class_id = f"C{self.start_ids['class']}"
        self.start_ids['class'] += 1
        
        # Choose teacher if not specified
        if teacher_id is None:
            if not self.teachers:
                # Create a teacher if none exist
                teacher = self.generate_teacher()
                teacher_id = teacher["teacher_id"]
            else:
                teacher_id = random.choice(list(self.teachers.keys()))
        
        teacher = self.teachers[teacher_id]
        department = teacher["department"]
        
        # Select a subject from the teacher's department
        if department in self.SUBJECTS:
            subject_options = self.SUBJECTS[department]
            class_name = random.choice(subject_options)
        else:
            # Fallback for departments without specific subjects
            class_name = f"{department} {random.randint(101, 499)}"
        
        # Generate room number
        building = random.choice(["A", "B", "C", "D"])
        room_number = f"{building}{random.randint(100, 300)}"
        
        # Generate schedule
        schedule_time = random.choice(self.SCHEDULE_TIMES)
        schedule_days = random.choice(self.SCHEDULE_DAYS)
        schedule = f"{schedule_days} {schedule_time}"
        
        # Choose semester
        semester = random.choice(self.SEMESTERS)
        
        # Set capacity and enrollment
        max_capacity = random.randint(20, 35)
        current_enrollment = 0  # Will be updated as students are enrolled
        
        class_data = {
            "class_id": class_id,
            "class_name": class_name,
            "room_number": room_number,
            "teacher_id": teacher_id,
            "schedule": schedule,
            "semester": semester,
            "max_capacity": max_capacity,
            "current_enrollment": current_enrollment
        }
        
        self.classes[class_id] = class_data
        return class_data
    
    def generate_student_class(
        self, 
        student_id: Optional[str] = None, 
        class_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a random student-class relationship.
        
        Args:
            student_id: Specific student ID to assign (if None, choose randomly)
            class_id: Specific class ID to assign (if None, choose randomly)
            
        Returns:
            Dict with student-class relationship data
        """
        student_class_id = f"SC{self.start_ids['student_class']}"
        self.start_ids['student_class'] += 1
        
        # Choose student if not specified
        if student_id is None:
            if not self.students:
                # Create a student if none exist
                student = self.generate_student()
                student_id = student["student_id"]
            else:
                student_id = random.choice(list(self.students.keys()))
        
        # Choose class if not specified
        if class_id is None:
            if not self.classes:
                # Create a class if none exist
                class_data = self.generate_class()
                class_id = class_data["class_id"]
            else:
                # Filter to classes that aren't at capacity
                available_classes = [
                    cid for cid, c in self.classes.items() 
                    if c["current_enrollment"] < c["max_capacity"]
                ]
                
                if not available_classes:
                    # Create a new class if all are at capacity
                    class_data = self.generate_class()
                    class_id = class_data["class_id"]
                else:
                    class_id = random.choice(available_classes)
        
        # Update class enrollment count
        class_data = self.classes[class_id]
        class_data["current_enrollment"] += 1
        
        # Generate enrollment date (within the last year)
        days_ago = random.randint(0, 365)
        enrollment_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        
        # Generate grade (some might be None if recently enrolled)
        if random.random() < 0.8:  # 80% chance of having a grade
            grade = random.choices(self.GRADES, weights=self.GRADE_WEIGHTS)[0]
        else:
            grade = None
        
        student_class = {
            "student_class_id": student_class_id,
            "student_id": student_id,
            "class_id": class_id,
            "grade": grade,
            "enrollment_date": enrollment_date
        }
        
        self.student_classes[student_class_id] = student_class
        return student_class
    
    def generate_batch(
        self,
        num_students: int = 0,
        num_teachers: int = 0,
        num_classes: int = 0,
        num_enrollments: int = 0
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generate a batch of random school data.
        
        Args:
            num_students: Number of new students to generate
            num_teachers: Number of new teachers to generate
            num_classes: Number of new classes to generate
            num_enrollments: Number of new student-class enrollments to generate
            
        Returns:
            Dict containing lists of generated entities
        """
        result = {
            "students": [],
            "teachers": [],
            "classes": [],
            "student_classes": []
        }
        
        # Generate new teachers
        for _ in range(num_teachers):
            teacher = self.generate_teacher()
            result["teachers"].append(teacher)
        
        # Generate new classes (with teachers)
        for _ in range(num_classes):
            class_data = self.generate_class()
            result["classes"].append(class_data)
        
        # Generate new students
        for _ in range(num_students):
            student = self.generate_student()
            result["students"].append(student)
        
        # Generate new enrollments
        for _ in range(num_enrollments):
            student_class = self.generate_student_class()
            result["student_classes"].append(student_class)
        
        return result
    
    def save_json(self, data: Dict[str, Any], filename: Optional[str] = None) -> str:
        """
        Save data to a JSON file atomically to prevent race conditions.
        
        Args:
            data: Data to save
            filename: Filename to use (if None, generate a timestamp-based name)
            
        Returns:
            Path to the saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"school_data_{timestamp}.json"
        
        file_path = self.output_dir / filename
        # Create a temporary file path with .tmp extension
        temp_path = file_path.with_suffix('.tmp')
        
        # Write to the temporary file first
        with open(temp_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Atomic rename operation to ensure files are completely written before being visible
        temp_path.rename(file_path)
        
        logger.info(f"Saved data to {file_path} (atomically)")
        return str(file_path)


class ContinuousGenerator:
    """Continuously generates JSON files at specified intervals."""
    
    def __init__(
        self,
        output_dir: str,
        rate: float = 0.33,  # Files per second
        std_dev: float = 0.1,  # Std deviation for rate
        min_interval: float = 0.1,  # Minimum time between generations (seconds)
        min_size: int = 1,
        max_size: int = 10,
        seed: Optional[int] = None
    ):
        """
        Initialize the continuous generator.
        
        Args:
            output_dir: Directory to save generated JSON files
            rate: Number of files to generate per second
            std_dev: Standard deviation of generation rate (for timing variability)
            min_interval: Minimum time between generations (seconds)
            min_size: Minimum number of entities per batch
            max_size: Maximum number of entities per batch
            seed: Random seed for reproducibility
        """
        self.generator = SchoolDataGenerator(output_dir=output_dir, seed=seed)
        
        # Convert rate (files per second) to interval (seconds per file)
        if rate <= 0:
            raise ValueError("Rate must be positive")
        
        self.avg_interval = 1.0 / rate  # Convert rate to interval
        self.std_dev = std_dev
        self.min_interval = min_interval
        self.min_size = min_size
        self.max_size = max_size
        
        logger.info(f"Continuous generator initialized with rate: {rate} files/second (avg interval: {self.avg_interval:.2f}s)")
    
    def _get_next_interval(self) -> float:
        """Get the next random interval using Gaussian distribution."""
        interval = max(
            self.min_interval,
            np.random.normal(self.avg_interval, self.std_dev)
        )
        return interval
    
    def _get_random_batch_size(self) -> Tuple[int, int, int, int]:
        """Get random batch sizes for each entity type."""
        # Decide which entity types to include in this batch
        entity_types = random.sample(['students', 'teachers', 'classes', 'enrollments'], 
                                     k=random.randint(1, 4))
        
        num_students = random.randint(self.min_size, self.max_size) if 'students' in entity_types else 0
        num_teachers = random.randint(self.min_size, self.max_size // 2) if 'teachers' in entity_types else 0
        num_classes = random.randint(self.min_size, self.max_size // 2) if 'classes' in entity_types else 0
        
        # Generate more enrollments than other entities
        num_enrollments = random.randint(
            self.min_size, 
            max(self.max_size * 2, num_students * 2)
        ) if 'enrollments' in entity_types else 0
        
        return num_students, num_teachers, num_classes, num_enrollments
    
    def run(self, num_iterations: Optional[int] = None):
        """
        Run the continuous generator.
        
        Args:
            num_iterations: Number of JSON files to generate (None for infinite)
        """
        iteration = 0
        running = True
        
        logger.info(f"Starting continuous generation (iterations: {'infinite' if num_iterations is None else num_iterations})")
        
        try:
            while running:
                # Determine batch size
                num_students, num_teachers, num_classes, num_enrollments = self._get_random_batch_size()
                
                # Generate and save data
                batch = self.generator.generate_batch(
                    num_students=num_students,
                    num_teachers=num_teachers,
                    num_classes=num_classes,
                    num_enrollments=num_enrollments
                )
                
                # Log batch statistics
                total_entities = sum(len(entities) for entities in batch.values())
                logger.info(f"Generated batch #{iteration + 1} with {total_entities} total entities")
                
                # Save to file
                self.generator.save_json(batch)
                
                # Update iteration counter and check if we should stop
                iteration += 1
                if num_iterations is not None and iteration >= num_iterations:
                    running = False
                else:
                    # Wait for next interval
                    interval = self._get_next_interval()
                    logger.debug(f"Waiting {interval:.2f}s until next generation")
                    time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Generator stopped by user")
        
        logger.info(f"Generator completed after {iteration} iterations")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Generate random school registry data in JSON format")
    
    parser.add_argument("--output-dir", default="./input", help="Directory to save generated JSON files")
    parser.add_argument("--num-iterations", type=int, default=None, help="Number of files to generate (default: infinite)")
    parser.add_argument("--rate", type=float, default=0.33, help="Number of files to generate per second")
    parser.add_argument("--std-dev", type=float, default=0.1, help="Standard deviation for rate variability")
    parser.add_argument("--min-interval", type=float, default=0.1, help="Minimum time between generations (seconds)")
    parser.add_argument("--min-size", type=int, default=1, help="Minimum number of entities per batch")
    parser.add_argument("--max-size", type=int, default=10, help="Maximum number of entities per batch")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    # Create continuous generator and run
    generator = ContinuousGenerator(
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