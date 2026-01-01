"""
BITS Pilani StudyDeck - Database Population Command
===================================================
Populates database with categories, departments, courses, and resources.

Usage:
    python manage.py populate_courses
    python manage.py populate_courses --verbosity 2
    python manage.py populate_courses --no-color
    python manage.py populate_courses --clear

Author: StudyDeck Team
Version: 2.0.0
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction, IntegrityError
from django.core.exceptions import ValidationError
from courses.models import Department, Course, Resource
from typing import List, Tuple, Dict
import sys
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Optional fancy output
try:
    from tqdm import tqdm
    from colorama import init, Fore, Style
    init(autoreset=True)
    HAS_FANCY_OUTPUT = True
except ImportError:
    HAS_FANCY_OUTPUT = False
    # Graceful fallback - no warnings needed


class Command(BaseCommand):
    """Django management command to populate BITS Pilani course data"""
    
    help = 'Populates the database with BITS Pilani categories, departments, courses, and resources'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stats = {
            'categories': {'created': 0, 'skipped': 0, 'failed': 0},
            'departments': {'created': 0, 'skipped': 0, 'failed': 0},
            'courses': {'created': 0, 'skipped': 0, 'failed': 0},
            'resources': {'created': 0, 'skipped': 0, 'failed': 0},
        }
        self.verbosity = 1
        self.use_color = True
    
    def add_arguments(self, parser):
        """Add custom command arguments"""
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear existing data before populating (DESTRUCTIVE!)',
        )
    
    def _print(self, message: str, style=None, level: int = 1):
        """Helper to print with verbosity and style control"""
        if self.verbosity >= level:
            if style and self.use_color:
                self.stdout.write(style(message))
            else:
                self.stdout.write(message)
    
    def print_header(self):
        """Print formatted header"""
        if HAS_FANCY_OUTPUT and self.use_color:
            self._print("\n" + "="*80)
            self._print(f"{Fore.CYAN}{Style.BRIGHT}{'BITS PILANI STUDYDECK - DATABASE POPULATION':^80}{Style.RESET_ALL}")
            self._print("="*80 + "\n")
        else:
            self._print("\n" + "="*80)
            self._print("BITS PILANI STUDYDECK - DATABASE POPULATION".center(80))
            self._print("="*80 + "\n")
    
    def print_footer(self):
        """Print comprehensive summary"""
        if HAS_FANCY_OUTPUT and self.use_color:
            self._print("\n" + "="*80)
            self._print(f"{Fore.GREEN}{Style.BRIGHT}{'✓ DATABASE POPULATION COMPLETED':^80}{Style.RESET_ALL}")
            self._print("="*80)
            
            self._print(f"\n{Fore.YELLOW}{Style.BRIGHT}📊 Summary Statistics:{Style.RESET_ALL}")
            
            # Categories
            self._print(f"  {Fore.CYAN}Categories:{Style.RESET_ALL}")
            self._print(f"    ✓ Created: {Fore.GREEN}{self.stats['categories']['created']}{Style.RESET_ALL}")
            self._print(f"    ⊙ Skipped: {Fore.YELLOW}{self.stats['categories']['skipped']}{Style.RESET_ALL}")
            if self.stats['categories']['failed'] > 0:
                self._print(f"    ✗ Failed:  {Fore.RED}{self.stats['categories']['failed']}{Style.RESET_ALL}")
            
            # Departments
            self._print(f"  {Fore.CYAN}Departments:{Style.RESET_ALL}")
            self._print(f"    ✓ Created: {Fore.GREEN}{self.stats['departments']['created']}{Style.RESET_ALL}")
            self._print(f"    ⊙ Skipped: {Fore.YELLOW}{self.stats['departments']['skipped']}{Style.RESET_ALL}")
            if self.stats['departments']['failed'] > 0:
                self._print(f"    ✗ Failed:  {Fore.RED}{self.stats['departments']['failed']}{Style.RESET_ALL}")
            
            # Courses
            self._print(f"  {Fore.CYAN}Courses:{Style.RESET_ALL}")
            self._print(f"    ✓ Created: {Fore.GREEN}{self.stats['courses']['created']}{Style.RESET_ALL}")
            self._print(f"    ⊙ Skipped: {Fore.YELLOW}{self.stats['courses']['skipped']}{Style.RESET_ALL}")
            if self.stats['courses']['failed'] > 0:
                self._print(f"    ✗ Failed:  {Fore.RED}{self.stats['courses']['failed']}{Style.RESET_ALL}")
            
            # Resources
            self._print(f"  {Fore.CYAN}Resources:{Style.RESET_ALL}")
            self._print(f"    ✓ Created: {Fore.GREEN}{self.stats['resources']['created']}{Style.RESET_ALL}")
            self._print(f"    ⊙ Skipped: {Fore.YELLOW}{self.stats['resources']['skipped']}{Style.RESET_ALL}")
            if self.stats['resources']['failed'] > 0:
                self._print(f"    ✗ Failed:  {Fore.RED}{self.stats['resources']['failed']}{Style.RESET_ALL}")
            
            self._print(f"\n{Fore.MAGENTA}📈 Final Database State:{Style.RESET_ALL}")
        else:
            self._print("\n" + "="*80)
            self._print("✓ DATABASE POPULATION COMPLETED".center(80))
            self._print("="*80)
            self._print("\n📊 Summary Statistics:")
            self._print(f"  Categories: Created={self.stats['categories']['created']}, "
                       f"Skipped={self.stats['categories']['skipped']}, "
                       f"Failed={self.stats['categories']['failed']}")
            self._print(f"  Departments: Created={self.stats['departments']['created']}, "
                       f"Skipped={self.stats['departments']['skipped']}, "
                       f"Failed={self.stats['departments']['failed']}")
            self._print(f"  Courses: Created={self.stats['courses']['created']}, "
                       f"Skipped={self.stats['courses']['skipped']}, "
                       f"Failed={self.stats['courses']['failed']}")
            self._print(f"  Resources: Created={self.stats['resources']['created']}, "
                       f"Skipped={self.stats['resources']['skipped']}, "
                       f"Failed={self.stats['resources']['failed']}")
            self._print("\n📈 Final Database State:")
        
        # Database counts
        try:
            from threads.models import Category
            self._print(f"  • Total Categories: {Category.objects.count()}")
            self._print(f"  • Total Departments: {Department.objects.count()}")
            self._print(f"  • Total Courses: {Course.objects.count()}")
            self._print(f"  • Total Resources: {Resource.objects.count()}")
        except Exception as e:
            self._print(f"  • Error querying database: {e}", self.style.ERROR)
        
        self._print("")
    
    def get_categories_data(self) -> List[str]:
        """Returns list of forum categories"""
        return [
            # Academic Categories
            "General Queries",
            "Course Discussion",
            "Exam Preparation",
            "Assignment Help",
            "Project Collaboration",
            "Study Groups",
            "Lecture Notes",
            "Previous Year Questions",
            
            # Subject-wise Categories
            "Computer Science & Programming",
            "Mathematics & Statistics",
            "Physics & Applied Sciences",
            "Chemistry & Chemical Sciences",
            "Electronics & Communication",
            "Mechanical Engineering",
            "Civil Engineering",
            "Biological Sciences",
            "Economics & Finance",
            "Management & Entrepreneurship",
            "Humanities & Social Sciences",
            
            # Campus Life
            "Campus Life",
            "Hostel & Accommodation",
            "Clubs & Activities",
            "Events & Fests",
            "Sports & Recreation",
            "Food & Mess",
            
            # Career & Placement
            "Internships",
            "Placements",
            "Higher Studies",
            "Career Guidance",
            "Interview Experiences",
            "Resume Reviews",
            
            # Resources & Tools
            "Online Courses",
            "Books & References",
            "Software & Tools",
            "Coding Platforms",
            
            # Other
            "Off-Topic",
            "Announcements",
            "Feedback & Suggestions",
            "Lost & Found",
            "Buy & Sell",
        ]
    
    def get_departments_data(self) -> List[str]:
        """Returns comprehensive list of BITS Pilani departments"""
        return [
            "Computer Science & Information Systems",
            "Electrical & Electronics Engineering",
            "Electronics & Instrumentation Engineering",
            "Mechanical Engineering",
            "Chemical Engineering",
            "Civil Engineering",
            "Manufacturing Engineering",
            "Mathematics",
            "Physics",
            "Chemistry",
            "Biological Sciences",
            "Economics & Finance",
            "Management",
            "Pharmacy",
            "Humanities & Social Sciences",
            "General Studies",
            "Languages",
        ]
    
    def get_courses_data(self) -> List[Tuple[str, str, str]]:
        """Returns list of (code, title, department_name)"""
        return [
            # Computer Science (23 courses)
            ("CS F111", "Computer Programming", "Computer Science & Information Systems"),
            ("CS F211", "Data Structures & Algorithms", "Computer Science & Information Systems"),
            ("CS F212", "Database Systems", "Computer Science & Information Systems"),
            ("CS F213", "Object Oriented Programming", "Computer Science & Information Systems"),
            ("CS F214", "Logic in Computer Science", "Computer Science & Information Systems"),
            ("CS F215", "Digital Design", "Computer Science & Information Systems"),
            ("CS F222", "Discrete Structures for Computer Science", "Computer Science & Information Systems"),
            ("CS F241", "Microprocessors & Interfacing", "Computer Science & Information Systems"),
            ("CS F301", "Principles of Programming Languages", "Computer Science & Information Systems"),
            ("CS F303", "Computer Networks", "Computer Science & Information Systems"),
            ("CS F320", "Foundations of Data Science", "Computer Science & Information Systems"),
            ("CS F342", "Computer Architecture", "Computer Science & Information Systems"),
            ("CS F351", "Theory of Computation", "Computer Science & Information Systems"),
            ("CS F363", "Compiler Construction", "Computer Science & Information Systems"),
            ("CS F364", "Design & Analysis of Algorithms", "Computer Science & Information Systems"),
            ("CS F372", "Operating Systems", "Computer Science & Information Systems"),
            ("CS F407", "Artificial Intelligence", "Computer Science & Information Systems"),
            ("CS F415", "Data Mining", "Computer Science & Information Systems"),
            ("CS F425", "Deep Learning", "Computer Science & Information Systems"),
            ("CS F429", "Natural Language Processing", "Computer Science & Information Systems"),
            ("CS F441", "Computer Graphics", "Computer Science & Information Systems"),
            ("CS F446", "Machine Learning", "Computer Science & Information Systems"),
            ("CS F469", "Information Retrieval", "Computer Science & Information Systems"),
            
            # Electrical & Electronics (8 courses)
            ("EEE F111", "Electrical Sciences", "Electrical & Electronics Engineering"),
            ("EEE F211", "Electrical Machines", "Electrical & Electronics Engineering"),
            ("EEE F212", "Electrical & Electronics Circuits", "Electrical & Electronics Engineering"),
            ("EEE F241", "Signals & Systems", "Electrical & Electronics Engineering"),
            ("EEE F242", "Control Systems", "Electrical & Electronics Engineering"),
            ("EEE F243", "Digital Design", "Electrical & Electronics Engineering"),
            ("EEE F311", "Power Electronics", "Electrical & Electronics Engineering"),
            ("EEE F342", "Electromagnetic Theory", "Electrical & Electronics Engineering"),
            
            # Electronics & Instrumentation (4 courses)
            ("ENI F111", "Engineering Graphics", "Electronics & Instrumentation Engineering"),
            ("ENI F243", "Analog Electronics", "Electronics & Instrumentation Engineering"),
            ("ENI F244", "Digital Circuits & Systems", "Electronics & Instrumentation Engineering"),
            ("ENI F342", "Control Systems", "Electronics & Instrumentation Engineering"),
            
            # Mechanical Engineering (6 courses)
            ("MECH F110", "Engineering Mechanics", "Mechanical Engineering"),
            ("MECH F211", "Thermodynamics", "Mechanical Engineering"),
            ("MECH F222", "Manufacturing Processes", "Mechanical Engineering"),
            ("MECH F241", "Mechanics of Solids", "Mechanical Engineering"),
            ("MECH F342", "Fluid Mechanics", "Mechanical Engineering"),
            ("MECH F343", "Heat Transfer", "Mechanical Engineering"),
            
            # Chemical Engineering (4 courses)
            ("CHE F211", "Chemical Engineering Thermodynamics", "Chemical Engineering"),
            ("CHE F241", "Fluid Mechanics for Chemical Engineers", "Chemical Engineering"),
            ("CHE F243", "Material & Energy Balance", "Chemical Engineering"),
            ("CHE F244", "Mass Transfer", "Chemical Engineering"),
            
            # Civil Engineering (4 courses)
            ("CE F111", "Civil Engineering Materials", "Civil Engineering"),
            ("CE F211", "Strength of Materials", "Civil Engineering"),
            ("CE F241", "Structural Analysis", "Civil Engineering"),
            ("CE F242", "Geotechnical Engineering", "Civil Engineering"),
            
            # Mathematics (6 courses)
            ("MATH F111", "Mathematics I", "Mathematics"),
            ("MATH F112", "Mathematics II", "Mathematics"),
            ("MATH F113", "Probability & Statistics", "Mathematics"),
            ("MATH F211", "Mathematics III", "Mathematics"),
            ("MATH F241", "Numerical Analysis", "Mathematics"),
            ("MATH F342", "Operations Research", "Mathematics"),
            
            # Physics (5 courses)
            ("PHY F110", "Mechanics, Oscillations & Waves", "Physics"),
            ("PHY F111", "Electricity & Magnetism", "Physics"),
            ("PHY F112", "Thermodynamics", "Physics"),
            ("BITS F111", "Thermodynamics", "Physics"),
            ("PHY F211", "Quantum Mechanics & Applications", "Physics"),
            
            # Chemistry (4 courses)
            ("CHEM F110", "Chemistry Laboratory", "Chemistry"),
            ("CHEM F111", "General Chemistry", "Chemistry"),
            ("CHEM F211", "Physical Chemistry", "Chemistry"),
            ("CHEM F212", "Organic Chemistry I", "Chemistry"),
            
            # Biological Sciences (3 courses)
            ("BIO F110", "Biology Laboratory", "Biological Sciences"),
            ("BIO F111", "General Biology", "Biological Sciences"),
            ("BIO F211", "Biological Chemistry", "Biological Sciences"),
            
            # Economics & Finance (4 courses)
            ("ECON F211", "Principles of Economics", "Economics & Finance"),
            ("ECON F243", "Microeconomics", "Economics & Finance"),
            ("ECON F244", "Macroeconomics", "Economics & Finance"),
            ("FIN F211", "Financial Management", "Economics & Finance"),
            
            # Management (3 courses)
            ("MGTS F211", "Principles of Management", "Management"),
            ("MGTS F241", "Organizational Behavior", "Management"),
            ("MGTS F242", "Marketing Management", "Management"),
            
            # Humanities & Social Sciences (3 courses)
            ("HSS F211", "Philosophy", "Humanities & Social Sciences"),
            ("HSS F222", "Professional Ethics", "Humanities & Social Sciences"),
            ("HSS F341", "Indian Constitution", "Humanities & Social Sciences"),
            
            # General Studies (1 course)
            ("GS F111", "General Studies", "General Studies"),
            
            # Languages (1 course)
            ("ENGL C111", "English Proficiency", "Languages"),
        ]
    
    def get_resources_data(self) -> List[Tuple[str, str, str, str]]:
        """Returns list of (course_code, title, type, link)"""
        return [
            # CS F111 (4 resources)
            ("CS F111", "Introduction to C Programming - Complete Guide", "PDF", "https://drive.google.com/file/d/sample-c-intro"),
            ("CS F111", "Arrays and Pointers Masterclass", "VIDEO", "https://youtube.com/watch?v=sample-arrays"),
            ("CS F111", "C Programming Tutorial - GeeksforGeeks", "LINK", "https://www.geeksforgeeks.org/c-programming-language/"),
            ("CS F111", "Lab Manual - C Programming", "PDF", "https://drive.google.com/file/d/sample-lab-manual"),
            
            # CS F211 (4 resources)
            ("CS F211", "Data Structures Complete Notes", "PDF", "https://drive.google.com/file/d/sample-ds-notes"),
            ("CS F211", "Binary Trees Deep Dive", "VIDEO", "https://youtube.com/watch?v=sample-trees"),
            ("CS F211", "Graph Algorithms Visualization", "VIDEO", "https://youtube.com/watch?v=sample-graphs"),
            ("CS F211", "VisuAlgo - Algorithm Visualizer", "LINK", "https://visualgo.net/"),
            
            # CS F212 (4 resources)
            ("CS F212", "SQL Tutorial Complete Course", "VIDEO", "https://youtube.com/watch?v=sample-sql"),
            ("CS F212", "Database Design and Normalization", "PDF", "https://drive.google.com/file/d/sample-db-design"),
            ("CS F212", "ER Diagrams Step by Step", "PDF", "https://drive.google.com/file/d/sample-er-diagrams"),
            ("CS F212", "PostgreSQL Official Documentation", "LINK", "https://www.postgresql.org/docs/"),
            
            # CS F213 (3 resources)
            ("CS F213", "Java OOP Concepts Detailed", "PDF", "https://drive.google.com/file/d/sample-java-oop"),
            ("CS F213", "Inheritance and Polymorphism Tutorial", "VIDEO", "https://youtube.com/watch?v=sample-inheritance"),
            ("CS F213", "Design Patterns - Gang of Four", "LINK", "https://refactoring.guru/design-patterns"),
            
            # CS F303 (3 resources)
            ("CS F303", "Computer Networks - Tanenbaum", "PDF", "https://drive.google.com/file/d/sample-networks-book"),
            ("CS F303", "OSI Model Explained", "VIDEO", "https://youtube.com/watch?v=sample-osi"),
            ("CS F303", "TCP/IP Protocol Suite Guide", "PDF", "https://drive.google.com/file/d/sample-tcpip"),
            
            # CS F342 (3 resources)
            ("CS F342", "Computer Organization and Architecture", "PDF", "https://drive.google.com/file/d/sample-comp-org"),
            ("CS F342", "Pipeline Processing Explained", "VIDEO", "https://youtube.com/watch?v=sample-pipeline"),
            ("CS F342", "Cache Memory Deep Dive", "VIDEO", "https://youtube.com/watch?v=sample-cache"),
            
            # CS F364 (3 resources)
            ("CS F364", "Algorithm Analysis Complete Guide", "PDF", "https://drive.google.com/file/d/sample-algo-analysis"),
            ("CS F364", "Dynamic Programming Masterclass", "VIDEO", "https://youtube.com/watch?v=sample-dp"),
            ("CS F364", "LeetCode Algorithm Problems", "LINK", "https://leetcode.com/problemset/"),
            
            # CS F372 (3 resources)
            ("CS F372", "Operating System Concepts - Silberschatz", "PDF", "https://drive.google.com/file/d/sample-os-book"),
            ("CS F372", "Process Scheduling Algorithms", "VIDEO", "https://youtube.com/watch?v=sample-scheduling"),
            ("CS F372", "Deadlock Prevention and Avoidance", "PDF", "https://drive.google.com/file/d/sample-deadlock"),
            
            # CS F407 (3 resources)
            ("CS F407", "AI: A Modern Approach - Russell & Norvig", "PDF", "https://drive.google.com/file/d/sample-ai-book"),
            ("CS F407", "Search Algorithms in AI", "VIDEO", "https://youtube.com/watch?v=sample-search"),
            ("CS F407", "Stanford AI Course", "LINK", "https://stanford.edu/~cpiech/cs221/"),
            
            # CS F446 (4 resources)
            ("CS F446", "Machine Learning Crash Course by Google", "LINK", "https://developers.google.com/machine-learning/crash-course"),
            ("CS F446", "Linear Regression from Scratch", "VIDEO", "https://youtube.com/watch?v=sample-linear-reg"),
            ("CS F446", "Neural Networks Fundamentals", "PDF", "https://drive.google.com/file/d/sample-nn-basics"),
            ("CS F446", "Scikit-learn Official Documentation", "LINK", "https://scikit-learn.org/stable/"),
            
            # MATH F111 (3 resources)
            ("MATH F111", "Calculus - Early Transcendentals", "PDF", "https://drive.google.com/file/d/sample-calculus"),
            ("MATH F111", "Differential Equations Tutorial", "VIDEO", "https://youtube.com/watch?v=sample-diff-eq"),
            ("MATH F111", "3Blue1Brown Calculus Series", "LINK", "https://www.youtube.com/playlist?list=PLZHQObOWTQDMsr9K"),
            
            # MATH F113 (3 resources)
            ("MATH F113", "Probability Theory Complete Notes", "PDF", "https://drive.google.com/file/d/sample-prob"),
            ("MATH F113", "Statistical Inference Tutorial", "VIDEO", "https://youtube.com/watch?v=sample-stats"),
            ("MATH F113", "Khan Academy Statistics Course", "LINK", "https://www.khanacademy.org/math/statistics-probability"),
            
            # PHY F110 (3 resources)
            ("PHY F110", "Classical Mechanics - Goldstein", "PDF", "https://drive.google.com/file/d/sample-mechanics"),
            ("PHY F110", "Newton's Laws Applications", "VIDEO", "https://youtube.com/watch?v=sample-newton"),
            ("PHY F110", "PhET Interactive Physics Simulations", "LINK", "https://phet.colorado.edu/"),
            
            # PHY F111 (2 resources)
            ("PHY F111", "Electromagnetism - Griffiths", "PDF", "https://drive.google.com/file/d/sample-em"),
            ("PHY F111", "Maxwell's Equations Explained", "VIDEO", "https://youtube.com/watch?v=sample-maxwell"),
            
            # CHEM F111 (3 resources)
            ("CHEM F111", "General Chemistry Lab Manual", "PDF", "https://drive.google.com/file/d/sample-chem-lab"),
            ("CHEM F111", "Organic Chemistry Basics", "VIDEO", "https://youtube.com/watch?v=sample-organic"),
            ("CHEM F111", "Interactive Periodic Table", "LINK", "https://ptable.com/"),
            
            # EEE F111 (3 resources)
            ("EEE F111", "Circuit Theory Fundamentals", "PDF", "https://drive.google.com/file/d/sample-circuits"),
            ("EEE F111", "Kirchhoff's Laws Deep Dive", "VIDEO", "https://youtube.com/watch?v=sample-kirchhoff"),
            ("EEE F111", "Falstad Circuit Simulator", "LINK", "https://www.falstad.com/circuit/"),
            
            # MECH F110 (2 resources)
            ("MECH F110", "Statics and Dynamics - Beer & Johnston", "PDF", "https://drive.google.com/file/d/sample-statics"),
            ("MECH F110", "Free Body Diagrams Tutorial", "VIDEO", "https://youtube.com/watch?v=sample-fbd"),
            
            # MECH F211 (2 resources)
            ("MECH F211", "Thermodynamics - Cengel & Boles", "PDF", "https://drive.google.com/file/d/sample-thermo"),
            ("MECH F211", "First Law of Thermodynamics", "VIDEO", "https://youtube.com/watch?v=sample-first-law"),
            
            # BIO F111 (2 resources)
            ("BIO F111", "Campbell Biology 11th Edition", "PDF", "https://drive.google.com/file/d/sample-biology"),
            ("BIO F111", "Khan Academy Biology Course", "LINK", "https://www.khanacademy.org/science/biology"),
            
            # ECON F211 (2 resources)
            ("ECON F211", "Principles of Economics - Mankiw", "PDF", "https://drive.google.com/file/d/sample-econ-book"),
            ("ECON F211", "MIT OpenCourseWare Economics", "LINK", "https://ocw.mit.edu/courses/economics/"),
            
            # MGTS F211 (2 resources)
            ("MGTS F211", "Management Principles - Robbins", "PDF", "https://drive.google.com/file/d/sample-mgmt"),
            ("MGTS F211", "Harvard Business Review", "LINK", "https://hbr.org/"),
        ]
    
    @transaction.atomic
    def populate_categories(self):
        """Populate categories with robust error handling"""
        categories_data = self.get_categories_data()
        
        if HAS_FANCY_OUTPUT and self.use_color:
            self._print(f"\n{Fore.BLUE}{Style.BRIGHT}📂 Populating Categories...{Style.RESET_ALL}")
            iterator = tqdm(categories_data, 
                          bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]',
                          colour='blue',
                          disable=(self.verbosity < 1))
        else:
            self._print("\n📂 Populating Categories...")
            iterator = categories_data
        
        for category_name in iterator:
            try:
                # Validate data
                if not category_name or not category_name.strip():
                    self.stats['categories']['failed'] += 1
                    self._print(f"  ✗ Empty category name skipped", self.style.WARNING, level=2)
                    continue
                
                # Import Category model
                from threads.models import Category
                
                category, created = Category.objects.get_or_create(name=category_name.strip())
                
                if created:
                    self.stats['categories']['created'] += 1
                    self._print(f"  ✓ Created: {category_name}", level=2)
                else:
                    self.stats['categories']['skipped'] += 1
                    self._print(f"  ⊙ Exists: {category_name}", level=2)
                    
            except (IntegrityError, ValidationError) as e:
                self.stats['categories']['failed'] += 1
                self._print(f"  ✗ Error with '{category_name}': {e}", self.style.ERROR, level=1)
            except Exception as e:
                self.stats['categories']['failed'] += 1
                logger.exception(f"Unexpected error creating category '{category_name}'")
                self._print(f"  ✗ Unexpected error: {e}", self.style.ERROR, level=1)
    
    @transaction.atomic
    def populate_departments(self):
        """Populate departments with robust error handling"""
        departments_data = self.get_departments_data()
        
        if HAS_FANCY_OUTPUT and self.use_color:
            self._print(f"\n{Fore.CYAN}{Style.BRIGHT}📚 Populating Departments...{Style.RESET_ALL}")
            iterator = tqdm(departments_data, 
                          bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]',
                          colour='cyan',
                          disable=(self.verbosity < 1))
        else:
            self._print("\n📚 Populating Departments...")
            iterator = departments_data
        
        for dept_name in iterator:
            try:
                # Validate data
                if not dept_name or not dept_name.strip():
                    self.stats['departments']['failed'] += 1
                    self._print(f"  ✗ Empty department name skipped", self.style.WARNING, level=2)
                    continue
                
                dept, created = Department.objects.get_or_create(name=dept_name.strip())
                
                if created:
                    self.stats['departments']['created'] += 1
                    self._print(f"  ✓ Created: {dept_name}", level=2)
                else:
                    self.stats['departments']['skipped'] += 1
                    self._print(f"  ⊙ Exists: {dept_name}", level=2)
                    
            except (IntegrityError, ValidationError) as e:
                self.stats['departments']['failed'] += 1
                self._print(f"  ✗ Error with '{dept_name}': {e}", self.style.ERROR, level=1)
            except Exception as e:
                self.stats['departments']['failed'] += 1
                logger.exception(f"Unexpected error creating department '{dept_name}'")
                self._print(f"  ✗ Unexpected error: {e}", self.style.ERROR, level=1)
    
    @transaction.atomic
    def populate_courses(self):
        """Populate courses with robust error handling"""
        courses_data = self.get_courses_data()
        
        if HAS_FANCY_OUTPUT and self.use_color:
            self._print(f"\n{Fore.MAGENTA}{Style.BRIGHT}📖 Populating Courses...{Style.RESET_ALL}")
            iterator = tqdm(courses_data,
                          bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]',
                          colour='magenta',
                          disable=(self.verbosity < 1))
        else:
            self._print("\n📖 Populating Courses...")
            iterator = courses_data
        
        for code, title, dept_name in iterator:
            try:
                # Validate data
                if not code or not code.strip():
                    self.stats['courses']['failed'] += 1
                    self._print(f"  ✗ Empty course code skipped", self.style.WARNING, level=2)
                    continue
                
                if not title or not title.strip():
                    self.stats['courses']['failed'] += 1
                    self._print(f"  ✗ Course {code}: Empty title skipped", self.style.WARNING, level=2)
                    continue
                
                # Get department
                dept = Department.objects.get(name=dept_name)
                
                # Create or get course
                course, created = Course.objects.get_or_create(
                    code=code.strip(),
                    defaults={'title': title.strip(), 'department': dept}
                )
                
                if created:
                    self.stats['courses']['created'] += 1
                    self._print(f"  ✓ Created: {code} - {title}", level=2)
                else:
                    self.stats['courses']['skipped'] += 1
                    self._print(f"  ⊙ Exists: {code}", level=2)
                    
            except Department.DoesNotExist:
                self.stats['courses']['failed'] += 1
                self._print(f"  ✗ {code}: Department '{dept_name}' not found", self.style.ERROR, level=1)
            except (IntegrityError, ValidationError) as e:
                self.stats['courses']['failed'] += 1
                self._print(f"  ✗ {code}: {e}", self.style.ERROR, level=1)
            except Exception as e:
                self.stats['courses']['failed'] += 1
                logger.exception(f"Unexpected error creating course '{code}'")
                self._print(f"  ✗ {code}: Unexpected error - {e}", self.style.ERROR, level=1)
    
    @transaction.atomic
    def populate_resources(self):
        """Populate resources with robust error handling"""
        resources_data = self.get_resources_data()
        
        if HAS_FANCY_OUTPUT and self.use_color:
            self._print(f"\n{Fore.YELLOW}{Style.BRIGHT}📁 Populating Resources...{Style.RESET_ALL}")
            iterator = tqdm(resources_data,
                          bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]',
                          colour='yellow',
                          disable=(self.verbosity < 1))
        else:
            self._print("\n📁 Populating Resources...")
            iterator = resources_data
        
        for course_code, title, res_type, link in iterator:
            try:
                # Validate data
                if not course_code or not course_code.strip():
                    self.stats['resources']['failed'] += 1
                    continue
                
                if not title or not title.strip():
                    self.stats['resources']['failed'] += 1
                    self._print(f"  ✗ {course_code}: Empty resource title", self.style.WARNING, level=2)
                    continue
                
                if res_type not in ['PDF', 'VIDEO', 'LINK']:
                    self.stats['resources']['failed'] += 1
                    self._print(f"  ✗ {course_code}: Invalid type '{res_type}'", self.style.WARNING, level=2)
                    continue
                
                # Get course
                course = Course.objects.get(code=course_code.strip())
                
                # Create or get resource
                resource, created = Resource.objects.get_or_create(
                    course=course,
                    title=title.strip(),
                    defaults={'type': res_type, 'link': link.strip()}
                )
                
                if created:
                    self.stats['resources']['created'] += 1
                    self._print(f"  ✓ Created: {course_code} - {title[:40]}", level=2)
                else:
                    self.stats['resources']['skipped'] += 1
                    self._print(f"  ⊙ Exists: {course_code} - {title[:40]}", level=2)
                    
            except Course.DoesNotExist:
                self.stats['resources']['failed'] += 1
                self._print(f"  ✗ Resource: Course '{course_code}' not found", self.style.WARNING, level=2)
            except (IntegrityError, ValidationError) as e:
                self.stats['resources']['failed'] += 1
                self._print(f"  ✗ {course_code}: {e}", self.style.ERROR, level=1)
            except Exception as e:
                self.stats['resources']['failed'] += 1
                logger.exception(f"Unexpected error creating resource for '{course_code}'")
                self._print(f"  ✗ {course_code}: Unexpected error - {e}", self.style.ERROR, level=1)
    
    def clear_existing_data(self):
        """Clear all existing data (DESTRUCTIVE!)"""
        self._print("\n⚠️  WARNING: Clearing existing data...", self.style.WARNING)
        
        try:
            from threads.models import Category
            
            category_count = Category.objects.count()
            resource_count = Resource.objects.count()
            course_count = Course.objects.count()
            dept_count = Department.objects.count()
            
            with transaction.atomic():
                Category.objects.all().delete()
                Resource.objects.all().delete()
                Course.objects.all().delete()
                Department.objects.all().delete()
            
            self._print(f"  ✓ Deleted {category_count} categories", self.style.SUCCESS)
            self._print(f"  ✓ Deleted {resource_count} resources", self.style.SUCCESS)
            self._print(f"  ✓ Deleted {course_count} courses", self.style.SUCCESS)
            self._print(f"  ✓ Deleted {dept_count} departments", self.style.SUCCESS)
        except Exception as e:
            raise CommandError(f"Failed to clear data: {e}")
    
    def handle(self, *args, **options):
        """Main command handler with comprehensive error handling"""
        # Store options
        self.verbosity = options.get('verbosity', 1)
        self.use_color = not options.get('no_color', False) and sys.stdout.isatty()
        clear_data = options.get('clear', False)
        
        try:
            # Print header
            self.print_header()
            
            # Clear data if requested
            if clear_data:
                self.clear_existing_data()
            
            # Populate data
            self.populate_categories()
            self.populate_departments()
            self.populate_courses()
            self.populate_resources()
            
            # Print summary
            self.print_footer()
            
            # Final status
            total_failed = (self.stats['categories']['failed'] +
                          self.stats['departments']['failed'] + 
                          self.stats['courses']['failed'] + 
                          self.stats['resources']['failed'])
            
            if total_failed > 0:
                self._print(self.style.WARNING(
                    f'\n⚠️  Completed with {total_failed} failures. Check logs for details.\n'
                ))
            else:
                self._print(self.style.SUCCESS(
                    '\n✓ Population completed successfully with no errors!\n'
                ))
            
        except KeyboardInterrupt:
            self._print(self.style.ERROR('\n\n⚠️  Operation cancelled by user!\n'))
            sys.exit(130)  # Standard exit code for Ctrl+C
            
        except Exception as e:
            logger.exception("Fatal error during population")
            raise CommandError(f'Fatal error: {e}')
