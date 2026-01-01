"""
BITS Pilani StudyDeck - Realistic Forum Data Population
========================================================
Generates authentic BITS-themed forum content with parallel processing.

Features:
- Multi-core parallel processing
- Colored progress bars
- Email safety locks
- Exponential backoff for DB conflicts
- Real BITS course codes and templates

Usage:
    python manage.py populate_forum
    python manage.py populate_forum --users 200 --threads 5000
"""

import random
import time
import os
import sys
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from unittest.mock import patch
from faker import Faker
from django.core.management.base import BaseCommand
from django.db import transaction, connections, OperationalError, IntegrityError
from django.contrib.auth import get_user_model
from django.conf import settings
from django.core import mail

# Import your models
from courses.models import Department, Course, Resource
from threads.models import Category, Tag, Thread, Reply, Report

# Optional fancy output
try:
    from tqdm import tqdm
    from colorama import init, Fore, Style, Back
    init(autoreset=True)
    HAS_FANCY_OUTPUT = True
except ImportError:
    HAS_FANCY_OUTPUT = False
    tqdm = None

User = get_user_model()
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
NUM_USERS = 100
TOTAL_THREADS = 2000
MAX_REPLIES = 8
MAX_UPVOTES = 20
WORKERS = os.cpu_count() or 4

# --- BITS PILANI CONTEXT DATA ---
DEPARTMENTS = [
    "Computer Science & Information Systems",
    "Electrical & Electronics Engineering",
    "Electronics & Instrumentation Engineering",
    "Mechanical Engineering",
    "Chemical Engineering",
    "Civil Engineering",
    "Biological Sciences",
    "Physics",
    "Mathematics",
    "Chemistry",
    "Humanities & Social Sciences",
    "Economics & Finance",
    "Management",
    "Pharmacy"
]

BITS_COURSES = [
    ("CS", "F111", "Computer Programming"),
    ("CS", "F211", "Data Structures & Algorithms"),
    ("CS", "F212", "Database Systems"),
    ("CS", "F213", "Object Oriented Programming"),
    ("CS", "F301", "Principles of Programming Languages"),
    ("CS", "F303", "Computer Networks"),
    ("CS", "F372", "Operating Systems"),
    ("CS", "F407", "Artificial Intelligence"),
    ("CS", "F446", "Machine Learning"),
    ("EEE", "F111", "Electrical Sciences"),
    ("EEE", "F211", "Electrical Machines"),
    ("MATH", "F111", "Mathematics I"),
    ("MATH", "F113", "Probability & Statistics"),
    ("PHY", "F110", "Mechanics, Oscillations & Waves"),
    ("PHY", "F111", "Electricity & Magnetism"),
    ("CHEM", "F111", "General Chemistry"),
    ("BIO", "F111", "General Biology"),
    ("ECON", "F211", "Principles of Economics"),
    ("MGTS", "F211", "Principles of Management"),
    ("HSS", "F222", "Professional Ethics")
]

CATEGORIES = [
    "General Queries",
    "Course Discussion",
    "Exam Preparation",
    "Assignment Help",
    "Project Collaboration",
    "Resource Sharing",
    "Professor Reviews",
    "Campus Life",
    "Internships",
    "Lost & Found"
]

TAG_DATA = [
    ("#midsem", "#FF5733"),
    ("#compre", "#C70039"),
    ("#urgent", "#FFC300"),
    ("#resources", "#DAF7A6"),
    ("#quiz", "#33FF57"),
    ("#ps1", "#3371FF"),
    ("#grading", "#2C3E50"),
    ("#attendance", "#E74C3C"),
    ("#lite", "#1ABC9C"),
    ("#doubt", "#9B59B6")
]

TEMPLATES = [
    ("Has the {code} handout been released?", "academics"),
    ("How is the grading for {code} with Prof. {prof}?", "review"),
    ("Anyone have previous year midsem papers for {code}?", "resource"),
    ("Is attendance compulsory for {code} lectures?", "academics"),
    ("Found a {item} in {loc}. DM to claim.", "lost_found"),
    ("Opinion on taking {code} and {code2} together?", "advice"),
    ("Review of Prof. {prof} for {code}? Is he lite?", "review"),
    ("Can someone explain the {code} tutorial problem set?", "academics"),
    ("Looking for PS station reviews for {code} domain", "ps"),
    ("Best resources for learning {code}?", "resource"),
]

LOCATIONS = ["NAB", "FD-1", "FD-2", "FD-3", "Library", "Rotunda", "Sky Lawns", "C'not", "Lecture Hall Complex"]
ITEMS = ["ID Card", "Calculator", "Water Bottle", "Umbrella", "Charger", "Notebook", "Lab Coat"]


def create_content_worker(count, user_ids, course_map, cat_ids, tag_ids):
    """
    Worker process to generate data in parallel.
    Includes SAFETY LOCKS to ensure no emails are sent.
    """
    # SAFETY: Override Settings in this process
    settings.EMAIL_BACKEND = 'django.core.mail.backends.locmem.EmailBackend'
    
    # SAFETY: Patch Email functions
    p1 = patch('threads.utils.queue_mail')
    p2 = patch('threads.utils.queue_mass_mail')
    p1.start()
    p2.start()

    # Reset DB Connection
    connections.close_all()
    
    fake = Faker('en_IN')
    created = 0
    course_ids = list(course_map.keys())

    for _ in range(count):
        retry_delay = 0.1
        for attempt in range(5):
            try:
                with transaction.atomic():
                    # --- PREPARE DATA ---
                    author_id = random.choice(user_ids)
                    cat_id = random.choice(cat_ids)
                    
                    # Template Logic
                    tmpl_str, tmpl_type = random.choice(TEMPLATES)
                    relevant_course_id = random.choice(course_ids)
                    code1 = course_map[relevant_course_id]
                    code2 = course_map[random.choice(course_ids)]
                    
                    title = tmpl_str.format(
                        code=code1,
                        code2=code2,
                        prof=fake.last_name(),
                        loc=random.choice(LOCATIONS),
                        item=random.choice(ITEMS)
                    )

                    content = f"{fake.paragraph(nb_sentences=4)}\n\nRef: **{code1}**"

                    # --- CREATE THREAD ---
                    thread = Thread.objects.create(
                        title=title,
                        raw_content=content,
                        author_id=author_id,
                        category_id=cat_id,
                        is_locked=random.random() < 0.05
                    )
                    
                    # Tags/Courses
                    if tmpl_type in ['academics', 'review', 'resource']:
                        thread.tagged_courses.add(relevant_course_id)
                    if random.random() > 0.6:
                        thread.tags.add(random.choice(tag_ids))

                    # --- UPVOTES ---
                    num_votes = random.randint(0, MAX_UPVOTES)
                    if num_votes > 0:
                        voters = random.sample(user_ids, k=min(num_votes, len(user_ids)))
                        thread.upvotes.set(voters)
                        thread.upvote_count = num_votes
                        thread.save(update_fields=['upvote_count'])

                    # --- REPLIES ---
                    for _ in range(random.randint(0, MAX_REPLIES)):
                        r_content = fake.sentence()
                        if random.random() < 0.3:
                            r_content = f"@{User.objects.get(id=author_id).username} {r_content}"
                        
                        Reply.objects.create(
                            thread=thread,
                            author_id=random.choice(user_ids),
                            raw_content=r_content
                        )

                created += 1
                break  # Success

            except OperationalError:
                time.sleep(retry_delay)
                retry_delay *= 1.5
            except Exception as e:
                logger.exception(f"Error creating thread: {e}")
                break

    p1.stop()
    p2.stop()
    return created


class Command(BaseCommand):
    help = 'Populates the DB with realistic BITS Pilani forum data using parallel processing'

    def add_arguments(self, parser):
        parser.add_argument(
            '--users',
            type=int,
            default=NUM_USERS,
            help=f'Number of users to create (default: {NUM_USERS})'
        )
        parser.add_argument(
            '--threads',
            type=int,
            default=TOTAL_THREADS,
            help=f'Number of threads to create (default: {TOTAL_THREADS})'
        )
        parser.add_argument(
            '--workers',
            type=int,
            default=WORKERS,
            help=f'Number of parallel workers (default: {WORKERS})'
        )

    def _print(self, message, style=None):
        """Helper for styled output"""
        if style:
            self.stdout.write(style(message))
        else:
            self.stdout.write(message)

    def print_header(self):
        """Print colorful header"""
        if HAS_FANCY_OUTPUT:
            self._print("\n" + "="*80)
            self._print(f"{Fore.CYAN}{Style.BRIGHT}{'BITS PILANI STUDYDECK - FORUM DATA POPULATION':^80}{Style.RESET_ALL}")
            self._print("="*80)
            self._print(f"\n{Fore.YELLOW}⚠️  SAFETY MODE: All emails will be captured in memory{Style.RESET_ALL}")
            self._print(f"{Fore.GREEN}✓ Multi-core processing enabled{Style.RESET_ALL}\n")
        else:
            self._print("\n" + "="*80)
            self._print("BITS PILANI STUDYDECK - FORUM DATA POPULATION".center(80))
            self._print("="*80)
            self._print("\n⚠️  SAFETY MODE: All emails will be captured in memory")
            self._print("✓ Multi-core processing enabled\n")

    def print_summary(self, stats, duration):
        """Print beautiful summary"""
        if HAS_FANCY_OUTPUT:
            self._print("\n" + "="*80)
            self._print(f"{Fore.GREEN}{Style.BRIGHT}{'✓ POPULATION COMPLETED':^80}{Style.RESET_ALL}")
            self._print("="*80)
            
            self._print(f"\n{Fore.YELLOW}{Style.BRIGHT}📊 Summary Statistics:{Style.RESET_ALL}")
            self._print(f"  {Fore.CYAN}Users:{Style.RESET_ALL} {stats['users']}")
            self._print(f"  {Fore.CYAN}Departments:{Style.RESET_ALL} {stats['departments']}")
            self._print(f"  {Fore.CYAN}Courses:{Style.RESET_ALL} {stats['courses']}")
            self._print(f"  {Fore.CYAN}Resources:{Style.RESET_ALL} {stats['resources']}")
            self._print(f"  {Fore.CYAN}Categories:{Style.RESET_ALL} {stats['categories']}")
            self._print(f"  {Fore.CYAN}Tags:{Style.RESET_ALL} {stats['tags']}")
            self._print(f"  {Fore.CYAN}Threads:{Style.RESET_ALL} {stats['threads']}")
            self._print(f"  {Fore.CYAN}Replies:{Style.RESET_ALL} {stats['replies']}")
            
            self._print(f"\n{Fore.MAGENTA}⏱️  Time Taken:{Style.RESET_ALL} {duration:.2f}s")
            self._print(f"{Fore.MAGENTA}📧 Emails Intercepted:{Style.RESET_ALL} {len(mail.outbox)} (None sent)\n")
        else:
            self._print("\n" + "="*80)
            self._print("✓ POPULATION COMPLETED".center(80))
            self._print("="*80)
            self._print(f"\n📊 Summary: Users={stats['users']}, Threads={stats['threads']}, "
                       f"Replies={stats['replies']}")
            self._print(f"⏱️  Time: {duration:.2f}s")
            self._print(f"📧 Emails Intercepted: {len(mail.outbox)}\n")

    def handle(self, *args, **options):
        # Override configuration from arguments
        num_users = options['users']
        total_threads = options['threads']
        workers = options['workers']
        
        # CRITICAL SAFETY OVERRIDE
        settings.EMAIL_BACKEND = 'django.core.mail.backends.locmem.EmailBackend'
        
        self.print_header()
        start_time = time.time()
        
        # 1. SETUP FOUNDATION
        if HAS_FANCY_OUTPUT:
            self._print(f"{Fore.BLUE}{Style.BRIGHT}[1/3] Setting up Users and Metadata...{Style.RESET_ALL}")
        else:
            self._print("[1/3] Setting up Users and Metadata...")
        
        users = self.setup_users(num_users)
        courses, categories, tags = self.setup_metadata()
        
        if HAS_FANCY_OUTPUT:
            self._print(f"  {Fore.GREEN}✓ Created {len(users)} users, {len(courses)} courses, "
                       f"{len(categories)} categories, {len(tags)} tags{Style.RESET_ALL}\n")
        else:
            self._print(f"  ✓ Setup complete\n")

        # Prepare Data for Workers
        user_ids = [u.id for u in users]
        course_map = {c.id: c.code for c in courses}
        cat_ids = [c.id for c in categories]
        tag_ids = [t.id for t in tags]

        # 2. PARALLEL CONTENT GENERATION
        if HAS_FANCY_OUTPUT:
            self._print(f"{Fore.MAGENTA}{Style.BRIGHT}[2/3] Launching {workers} workers for {total_threads} threads...{Style.RESET_ALL}\n")
        else:
            self._print(f"[2/3] Generating {total_threads} threads with {workers} workers...\n")
        
        chunk_size = total_threads // workers
        remainder = total_threads % workers
        tasks = [chunk_size + (1 if i < remainder else 0) for i in range(workers)]

        total_done = 0
        
        if HAS_FANCY_OUTPUT and tqdm:
            # Beautiful progress bar
            with tqdm(total=total_threads, 
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                     colour='green') as pbar:
                
                with ProcessPoolExecutor(max_workers=workers) as executor:
                    futures = [
                        executor.submit(create_content_worker, t, user_ids, course_map, cat_ids, tag_ids)
                        for t in tasks
                    ]
                    
                    for future in as_completed(futures):
                        try:
                            res = future.result()
                            total_done += res
                            pbar.update(res)
                            pbar.set_description(f"{Fore.GREEN}Threads Created{Style.RESET_ALL}")
                        except Exception as e:
                            self._print(f"  {Fore.RED}✗ Worker Error: {e}{Style.RESET_ALL}")
        else:
            # Fallback without progress bar
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = [
                    executor.submit(create_content_worker, t, user_ids, course_map, cat_ids, tag_ids)
                    for t in tasks
                ]
                
                for i, future in enumerate(as_completed(futures), 1):
                    try:
                        res = future.result()
                        total_done += res
                        self._print(f"  [{i}/{workers}] Batch completed: {res} threads")
                    except Exception as e:
                        self._print(f"  ✗ Worker Error: {e}")

        # 3. FINAL STATISTICS
        if HAS_FANCY_OUTPUT:
            self._print(f"\n{Fore.YELLOW}{Style.BRIGHT}[3/3] Collecting Statistics...{Style.RESET_ALL}")
        else:
            self._print("\n[3/3] Collecting Statistics...")
        
        stats = {
            'users': User.objects.count(),
            'departments': Department.objects.count(),
            'courses': Course.objects.count(),
            'resources': Resource.objects.count(),
            'categories': Category.objects.count(),
            'tags': Tag.objects.count(),
            'threads': Thread.objects.count(),
            'replies': Reply.objects.count(),
        }
        
        duration = time.time() - start_time
        self.print_summary(stats, duration)
        
        self._print(self.style.SUCCESS('✓ Forum population completed successfully!\n'))

    @transaction.atomic
    def setup_users(self, num_users):
        """Create users with progress indication"""
        fake = Faker('en_IN')
        users = []
        
        # Create moderator
        if not User.objects.filter(username='moderator').exists():
            User.objects.create_superuser(
                'moderator',
                'mod@pilani.bits-pilani.ac.in',
                'password123'
            )

        needed = num_users - User.objects.count()
        if needed > 0:
            if HAS_FANCY_OUTPUT and tqdm:
                iterator = tqdm(range(needed), desc=f"{Fore.CYAN}Creating Users{Style.RESET_ALL}", colour='cyan')
            else:
                iterator = range(needed)
            
            for _ in iterator:
                year = random.choice(['2022', '2023', '2024', '2025'])
                uid = f"f{year}{random.randint(100, 9999)}"
                if not User.objects.filter(username=uid).exists():
                    try:
                        u = User.objects.create_user(
                            username=uid,
                            email=f"{uid}@pilani.bits-pilani.ac.in",
                            password='password123',
                            full_name=fake.name()
                        )
                        users.append(u)
                    except IntegrityError:
                        pass
        
        return list(User.objects.all())

    @transaction.atomic
    def setup_metadata(self):
        """Setup departments, courses, categories, tags"""
        fake = Faker('en_IN')
        
        # Departments
        dept_objs = {}
        for name in DEPARTMENTS:
            d, _ = Department.objects.get_or_create(name=name)
            dept_objs[name] = d

        # Courses
        courses = []
        for d_code, num, title in BITS_COURSES:
            full_code = f"{d_code} {num}"
            # Find matching department
            relevant_dept = next(
                (d for n, d in dept_objs.items() if d_code in n or title.split()[0] in n),
                list(dept_objs.values())[0]
            )
            
            c, created = Course.objects.get_or_create(
                code=full_code,
                defaults={'title': title, 'department': relevant_dept}
            )
            courses.append(c)
            
            # Add sample resource
            if created or not c.resources.exists():
                Resource.objects.get_or_create(
                    course=c,
                    title=f"{full_code} Handout",
                    defaults={'type': "PDF", 'link': fake.url()}
                )

        # Categories & Tags
        cats = [Category.objects.get_or_create(name=n)[0] for n in CATEGORIES]
        tags = [Tag.objects.get_or_create(name=n, defaults={'color': c})[0] for n, c in TAG_DATA]

        return courses, cats, tags
