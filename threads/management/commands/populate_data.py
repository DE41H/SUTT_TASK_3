import random
import time
import os
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

User = get_user_model()

# --- CONFIGURATION ---
# Adjust these numbers based on your needs
NUM_USERS = 100          # Total users to create
TOTAL_THREADS = 2000     # Total threads to generate
MAX_REPLIES = 8          # Max replies per thread
MAX_UPVOTES = 20         # Max upvotes per item
WORKERS = os.cpu_count() # Use all CPU cores

# --- BITS PILANI CONTEXT DATA ---
DEPARTMENTS = [
    "Computer Science", "Electrical & Electronics", "Instrumentation", 
    "Mechanical", "Chemical", "Biological Sciences", "Physics", 
    "Mathematics", "Humanities", "Economics", "Management", "Pharmacy"
]

BITS_COURSES = [
    ("CS", "F111", "Computer Programming"), ("CS", "F211", "Data Structures & Algorithms"),
    ("CS", "F212", "Database Systems"), ("CS", "F213", "OOP"), ("CS", "F301", "OS"),
    ("CS", "F303", "Computer Networks"), ("CS", "F372", "AI"), ("EEE", "F111", "Electrical Sciences"), 
    ("EEE", "F211", "Electrical Machines"), ("MATH", "F111", "Mathematics I"), 
    ("MATH", "F113", "Probability & Statistics"), ("PHY", "F111", "Mech Osc & Waves"), 
    ("BIO", "F111", "General Biology"), ("ECON", "F211", "Principles of Economics"),
    ("GS", "F221", "Business Communication"), ("HSS", "F221", "Intro to Psychology")
]

CATEGORIES = [
    "General Queries", "Exam Prep", "Resource Sharing", 
    "Professor Reviews", "Lost & Found", "Internships & PS", 
    "Campus Life"
]

TAG_DATA = [
    ("#midsem", "#FF5733"), ("#compre", "#C70039"), ("#urgent", "#FFC300"),
    ("#resources", "#DAF7A6"), ("#quiz", "#33FF57"), ("#ps1", "#3371FF"),
    ("#grading", "#2C3E50"), ("#attendance", "#E74C3C"), ("#lite", "#1ABC9C")
]

TEMPLATES = [
    ("Has the {code} handout been released?", "academics"),
    ("How is the grading for {code} with {prof}?", "review"),
    ("Anyone have previous year midsem papers for {code}?", "resource"),
    ("Is attendance compulsory for {code} lectures?", "academics"),
    ("Found a {item} in {loc}. DM to claim.", "lost_found"),
    ("Opinion on taking {code} and {code2} together?", "advice"),
    ("Review of {prof} for {code}? Is he lite?", "review"),
]

LOCATIONS = ["NAB", "FD-1", "FD-2", "FD-3", "Library", "Rotunda", "Sky Lawns"]
ITEMS = ["ID Card", "Calculator", "Water Bottle", "Umbrella", "Charger"]

def create_content_worker(count, user_ids, course_map, cat_ids, tag_ids):
    """
    Worker process to generate data in parallel.
    Includes SAFETY LOCKS to ensure no emails are sent.
    """
    # 1. SAFETY: Override Settings in this process
    settings.EMAIL_BACKEND = 'django.core.mail.backends.locmem.EmailBackend'
    
    # 2. SAFETY: Patch Email functions
    p1 = patch('threads.utils.queue_mail')
    p2 = patch('threads.utils.queue_mass_mail')
    p1.start()
    p2.start()

    # 3. Reset DB Connection
    connections.close_all()
    
    fake = Faker('en_IN')
    created = 0
    course_ids = list(course_map.keys())

    for _ in range(count):
        # Retry logic for Database Locking (Stability)
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
                        code=code1, code2=code2, prof=fake.last_name(),
                        loc=random.choice(LOCATIONS), item=random.choice(ITEMS)
                    )

                    content = f"{fake.paragraph(nb_sentences=4)}\n\nRef: **{code1}**"

                    # --- CREATE THREAD ---
                    # Calling .create() ensures your custom save() logic runs (Trigrams, etc.)
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
                    # Using update_upvotes to test locking logic
                    num_votes = random.randint(0, MAX_UPVOTES)
                    if num_votes > 0:
                        voters = random.sample(user_ids, k=min(num_votes, len(user_ids)))
                        user_objs = User.objects.filter(id__in=voters)
                        for u in user_objs:
                            thread.update_upvotes(u)

                    # --- REPLIES ---
                    # Using create() to ensure Reply count updates
                    for _ in range(random.randint(0, MAX_REPLIES)):
                        r_content = fake.sentence()
                        if random.random() < 0.3:
                            r_content = f"@{thread.author.username} {r_content}"
                        
                        Reply.objects.create(
                            thread=thread,
                            author_id=random.choice(user_ids),
                            raw_content=r_content
                        )

                created += 1
                break # Success

            except OperationalError:
                time.sleep(retry_delay)
                retry_delay *= 1.5 # Exponential backoff
            except Exception:
                break # Skip bad data

    p1.stop()
    p2.stop()
    return created

class Command(BaseCommand):
    help = 'Populates the DB with safe, stable, authentic BITS data.'

    def handle(self, *args, **options):
        # --- CRITICAL SAFETY OVERRIDE ---
        # This guarantees emails are dumped into RAM, not sent to the internet.
        settings.EMAIL_BACKEND = 'django.core.mail.backends.locmem.EmailBackend'
        
        self.stdout.write(self.style.WARNING("--- SAFETY MODE ACTIVE: EMAILS DISABLED ---"))
        
        start_time = time.time()
        
        # 1. SETUP FOUNDATION (Sequential)
        self.stdout.write("1. Setting up Users and Metadata...")
        users = self.setup_users()
        courses, categories, tags = self.setup_metadata()

        # Prepare Data for Workers
        user_ids = [u.id for u in users] # type: ignore
        course_map = {c.id: c.code for c in courses}
        cat_ids = [c.id for c in categories] # type: ignore
        tag_ids = [t.id for t in tags] # type: ignore

        # 2. PARALLEL CONTENT GENERATION
        self.stdout.write(f"2. Launching {WORKERS} workers for {TOTAL_THREADS} threads...")
        
        chunk_size = TOTAL_THREADS // WORKERS # type: ignore
        remainder = TOTAL_THREADS % WORKERS # type: ignore
        tasks = [chunk_size + (1 if i < remainder else 0) for i in range(WORKERS)] # type: ignore

        total_done = 0
        with ProcessPoolExecutor(max_workers=WORKERS) as executor:
            futures = [
                executor.submit(create_content_worker, t, user_ids, course_map, cat_ids, tag_ids)
                for t in tasks
            ]
            
            for future in as_completed(futures):
                try:
                    res = future.result()
                    total_done += res
                    self.stdout.write(f"   - Batch finished: {res} threads created.")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"   - Worker Error: {e}"))

        duration = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(f"\n--- DONE! Created {total_done} Threads in {duration:.2f}s ---"))
        self.stdout.write(f"Emails intercepted in memory: {len(mail.outbox)} (None sent to network)")

    @transaction.atomic
    def setup_users(self):
        fake = Faker('en_IN')
        users = []
        
        if not User.objects.filter(username='moderator').exists():
            User.objects.create_superuser('moderator', 'mod@pilani.bits-pilani.ac.in', 'password123')

        needed = NUM_USERS - User.objects.count()
        if needed > 0:
            for _ in range(needed):
                year = random.choice(['2022', '2023', '2024', '2025'])
                uid = f"f{year}{random.randint(100, 9999)}"
                if not User.objects.filter(username=uid).exists():
                    try:
                        u = User.objects.create_user(
                            username=uid, email=f"{uid}@pilani.bits-pilani.ac.in",
                            password='password123', full_name=fake.name()
                        )
                        users.append(u)
                    except IntegrityError: pass
        
        return list(User.objects.all())

    @transaction.atomic
    def setup_metadata(self):
        fake = Faker('en_IN')
        
        # Depts
        dept_objs = {}
        for name in DEPARTMENTS:
            d, _ = Department.objects.get_or_create(name=name)
            dept_objs[name] = d

        # Courses
        courses = []
        for d_code, num, title in BITS_COURSES:
            full_code = f"{d_code} {num}"
            # Find closest dept or default
            relevant_dept = next((d for n, d in dept_objs.items() if d_code in n), list(dept_objs.values())[0])
            
            c, created = Course.objects.get_or_create(
                code=full_code, defaults={'title': title, 'department': relevant_dept}
            )
            courses.append(c)
            
            if created or not c.resources.exists(): # type: ignore
                Resource.objects.create(course=c, title=f"{full_code} Handout", type="PDF", link=fake.url())

        # Cats & Tags
        cats = [Category.objects.get_or_create(name=n)[0] for n in CATEGORIES]
        tags = [Tag.objects.get_or_create(name=n, defaults={'color': c})[0] for n, c in TAG_DATA]

        return courses, cats, tags