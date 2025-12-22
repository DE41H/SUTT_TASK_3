import random
from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from courses.models import Department, Course, Resource
from threads.models import Category, Tag, Thread, Reply, Report

User = get_user_model()

class Command(BaseCommand):
    help = 'Populates the database with dummy data for the StudyDeck Forum'

    def handle(self, *args, **options):
        self.stdout.write("--- Starting Population Script ---")

        # 1. Create Users (Students and Moderators)
        # Requirement: BITS Email only [cite: 44, 45]
        users = []
        for i in range(5):
            user, _ = User.objects.get_or_create(
                username=f'student_{i}',
                email=f'f2023{i}@pilani.bits-pilani.ac.in',
                defaults={'first_name': f'Student_{i}', 'last_name': 'User'}
            )
            users.append(user)
        
        admin_user, _ = User.objects.get_or_create(
            username='moderator_pro',
            email='admin@pilani.bits-pilani.ac.in',
            defaults={'is_staff': True, 'is_superuser': True}
        )
        users.append(admin_user)

        # 2. Create Departments & Courses [cite: 52, 53, 54, 55]
        cs_dept, _ = Department.objects.get_or_create(name="Computer Science")
        phy_dept, _ = Department.objects.get_or_create(name="Physics")

        courses_data = [
            {"code": "CS F111", "title": "Computer Programming", "dept": cs_dept},
            {"code": "CS F211", "title": "Data Structures and Algorithms", "dept": cs_dept},
            {"code": "PHY F111", "title": "Mecht Oscil & Waves", "dept": phy_dept},
        ]

        courses = []
        for data in courses_data:
            course, _ = Course.objects.get_or_create(
                code=data['code'], 
                defaults={'title': data['title'], 'department': data['dept']}
            )
            courses.append(course)

        # 3. Create Resources [cite: 56, 57, 58, 59]
        for course in courses:
            Resource.objects.get_or_create(
                course=course,
                title=f"{course.code} Handout",
                defaults={
                    'type': "PDF",
                    'link': f"https://studydeck.bits/res/{course.code.replace(' ', '')}"
                }
            )

        # 4. Create Categories & Tags [cite: 69, 70, 87]
        categories = []
        for cat_name in ["General Queries", "Exam Prep", "Resource Sharing"]:
            cat, _ = Category.objects.get_or_create(name=cat_name)
            categories.append(cat)

        tags = []
        tag_data = [("#midsem", "#FF5733"), ("#urgent", "#C70039"), ("#quiz1", "#33FF57")]
        for name, color in tag_data:
            tag, _ = Tag.objects.get_or_create(name=name, defaults={'color': color})
            tags.append(tag)

        # 5. Create Threads [cite: 73, 74, 75, 76]
        for i in range(10):
            thread = Thread.objects.create(
                title=f"How to study for {random.choice(courses).code}?",
                raw_content=f"I am struggling with the modules in week {i}. Any tips?",
                author=random.choice(users),
                category=random.choice(categories)
            )
            thread.tagged_courses.add(random.choice(courses))
            thread.tags.add(random.choice(tags))

            # 6. Create Replies [cite: 79, 80]
            for j in range(random.randint(1, 3)):
                Reply.objects.create(
                    thread=thread,
                    author=random.choice(users),
                    raw_content=f"Check out the resource section for help with topic {j}!"
                )

        self.stdout.write(self.style.SUCCESS("--- Population Complete! ---"))
