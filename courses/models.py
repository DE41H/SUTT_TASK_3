from django.db import models

# Create your models here.

class Department(models.Model):

    class Meta:
        verbose_name = 'Department'
        verbose_name_plural = 'Departments'

    name = models.CharField(verbose_name='name')


class Course(models.Model):

    class Meta:
        verbose_name = 'Course'
        verbose_name_plural = 'Courses'

    code = models.CharField(verbose_name='code')
    title = models.CharField(verbose_name='title')
    department = models.ForeignKey(to='courses.Department', on_delete=models.CASCADE)


class Resource(models.Model):

    class Meta:
        verbose_name = 'Resource'
        verbose_name_plural = 'Resources'

    class types(models.TextChoices):
        PDF = 'PDF', 'PDF'
        VIDEO = 'VIDEO', 'Video'
        LINK = 'LINK', 'Link'

    title = models.CharField(verbose_name='title')
    type = models.CharField(verbose_name='type', choices=types.choices)
    link = models.URLField(verbose_name='link')
