from django.db import models
from django.conf import settings
from django.utils import timezone

# Create your models here.

class Thread(models.Model):

    class Meta:
        verbose_name = 'Thread'
        verbose_name_plural = 'Threads'

    category = models.ForeignKey(verbose_name='category', to='threads.Category', on_delete=models.CASCADE)
    title = models.CharField(verbose_name='title')
    content = models.CharField(verbose_name='content')
    author = models.ForeignKey(verbose_name='author', to=settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='threads')
    created_at = models.DateTimeField(verbose_name='created at', default=timezone.now)
    tagged_courses = models.ManyToManyField(verbose_name='tagged courses', to='courses.Course')
    tagged_documents = models.ManyToManyField(verbose_name='tagged documents', to='courses.Resource')


class Category(models.Model):

    class Meta:
        verbose_name = 'Category'
        verbose_name_plural = 'Categories'

    name = models.CharField(verbose_name='name')


class Reply(models.Model):

    class Meta:
        verbose_name = 'Reply'
        verbose_name_plural = 'Replies'

    author = models.ForeignKey(verbose_name='author', to=settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='replies')
    content = models.CharField(verbose_name='content')
    created_at = models.DateTimeField(verbose_name='created at', default=timezone.now)
