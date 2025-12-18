from django.db import models
from django.conf import settings
from django.core import exceptions, validators
from django.utils import timezone, text

# Create your models here.

class Category(models.Model):

    class Meta:
        verbose_name = 'Category'
        verbose_name_plural = 'Categories'

    name = models.CharField(verbose_name='name', max_length=255, unique=True)
    slug = models.SlugField(unique=True, blank=True, editable=False)

    def save(self, *args, **kwargs):
        if not self.slug:
            slug = text.slugify(self.name)
            count = 1
            while Category.objects.filter(slug=f'{slug}-{count}').exists():
                count += 1
            self.slug = f'{slug}-{count}'
        super().save(*args, **kwargs)
        
    def __str__(self) -> str:
        return f'Category Name: {self.name}'


class Tag(models.Model):
    
    class Meta:
        verbose_name = 'Tag'
        verbose_name_plural = 'Tags'

    hex_validator = validators.RegexValidator(
        regex=r'^#[A-Za-z0-9]{6}$',
        message='Enter a valid hex color code'
    )

    name = models.CharField(verbose_name='name', max_length=255, unique=True)
    color = models.CharField(verbose_name='color', max_length=7, validators=[hex_validator])

    def __str__(self) -> str:
        return f'Tag Name: {self.name}\nTag Color: {self.color}'
    

class ActiveManager(models.Manager):

    def get_queryset(self) -> models.QuerySet:
        return super().get_queryset().filter(is_deleted=False)


class Post(models.Model):

    class Meta:
        abstract = True

    objects = models.Manager()
    active = ActiveManager()

    upvotes = models.ManyToManyField(verbose_name='upvotes', to=settings.AUTH_USER_MODEL, blank=True, related_name='upvoted_%(class)s')
    upvote_count = models.PositiveIntegerField(verbose_name='upvote count', default=0, db_index=True)
    raw_content = models.TextField(verbose_name='raw content')
    author = models.ForeignKey(verbose_name='author', to=settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='%(class)s')
    created_at = models.DateTimeField(verbose_name='created at', default=timezone.now, db_index=True)
    is_deleted = models.BooleanField(verbose_name='is deleted', default=False, db_index=True)

    @property
    def content(self):
        if self.is_deleted:
            return '[This content has been removed by a moderator]'
        else:
            return str(self.raw_content)

    def update_upvotes(self, user) -> None:
        if self.upvotes.filter(id=user.id).exists():
            self.upvotes.remove(user)
            self.upvote_count = models.F('upvote_count') - 1
        else:
            self.upvotes.add(user)
            self.upvote_count = models.F('upvote_count') + 1
        self.save(update_fields=['upvote_count'])


class Thread(Post):

    class Meta(Post.Meta):
        abstract = False
        verbose_name = 'Thread'
        verbose_name_plural = 'Threads'
        ordering = ['-created_at']
        indexes = [models.Index(fields=['category', '-created_at'])]
    
    category = models.ForeignKey(verbose_name='category', to='threads.Category', on_delete=models.CASCADE, related_name='threads')
    title = models.CharField(verbose_name='title', max_length=255, db_index=True)
    tagged_courses = models.ManyToManyField(verbose_name='tagged courses', to='courses.Course', blank=True, related_name='tagged')
    tagged_documents = models.ManyToManyField(verbose_name='tagged documents', to='courses.Resource', blank=True, related_name='tagged')
    tags = models.ManyToManyField(verbose_name='tags', to='threads.Tag', blank=True, related_name='tagged')
    is_locked = models.BooleanField(verbose_name='is locked', default=False, db_index=True)
    reply_count = models.PositiveIntegerField(verbose_name='reply_count', default=0)

    def __str__(self) -> str:
        return f'Thread Title: {self.title}\nAuthor: {self.author}\nContent: {self.content}'


class Reply(Post):

    class Meta(Post.Meta):
        abstract = False
        verbose_name = 'Reply'
        verbose_name_plural = 'Replies'
        ordering = ['thread', '-created_at']

    thread = models.ForeignKey(verbose_name='thread', to='threads.Thread', on_delete=models.CASCADE, related_name='replies')

    def save(self, *args, **kwargs) -> None:
        self.thread.reply_count = models.F('reply_count') + 1
        self.thread.save(update_fields=['reply_count'])
        return super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f'Reply to: {self.thread}\nAuthor: {self.author}\nContent: {self.content}'


class Report(models.Model):

    class Meta:
        verbose_name = 'Report'
        verbose_name_plural = 'Reports'

    class StatusChoices(models.TextChoices):
        PENDING = 'PENDING', 'Pending'
        RESOLVED = 'RESOLVED', 'Resolved'

    reporter = models.ForeignKey(verbose_name='reporter', to=settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='reports')
    thread = models.ForeignKey(verbose_name='thread', to='threads.Thread', on_delete=models.CASCADE, related_name='reports', blank=True, null=True)
    reply = models.ForeignKey(verbose_name='reply', to='threads.Reply', on_delete=models.CASCADE, related_name='reports', blank=True, null=True)
    reason = models.TextField(verbose_name='reason')
    status = models.CharField(verbose_name='status', choices=StatusChoices.choices, max_length=8, default=StatusChoices.PENDING, db_index=True)
    
    def clean(self) -> None:
        if not self.thread and not self.reply:
            raise exceptions.ValidationError('A report must be linked to either a Reply or Thread')
        elif self.thread and self.reply:
            raise exceptions.ValidationError('A report cannot be linked to both a Reply and a Thread at the same time')

    def __str__(self) -> str:
        return f'Report By: {self.reporter}\nReport On: {self.thread if self.thread else self.reply}\nReason: {self.reason}\nStatus: {self.status}'
