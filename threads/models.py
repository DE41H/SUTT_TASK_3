import bleach
import markdown
from django.db import models, transaction
from django.conf import settings
from django.core import validators
from django.utils import text

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
        regex=r'^#[A-Fa-f0-9]{6}$',
        message='Enter a valid hex color code'
    )

    name = models.CharField(verbose_name='name', max_length=255, unique=True)
    color = models.CharField(verbose_name='color', max_length=7, validators=[hex_validator])

    def __str__(self) -> str:
        return f'Tag Name: {self.name}\nTag Color: {self.color}'


class Post(models.Model):

    class Meta:
        abstract = True

    upvotes = models.ManyToManyField(verbose_name='upvotes', to=settings.AUTH_USER_MODEL, blank=True, related_name='upvoted_%(class)s')
    upvote_count = models.PositiveIntegerField(verbose_name='upvote count', default=0)
    raw_content = models.TextField(verbose_name='raw_content')
    markdown_content = models.TextField(verbose_name='markdown_content', editable=False, blank=True)
    author = models.ForeignKey(verbose_name='author', to=settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='%(class)s')
    created_at = models.DateTimeField(verbose_name='created at', auto_now_add=True)
    is_deleted = models.BooleanField(verbose_name='is deleted', default=False)

    @property
    def content(self) -> str:
        if self.is_deleted:
            return '[This content has been removed]'
        else:
            return str(self.markdown_content)

    def update_upvotes(self, user) -> None:
        with transaction.atomic():
            if self.upvotes.filter(id=user.id).exists():
                self.upvotes.remove(user)
                amount = -1
            else:
                self.upvotes.add(user)
                amount = 1
            self.__class__.objects.filter(pk=self.pk).update(upvote_count=models.F('upvote_count') + amount)

    def soft_delete(self):
        if not self.is_deleted:
            self.is_deleted = True
            self.save(update_fields=['is_deleted'])

    def save(self, *args, **kwargs) -> None:
        markdown_content = markdown.markdown(text=self.raw_content, extensions=['extra'])
        allowed_tags = ['p', 'b', 'i', 'strong', 'em', 'ul', 'ol', 'li', 'code', 'pre']
        self.markdown_content = bleach.clean(text=markdown_content, tags=allowed_tags)
        return super().save(*args, **kwargs)


class Trigram(models.Model):
    
    class Meta:
        verbose_name = 'Trigram'
        verbose_name_plural = 'Trigrams'
    
    value = models.CharField(verbose_name='value', max_length=3, db_index=True, unique=True)


class Thread(Post):

    class Meta(Post.Meta):
        abstract = False
        verbose_name = 'Thread'
        verbose_name_plural = 'Threads'
        ordering = ['-created_at']
    
    category = models.ForeignKey(verbose_name='category', to='threads.Category', on_delete=models.CASCADE, related_name='threads')
    title = models.CharField(verbose_name='title', max_length=255)
    trigrams = models.ManyToManyField(verbose_name='trigrams', to='threads.Trigram', blank=True, related_name='threads')
    tagged_courses = models.ManyToManyField(verbose_name='tagged courses', to='courses.Course', blank=True, related_name='tagged')
    tagged_documents = models.ManyToManyField(verbose_name='tagged documents', to='courses.Resource', blank=True, related_name='tagged')
    tags = models.ManyToManyField(verbose_name='tags', to='threads.Tag', blank=True, related_name='tagged')
    is_locked = models.BooleanField(verbose_name='is locked', default=False)
    reply_count = models.PositiveIntegerField(verbose_name='reply_count', default=0)

    def _save_trigrams(self) -> None:
        with transaction.atomic():
            title = f'  {self.title.lower()}  '
            values: set[str] = set([title[i:i+3] for i in range(len(title) - 2)])
            old: set[str] = set(Trigram.objects.filter(value__in=values).values_list('value', flat=True))
            new = values - old
            Trigram.objects.bulk_create([Trigram(value=value) for value in new])
            trigrams = Trigram.objects.filter(value__in=values)
            self.trigrams.set(trigrams)

    def save(self, *args, **kwargs) -> None:
        pk = self.pk
        super().save(*args, **kwargs)
        if pk is None:
            self._save_trigrams()

    def __str__(self) -> str:
        return f'Thread Title: {self.title}\nAuthor: {self.author}\nContent: {self.content}'


class Reply(Post):

    class Meta(Post.Meta):
        abstract = False
        verbose_name = 'Reply'
        verbose_name_plural = 'Replies'
        ordering = ['thread', '-created_at']

    thread = models.ForeignKey(verbose_name='thread', to='threads.Thread', on_delete=models.CASCADE, related_name='replies')

    def soft_delete(self) -> None:
        with transaction.atomic():
            if not self.is_deleted:
                self.is_deleted = True
                self.save(update_fields=['is_deleted'])
                Thread.objects.filter(pk=self.thread.pk).update(reply_count=models.F('reply_count') - 1)

    def save(self, *args, **kwargs) -> None:
        pk = self.pk
        markdown_content = markdown.markdown(text=self.raw_content, extensions=['extra'])
        allowed_tags = ['p', 'b', 'i', 'strong', 'em', 'ul', 'ol', 'li', 'code', 'pre']
        self.markdown_content = bleach.clean(text=markdown_content, tags=allowed_tags)
        with transaction.atomic():
            super().save(*args, **kwargs)
            if pk is None:
                Thread.objects.filter(pk=self.thread.pk).update(reply_count=models.F('reply_count') + 1)

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
    status = models.CharField(verbose_name='status', choices=StatusChoices.choices, max_length=8, default=StatusChoices.PENDING)
    created_at = models.DateTimeField(verbose_name='created_at', auto_now_add=True)

    def __str__(self) -> str:
        return f'Report By: {self.reporter}\nReport On: {self.thread if self.thread else self.reply}\nReason: {self.reason}\nStatus: {self.status}'
