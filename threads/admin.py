from django.contrib import admin
from threads.models import Category, Tag, Thread, Reply, Report

# Register your models here.

@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = ('name', 'slug')
    readonly_fields = ('slug', )


@admin.register(Tag)
class TagAdmin(admin.ModelAdmin):
    list_display = ('name', 'color')


@admin.register(Thread)
class ThreadAdmin(admin.ModelAdmin):

    class ReplyInline(admin.TabularInline):
        model = Reply
        extra = 0
        fields = ('author', 'raw_content', 'upvote_count', 'is_deleted')
        readonly_fields = ('author', 'raw_content', 'upvote_count')
        classes = ['collapse']

    list_select_related = ('author', 'category')
    list_display = ('title', 'author', 'raw_content', 'category', 'reply_count', 'upvote_count', 'is_locked', 'is_deleted')
    readonly_fields = ('title', 'author', 'raw_content', 'reply_count', 'upvote_count')
    list_filter = ('category', 'is_locked', 'is_deleted', 'created_at')
    list_editable = ('is_locked', 'is_deleted')
    search_fields = ('title', 'author__username', 'raw_content')
    inlines = [ReplyInline]
    actions = ['soft_delete_threads', 'lock_threads']
    
    @admin.action(description='Soft delete selected Threads')
    def soft_delete_threads(self, request, queryset) -> None:
        for thread in queryset:
            thread.soft_delete()
    
    @admin.action(description='Lock selected Threads')
    def lock_threads(self, request, queryset) -> None:
        queryset.update(is_locked=True)


@admin.register(Report)
class ReportAdmin(admin.ModelAdmin):
    list_select_related = ('reporter',)
    list_display = ('reporter', 'target', 'reason', 'status')
    readonly_fields = ('reporter', 'target', 'reason')
    list_filter = ('status', )
    list_editable = ('status', )
    search_fields = ('reporter__username', 'reason')

    def target(self, obj):
        if obj.thread:
            return f'Thread titled {obj.thread.title} by {obj.thread.author}'
        else:
            return f'Reply by {obj.reply.author}'
