from django.contrib import admin
from django.utils.html import format_html
from courses.models import Department, Course, Resource

# Register your models here.

@admin.register(Department)
class DepartmentAdmin(admin.ModelAdmin):
    list_display = ('name', )
    search_fields = ('name', )

@admin.register(Course)
class CourseAdmin(admin.ModelAdmin):

    class ResourceInline(admin.TabularInline):
        model = Resource
        extra = 0
        fields = ('title', 'type', 'link', 'link_display')
        readonly_fields = ('link_display', )
        ordering = ('type', 'title')
        classes = ['collapse']

        def link_display(self, obj) -> str:
            if obj.link:
                link = f'<a href="{obj.link}" target="_blank">Open Link</a>'
                return format_html(link)
            return 'No Link'

    list_select_related = ('department', )
    list_display = ('code', 'title', 'department')
    list_filter = ('department', )
    inlines = [ResourceInline]
    search_fields = ('code', 'title', 'department__name')
