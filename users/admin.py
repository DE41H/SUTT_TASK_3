from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from users.models import User

# Register your models here.

@admin.register(User)
class CustomUserAdmin(UserAdmin):
    fieldsets = UserAdmin.fieldsets + (
        ('Metadata', {'fields': ('full_name', 'avatar')}),
    ) # type: ignore
    add_fieldsets = UserAdmin.add_fieldsets + (
        ('Metadata', {'fields': ('full_name', 'avatar')}),
    )
