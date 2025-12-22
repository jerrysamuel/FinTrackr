from django.contrib import admin
from .models import Category, CategoryRule, Expense

admin.site.register(Category)
admin.site.register(CategoryRule)
admin.site.register(Expense)        

# Register your models here.
