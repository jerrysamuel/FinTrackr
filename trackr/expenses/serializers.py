
from rest_framework import serializers
from .models import Category, CategoryRule, Expense, Budget
from django.utils import timezone
from django.db.models import Sum

class CategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = Category
        fields = ['id', 'name', 'color', 'is_default', 'created_at']
        read_only_fields = ['is_default', 'created_at']


class CategoryRuleSerializer(serializers.ModelSerializer):
    category_name = serializers.CharField(source='category.name', read_only=True)
    
    class Meta:
        model = CategoryRule
        fields = ['id', 'category', 'category_name', 'description_keyword', 'priority', 'created_at']
        read_only_fields = ['created_at']


class ExpenseSerializer(serializers.ModelSerializer):
    category_name = serializers.CharField(source='category.name', read_only=True)
    
    class Meta:
        model = Expense
        fields = [
            'id', 'date', 'amount', 'transaction_type', 'description', 
            'category', 'category_name', 'notes', 'created_at', 'updated_at'
        ]
        read_only_fields = ['created_at', 'updated_at']


class BudgetSerializer(serializers.ModelSerializer):
    category_name = serializers.CharField(source='category.name', read_only=True)
    spent = serializers.SerializerMethodField()
    remaining = serializers.SerializerMethodField()
    is_over_budget = serializers.SerializerMethodField()
    
    class Meta:
        model = Budget
        fields = [
            'id', 'category', 'category_name', 'amount', 'month', 
            'spent', 'remaining', 'is_over_budget', 'created_at'
        ]
        read_only_fields = ['created_at']
    
    def get_spent(self, obj):
        month_start = obj.month
        month_end = (month_start.replace(day=28) + timezone.timedelta(days=4)).replace(day=1)
        
        spent = Expense.objects.filter(
            user=obj.user,
            category=obj.category,
            transaction_type='DEBIT',
            date__gte=month_start,
            date__lt=month_end
        ).aggregate(total=Sum('amount'))['total'] or 0
        
        return float(spent)
    
    def get_remaining(self, obj):
        spent = self.get_spent(obj)
        return float(obj.amount) - spent
    
    def get_is_over_budget(self, obj):
        return self.get_spent(obj) > float(obj.amount)