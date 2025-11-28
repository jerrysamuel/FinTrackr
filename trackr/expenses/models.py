from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from decimal import Decimal



class Category(models.Model):
    """Expense/Income categories"""
    name = models.CharField(max_length=100)
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE, 
        null=True, 
        blank=True,
        help_text="Null for default categories, set for user-custom"
        
    )
    is_default = models.BooleanField(default=False)
    color = models.CharField(max_length=7, default="#6B7280")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name_plural = "Categories"
        unique_together = ['name', 'user']
        ordering = ['name']

    def __str__(self):
        return self.name


class CategoryRule(models.Model):
    """Smart category assignment rules"""
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)
    description_keyword = models.CharField(max_length=255)
    priority = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-priority', '-created_at']
        unique_together = ['user', 'description_keyword']

    def __str__(self):
        return f"{self.description_keyword} → {self.category.name}"

    def matches(self, description):
        """Check if rule matches transaction description"""
        return self.description_keyword.lower() in description.lower()


class Expense(models.Model):
    """Individual transactions (debits and credits)"""
    TRANSACTION_TYPES = [
        ('DEBIT', 'Debit'),
        ('CREDIT', 'Credit'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='expenses')
    date = models.DateField()
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    transaction_type = models.CharField(max_length=6, choices=TRANSACTION_TYPES)
    description = models.CharField(max_length=500)
    category = models.ForeignKey(
        Category, 
        on_delete=models.SET_NULL, 
        null=True,
        blank=True,
        related_name='expenses'
    )
    notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['-date', '-created_at']
        indexes = [
            models.Index(fields=['user', 'date']),
            models.Index(fields=['user', 'category']),
            models.Index(fields=['user', 'transaction_type']),
        ]

    def __str__(self):
        return f"{self.date} - {self.description}: ${self.amount}"

    def save(self, *args, **kwargs):
        """Auto-apply category rules if no category set"""
        if not self.category and self.user:
            rules = CategoryRule.objects.filter(user=self.user)
            for rule in rules:
                if rule.matches(self.description):
                    self.category = rule.category
                    break
        super().save(*args, **kwargs)


class Budget(models.Model):
    """Monthly budget limits per category"""
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    month = models.DateField(help_text="First day of the month")
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ['user', 'category', 'month']
        ordering = ['-month']

    def __str__(self):
        return f"{self.category.name} - {self.month.strftime('%B %Y')}: ${self.amount}"

