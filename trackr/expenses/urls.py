
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'expenses', views.ExpenseViewSet, basename='expense')
router.register(r'categories', views.CategoryViewSet, basename='category')
router.register(r'category-rules', views.CategoryRuleViewSet, basename='categoryrule')
router.register(r'budgets', views.BudgetViewSet, basename='budget')

urlpatterns = [
    path('', include(router.urls)),
    path('analytics/summary/', views.analytics_summary, name='analytics-summary'),
    path('analytics/by-category/', views.analytics_by_category, name='analytics-by-category'),
    path('analytics/by-month/', views.analytics_by_month, name='analytics-by-month'),
]