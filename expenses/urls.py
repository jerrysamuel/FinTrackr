
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'expenses', views.ExpenseViewSet, basename='expense')
router.register(r'categories', views.CategoryViewSet, basename='category')
router.register(r'category-rules', views.CategoryRuleViewSet, basename='categoryrule')
router.register(r'budgets', views.BudgetViewSet, basename='budget')
router.register(r'analytics', views.AnalyticsViewSet, basename='analytics')


urlpatterns = [
    path('', include(router.urls)),
]