from rest_framework import viewsets, status, parsers
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.db.models import Sum, Count, Q
from django.db.models.functions import TruncMonth
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from datetime import date
from drf_spectacular.utils import OpenApiParameter, OpenApiTypes
from django.db.models import Avg
from decimal import Decimal

from .models import Expense, Category, CategoryRule, Budget
from .serializers import (
    ExpenseSerializer, CategorySerializer, 
    CategoryRuleSerializer, BudgetSerializer, AnalyticsSummarySerializer, MonthlyAnalyticsSerializer, CategoryAnalyticsSerializer
)
from .services.etl_service import ETLService
from drf_spectacular.utils import extend_schema
import logging
from decouple import config 

# Get logger for this module
logger = logging.getLogger(__name__)

api_key = config('OPENAI_API_KEY', default='')
# ============================================
# Category Views
# ============================================

class CategoryViewSet(viewsets.ModelViewSet):
    """Category CRUD - Returns default categories + user's custom categories"""
    serializer_class = CategorySerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        return Category.objects.filter(
            Q(is_default=True) | Q(user=self.request.user)
        )
    
    def perform_create(self, serializer):
        serializer.save(user=self.request.user, is_default=False)


class CategoryRuleViewSet(viewsets.ModelViewSet):
    """Category rule CRUD"""
    serializer_class = CategoryRuleSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        return CategoryRule.objects.filter(user=self.request.user)
    
    def perform_create(self, serializer):
        serializer.save(user=self.request.user)


# ============================================
# Expense Views
# ============================================

class ExpenseViewSet(viewsets.ModelViewSet):
    """
    Expense CRUD + File Upload
    
    Filters: ?type=DEBIT&category=1&start_date=2024-01-01&end_date=2024-01-31
    """
    serializer_class = ExpenseSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        queryset = Expense.objects.filter(user=self.request.user)
        
        # Apply optional filters
        if tx_type := self.request.query_params.get('type'):
            queryset = queryset.filter(transaction_type=tx_type)
        if category := self.request.query_params.get('category'):
            queryset = queryset.filter(category_id=category)
        if start := self.request.query_params.get('start_date'):
            queryset = queryset.filter(date__gte=start)
        if end := self.request.query_params.get('end_date'):
            queryset = queryset.filter(date__lte=end)
        
        return queryset
    
    def perform_create(self, serializer):
        serializer.save(user=self.request.user)
    @extend_schema(
    request={
        'multipart/form-data': {
            'type': 'object',
            'properties': {
                'file': {
                    'type': 'string',
                    'format': 'binary',
                    'description': 'CSV or Excel file containing bank statement transactions'
                }
            },
            'required': ['file']
        }
    },
    responses={
        200: {
            'type': 'object',
            'properties': {
                'transactions': {
                    'type': 'array',
                    'items': {'type': 'object'}
                },
                'column_mapping': {'type': 'object'},
                'total_count': {'type': 'integer'},
                'saved': {'type': 'integer'}
            }
        },
        400: {
            'type': 'object',
            'properties': {
                'error': {'type': 'string'}
            }
        }
    },
    description='Upload CSV/Excel file for bulk import. Returns preview of transactions with auto-categorization.',
    summary='Bulk upload bank statement'
)
    @action(detail=False, methods=['post'], parser_classes=[parsers.MultiPartParser, parsers.FormParser])
    def upload(self, request):
        file = request.FILES.get('file')
        if not file:
            return Response(
                {'error': 'No file provided'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            print(f"\n{'='*60}")
            print(f"UPLOAD REQUEST from user: {request.user}")
            print(f"{'='*60}")
            
            # ETL Process
            df, column_mapping = ETLService.extract(file)
            
            # Get Groq API key
            try:
                api_key = config("APIKEY_OPENAI")
                print(f"✓ API key loaded")
            except Exception as e:
                logger.warning(f"Could not load Groq API key: {e}")
                api_key = None
                print(f"✗ No API key")
            
            # Transform with AI categorization
            transactions, unique_categories = ETLService.transform(df, column_mapping, api_key=api_key)
            
            print(f"\n✓ Transformed {len(transactions)} transactions")
            print(f"✓ Found {len(unique_categories)} unique categories: {unique_categories}")
            
            # Apply category rules (BEFORE saving to override AI)
            rules = CategoryRule.objects.filter(user=request.user).select_related('category')
            if rules.exists():
                print(f"\nApplying {rules.count()} category rules...")
                rules_applied = 0
                for transaction in transactions:
                    for rule in rules:
                        if rule.matches(transaction['description']):
                            old_cat = transaction.get('category')
                            transaction['category'] = rule.category.name
                            rules_applied += 1
                            if rules_applied <= 3:  # Show first 3
                                print(f"  '{transaction['description'][:30]}': {old_cat} -> {rule.category.name}")
                            break
                print(f"✓ Applied rules to {rules_applied} transactions")
            
            # Save to database
            saved_count = ETLService.load(transactions, request.user)
            
            print(f"\n{'='*60}")
            print(f"UPLOAD COMPLETE: {saved_count} transactions saved")
            print(f"{'='*60}\n")
            
            return Response({
                'message': f'Successfully imported {saved_count} transactions',
                'total_count': len(transactions),
                'saved': saved_count,
                'unique_categories': sorted(list(unique_categories)),
                'column_mapping': column_mapping
            })
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"Upload failed:\n{error_trace}")
            print(f"\n{'!'*60}")
            print(f"UPLOAD FAILED: {str(e)}")
            print(f"{'!'*60}\n")
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        
    @action(detail=False, methods=['post'])
    def bulk_create(self, request):
        """Save multiple expenses at once"""
        transactions = request.data.get('transactions', [])
        
        if not transactions:
            return Response(
                {'error': 'No transactions provided'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Build expense objects
        expense_objects = [
            Expense(
                user=request.user,
                amount=tx['amount'],
                transaction_type=tx['transaction_type'],
                description=tx.get('description', ''),
                category_id=tx.get('category'),
            )
            for tx in transactions
        ]
        
        # Bulk insert (single DB query)
        Expense.objects.bulk_create(expense_objects)
        
        return Response(
            {'created': len(expense_objects)},
            status=status.HTTP_201_CREATED
        )
    
    @action(detail=True, methods=['patch'])
    def update_category(self, request, pk=None):
        """
        Update expense category + optionally create smart rule
        Body: {"category": 1, "create_rule": true}
        """
        expense = self.get_object()
        category_id = request.data.get('category')
        create_rule = request.data.get('create_rule', True)
        
        if not category_id:
            return Response(
                {'error': 'Category ID required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            category = Category.objects.get(id=category_id)
            expense.category = category
            expense.save()
            
            # Create smart categorization rule
            if create_rule:
                # Extract keyword from description
                words = expense.description.split()[:2]
                keyword = ' '.join(words) if words else expense.description[:20]
                
                CategoryRule.objects.update_or_create(
                    user=request.user,
                    description_keyword=keyword,
                    defaults={'category': category}
                )
                
                # Apply to similar uncategorized expenses
                Expense.objects.filter(
                    user=request.user,
                    description__icontains=keyword,
                    category__isnull=True
                ).update(category=category)
            
            return Response(ExpenseSerializer(expense).data)
        except Category.DoesNotExist:
            return Response(
                {'error': 'Category not found'},
                status=status.HTTP_404_NOT_FOUND
            )
    


# ============================================
# Analytics Views
# ============================================
class AnalyticsViewSet(viewsets.ViewSet):
    """Analytics endpoints for expenses and budgets"""
    permission_classes = [IsAuthenticated]
    
    def get_date_range(self, request):
        """Parse date range from query params"""
        start_date = request.query_params.get('start_date')
        end_date = request.query_params.get('end_date')
        
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        else:
            # Default to last 6 months
            start_date = date.today() - relativedelta(months=6)
        
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        else:
            end_date = date.today()
        
        return start_date, end_date
    
    @extend_schema(
        parameters=[
            OpenApiParameter('start_date', OpenApiTypes.DATE, description='Start date (YYYY-MM-DD)'),
            OpenApiParameter('end_date', OpenApiTypes.DATE, description='End date (YYYY-MM-DD)'),
            OpenApiParameter('transaction_type', OpenApiTypes.STR, description='DEBIT or CREDIT'),
        ],
        responses={200: CategoryAnalyticsSerializer(many=True)}
    )
    @action(detail=False, methods=['get'])
    def by_category(self, request):
        """
        Get expense/income analytics grouped by category.
        Query params:
        - start_date: Filter from date (YYYY-MM-DD)
        - end_date: Filter to date (YYYY-MM-DD)
        - transaction_type: DEBIT or CREDIT (optional)
        """
        start_date, end_date = self.get_date_range(request)
        transaction_type = request.query_params.get('transaction_type')
        
        # Base query
        queryset = Expense.objects.filter(
            user=request.user,
            date__gte=start_date,
            date__lte=end_date
        )
        
        # Filter by transaction type if specified
        if transaction_type in ['DEBIT', 'CREDIT']:
            queryset = queryset.filter(transaction_type=transaction_type)
        
        # Group by category
        category_stats = queryset.values(
            'category__id',
            'category__name'
        ).annotate(
            total_amount=Sum('amount'),
            transaction_count=Count('id'),
            avg_transaction=Avg('amount')
        ).order_by('-total_amount')
        
        # Calculate total for percentages
        total = queryset.aggregate(total=Sum('amount'))['total'] or Decimal('0')
        
        # Add percentages
        results = []
        for stat in category_stats:
            category_name = stat['category__name'] or 'Uncategorized'
            amount = stat['total_amount'] or Decimal('0')
            percentage = (amount / total * 100) if total > 0 else Decimal('0')
            
            results.append({
                'category_name': category_name,
                'category_id': stat['category__id'],
                'total_amount': amount,
                'transaction_count': stat['transaction_count'],
                'percentage': round(percentage, 2),
                'avg_transaction': round(stat['avg_transaction'] or Decimal('0'), 2)
            })
        
        serializer = CategoryAnalyticsSerializer(results, many=True)
        return Response(serializer.data)
    
    @extend_schema(
        parameters=[
            OpenApiParameter('start_date', OpenApiTypes.DATE, description='Start date (YYYY-MM-DD)'),
            OpenApiParameter('end_date', OpenApiTypes.DATE, description='End date (YYYY-MM-DD)'),
        ],
        responses={200: MonthlyAnalyticsSerializer(many=True)}
    )
    @action(detail=False, methods=['get'])
    def by_month(self, request):
        """
        Get expense/income analytics grouped by month.
        Query params:
        - start_date: Filter from date (YYYY-MM-DD)
        - end_date: Filter to date (YYYY-MM-DD)
        """
        start_date, end_date = self.get_date_range(request)
        
        # Get expenses grouped by month
        expenses_by_month = Expense.objects.filter(
            user=request.user,
            date__gte=start_date,
            date__lte=end_date
        ).annotate(
            month_date=TruncMonth('date')
        ).values('month_date').annotate(
            total_income=Sum('amount', filter=Q(transaction_type='CREDIT')),
            total_expenses=Sum('amount', filter=Q(transaction_type='DEBIT')),
            transaction_count=Count('id')
        ).order_by('month_date')
        
        # Format results
        results = []
        for stat in expenses_by_month:
            month_date = stat['month_date']
            income = stat['total_income'] or Decimal('0')
            expenses = stat['total_expenses'] or Decimal('0')
            
            results.append({
                'month': month_date.strftime('%B'),
                'year': month_date.year,
                'total_income': income,
                'total_expenses': expenses,
                'net_amount': income - expenses,
                'transaction_count': stat['transaction_count']
            })
        
        serializer = MonthlyAnalyticsSerializer(results, many=True)
        return Response(serializer.data)
    
    @extend_schema(
        parameters=[
            OpenApiParameter('start_date', OpenApiTypes.DATE, description='Start date (YYYY-MM-DD)'),
            OpenApiParameter('end_date', OpenApiTypes.DATE, description='End date (YYYY-MM-DD)'),
            OpenApiParameter('month', OpenApiTypes.DATE, description='Specific month for budget comparison (YYYY-MM-01)'),
        ],
        responses={200: AnalyticsSummarySerializer}
    )
    @action(detail=False, methods=['get'])
    def summary(self, request):
        """
        Get overall analytics summary including budget information.
        Query params:
        - start_date: Filter from date (YYYY-MM-DD)
        - end_date: Filter to date (YYYY-MM-DD)
        - month: Specific month for budget comparison (YYYY-MM-01)
        """
        start_date, end_date = self.get_date_range(request)
        
        # Get all expenses in range
        expenses = Expense.objects.filter(
            user=request.user,
            date__gte=start_date,
            date__lte=end_date
        )
        
        # Calculate totals
        totals = expenses.aggregate(
            total_income=Sum('amount', filter=Q(transaction_type='CREDIT')),
            total_expenses=Sum('amount', filter=Q(transaction_type='DEBIT')),
            total_count=Count('id')
        )
        
        income = totals['total_income'] or Decimal('0')
        expenses_total = totals['total_expenses'] or Decimal('0')
        
        # Top expense category
        top_category = expenses.filter(
            transaction_type='DEBIT'
        ).values(
            'category__name'
        ).annotate(
            total=Sum('amount')
        ).order_by('-total').first()
        
        # Budget information for current/specified month
        month_param = request.query_params.get('month')
        if month_param:
            budget_month = datetime.strptime(month_param, '%Y-%m-%d').date()
        else:
            budget_month = date.today().replace(day=1)
        
        # Get budgets for the month
        budgets = Budget.objects.filter(
            user=request.user,
            month=budget_month
        )
        
        total_budget = budgets.aggregate(total=Sum('amount'))['total'] or Decimal('0')
        
        # Calculate actual spending for budget month
        month_expenses = Expense.objects.filter(
            user=request.user,
            date__year=budget_month.year,
            date__month=budget_month.month,
            transaction_type='DEBIT'
        ).aggregate(total=Sum('amount'))['total'] or Decimal('0')
        
        budget_remaining = total_budget - month_expenses
        budget_utilization = (month_expenses / total_budget * 100) if total_budget > 0 else Decimal('0')
        
        result = {
            'total_income': income,
            'total_expenses': expenses_total,
            'net_balance': income - expenses_total,
            'total_transactions': totals['total_count'],
            'top_expense_category': top_category['category__name'] if top_category else None,
            'top_expense_amount': top_category['total'] if top_category else Decimal('0'),
            'period_start': start_date,
            'period_end': end_date,
            'total_budget': total_budget,
            'budget_remaining': budget_remaining,
            'budget_utilization': round(budget_utilization, 2)
        }
        
        serializer = AnalyticsSummarySerializer(result)
        return Response(serializer.data)


class BudgetViewSet(viewsets.ModelViewSet):
    """Budget management endpoints"""
    serializer_class = BudgetSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        queryset = Budget.objects.filter(user=self.request.user).select_related('category')
        
        # Filter by month if specified
        month = self.request.query_params.get('month')
        if month:
            try:
                month_date = datetime.strptime(month, '%Y-%m-%d').date()
                queryset = queryset.filter(month=month_date)
            except ValueError:
                pass
        
        return queryset
    
    def perform_create(self, serializer):
        serializer.save(user=self.request.user)
    
    @action(detail=False, methods=['get'])
    def current_month(self, request):
        """Get budgets for current month with spending info"""
        current_month = date.today().replace(day=1)
        
        budgets = Budget.objects.filter(
            user=request.user,
            month=current_month
        ).select_related('category')
        
        # Annotate with spending
        results = []
        for budget in budgets:
            spent = Expense.objects.filter(
                user=request.user,
                category=budget.category,
                date__year=current_month.year,
                date__month=current_month.month,
                transaction_type='DEBIT'
            ).aggregate(total=Sum('amount'))['total'] or Decimal('0')
            
            remaining = budget.amount - spent
            percentage_used = (spent / budget.amount * 100) if budget.amount > 0 else Decimal('0')
            
            budget_data = BudgetSerializer(budget).data
            budget_data['spent'] = spent
            budget_data['remaining'] = remaining
            budget_data['percentage_used'] = round(percentage_used, 2)
            
            results.append(budget_data)
        
        return Response(results)