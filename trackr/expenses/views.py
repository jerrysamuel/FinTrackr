
from rest_framework import viewsets, status, parsers
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.db.models import Sum, Count, Q
from django.db.models.functions import TruncMonth
from datetime import datetime, timedelta
from .models import Expense, Category, CategoryRule, Budget
from drf_spectacular.utils import (
    extend_schema, 
    OpenApiParameter, 
    OpenApiExample,
    OpenApiResponse,
    inline_serializer
)
from drf_spectacular.types import OpenApiTypes
from .serializers import (
    ExpenseSerializer, CategorySerializer, 
    CategoryRuleSerializer, BudgetSerializer
)
from .services.etl_service import ETLService


class CategoryViewSet(viewsets.ModelViewSet):
    """Category CRUD"""
    serializer_class = CategorySerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        # Return default categories + user's custom categories
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



@extend_schema(tags=['Expenses'])
class ExpenseViewSet(viewsets.ModelViewSet):
    """Expense CRUD + File Upload"""
    serializer_class = ExpenseSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        queryset = Expense.objects.filter(user=self.request.user)
        
        # Filters
        transaction_type = self.request.query_params.get('type', None)
        category = self.request.query_params.get('category', None)
        start_date = self.request.query_params.get('start_date', None)
        end_date = self.request.query_params.get('end_date', None)
        
        if transaction_type:
            queryset = queryset.filter(transaction_type=transaction_type)
        if category:
            queryset = queryset.filter(category_id=category)
        if start_date:
            queryset = queryset.filter(date__gte=start_date)
        if end_date:
            queryset = queryset.filter(date__lte=end_date)
        
        return queryset
    
    def perform_create(self, serializer):
        serializer.save(user=self.request.user)
    
    @extend_schema(
        summary='List all expenses',
        description='Retrieve paginated list of user expenses with optional filters.',
        parameters=[
            OpenApiParameter(
                name='type',
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description='Filter by transaction type',
                enum=['DEBIT', 'CREDIT'],
                required=False,
                examples=[
                    OpenApiExample('Debits only', value='DEBIT'),
                    OpenApiExample('Credits only', value='CREDIT'),
                ]
            ),
            OpenApiParameter(
                name='category',
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                description='Filter by category ID',
                required=False,
                examples=[
                    OpenApiExample('Food category', value=1),
                ]
            ),
            OpenApiParameter(
                name='start_date',
                type=OpenApiTypes.DATE,
                location=OpenApiParameter.QUERY,
                description='Filter from date (YYYY-MM-DD)',
                required=False,
                examples=[
                    OpenApiExample('Start of month', value='2024-01-01'),
                ]
            ),
            OpenApiParameter(
                name='end_date',
                type=OpenApiTypes.DATE,
                location=OpenApiParameter.QUERY,
                description='Filter to date (YYYY-MM-DD)',
                required=False,
                examples=[
                    OpenApiExample('End of month', value='2024-01-31'),
                ]
            ),
        ],
        responses={
            200: ExpenseSerializer(many=True),
            401: OpenApiResponse(description='Authentication credentials not provided'),
        }
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)
    
    @extend_schema(
        summary='Create single expense',
        description='Create a new expense manually (not via file upload).',
        request=ExpenseSerializer,
        responses={
            201: ExpenseSerializer,
            400: OpenApiResponse(description='Invalid data provided'),
            401: OpenApiResponse(description='Authentication required'),
        },
        examples=[
            OpenApiExample(
                'Create expense example',
                value={
                    'date': '2024-01-15',
                    'amount': 50.00,
                    'transaction_type': 'DEBIT',
                    'description': 'Lunch at restaurant',
                    'category': 1,
                    'notes': 'Business lunch'
                },
                request_only=True
            ),
        ]
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @extend_schema(
        summary='Get expense detail',
        description='Retrieve details of a specific expense.',
        responses={
            200: ExpenseSerializer,
            404: OpenApiResponse(description='Expense not found'),
        }
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @extend_schema(
        summary='Update expense',
        description='Update an existing expense (partial update supported).',
        request=ExpenseSerializer,
        responses={
            200: ExpenseSerializer,
            400: OpenApiResponse(description='Invalid data'),
            404: OpenApiResponse(description='Expense not found'),
        }
    )
    def partial_update(self, request, *args, **kwargs):
        return super().partial_update(request, *args, **kwargs)
    
    @extend_schema(
        summary='Delete expense',
        description='Delete an expense permanently.',
        responses={
            204: OpenApiResponse(description='Expense deleted successfully'),
            404: OpenApiResponse(description='Expense not found'),
        }
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)
    
    @extend_schema(
        summary='Upload expense file (ETL Extract + Transform)',
        description="""
        Upload CSV or Excel file for bulk expense import with automatic ETL processing.
        
        ## Supported File Formats
        - CSV (.csv)
        - Excel (.xlsx, .xls)
        
        ## Required Columns (Auto-detected)
        The system will automatically detect columns with these names (case-insensitive):
        - **Date**: Date, Trans Date, Transaction Date, Posted Date, Value Date
        - **Amount**: Amount, Value, Debit, Credit, Transaction Amount
        - **Description**: Description, Memo, Details, Narrative, Merchant
        
        ## Data Cleaning (Automatic)
        - Removes extra spaces from all fields
        - Handles multiple date formats (YYYY-MM-DD, DD/MM/YYYY, MM/DD/YYYY)
        - Parses amounts with currency symbols ($, £, €, ₦)
        - Handles negative amounts and accounting notation (50.00)
        - Removes duplicate transactions
        - Classifies as DEBIT (expenses) or CREDIT (income)
        
        ## Workflow
        1. Upload file
        2. System processes and cleans data
        3. Returns preview of transactions
        4. Review and assign categories if needed
        5. Call `/bulk_create/` endpoint to save
        
        ## Optional Parameters
        - **save_file**: Save original file for audit (default: false)
        - **auto_import**: Skip preview and save directly (default: false)
        - **date_column**: Manual column name override (if auto-detect fails)
        - **amount_column**: Manual column name override
        - **description_column**: Manual column name override
        """,
        request={
            'multipart/form-data': {
                'type': 'object',
                'properties': {
                    'file': {
                        'type': 'string',
                        'format': 'binary',
                        'description': 'CSV or Excel file to upload'
                    },
                    'save_file': {
                        'type': 'boolean',
                        'description': 'Save original file for reference',
                        'default': False
                    },
                    'auto_import': {
                        'type': 'boolean',
                        'description': 'Automatically import without preview',
                        'default': False
                    },
                    'date_column': {
                        'type': 'string',
                        'description': 'Manual date column name (optional)',
                        'example': 'Transaction Date'
                    },
                    'amount_column': {
                        'type': 'string',
                        'description': 'Manual amount column name (optional)',
                        'example': 'Value'
                    },
                    'description_column': {
                        'type': 'string',
                        'description': 'Manual description column name (optional)',
                        'example': 'Details'
                    }
                },
                'required': ['file']
            }
        },
        responses={
            200: OpenApiResponse(
                response=inline_serializer(
                    name='UploadPreviewResponse',
                    fields={
                        'transactions': drf_serializers.ListField(
                            child=drf_serializers.DictField(),
                            help_text='Array of processed transactions'
                        ),
                        'column_mapping': drf_serializers.DictField(
                            help_text='Detected column mappings'
                        ),
                        'total_count': drf_serializers.IntegerField(
                            help_text='Total number of transactions'
                        ),
                    }
                ),
                description='File processed successfully, preview returned',
                examples=[
                    OpenApiExample(
                        'Upload success',
                        value={
                            'transactions': [
                                {
                                    'date': '2024-01-15',
                                    'amount': 50.0,
                                    'transaction_type': 'DEBIT',
                                    'description': 'Uber ride downtown',
                                    'category': None,
                                    'category_name': None
                                },
                                {
                                    'date': '2024-01-16',
                                    'amount': 1500.0,
                                    'transaction_type': 'CREDIT',
                                    'description': 'Salary deposit',
                                    'category': None,
                                    'category_name': None
                                }
                            ],
                            'column_mapping': {
                                'date': 'Date',
                                'amount': 'Amount',
                                'description': 'Description'
                            },
                            'total_count': 2
                        }
                    )
                ]
            ),
            400: OpenApiResponse(
                description='File processing error',
                examples=[
                    OpenApiExample(
                        'No file',
                        value={'error': 'No file provided'}
                    ),
                    OpenApiExample(
                        'Invalid format',
                        value={'error': 'Unsupported file format. Use CSV or Excel.'}
                    ),
                    OpenApiExample(
                        'Column detection failed',
                        value={
                            'error': 'Could not auto-detect columns. Available columns: [...]'
                        }
                    ),
                ]
            ),
            401: OpenApiResponse(description='Authentication required'),
        }
    )
    @action(detail=False, methods=['post'], parser_classes=[parsers.MultiPartParser, parsers.FormParser])
    def upload(self, request):
        """Upload CSV/Excel and return preview (ETL Extract + Transform)"""
        file = request.FILES.get('file')
        
        if not file:
            return Response(
                {'error': 'No file provided'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Extract
            df, column_mapping = ETLService.extract(file)
            
            # Transform
            transactions = ETLService.transform(df, column_mapping)
            
            # Apply existing category rules
            user_rules = CategoryRule.objects.filter(user=request.user)
            for transaction in transactions:
                for rule in user_rules:
                    if rule.matches(transaction['description']):
                        transaction['category'] = rule.category.pk
                        transaction['category_name'] = rule.category.name
                        break
            
            return Response({
                'transactions': transactions,
                'column_mapping': column_mapping,
                'total_count': len(transactions)
            })
        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
    
    @extend_schema(
        summary='Bulk create expenses (ETL Load)',
        description="""
        Save multiple expenses from the preview data returned by the upload endpoint.
        
        ## Workflow
        1. Upload file via `/upload/` endpoint
        2. Review transactions in the preview response
        3. Optionally modify categories or amounts
        4. Send the transactions array to this endpoint to save
        
        ## Features
        - Validates each transaction before saving
        - Returns list of successfully created expenses
        - Reports any errors for individual transactions
        - Automatically links to the authenticated user
        
        ## Category Assignment
        If a transaction has a category ID, it will be assigned.
        If category is null/missing, smart categorization rules will be applied automatically.
        """,
        request=inline_serializer(
            name='BulkCreateRequest',
            fields={
                'transactions': drf_serializers.ListField(
                    child=drf_serializers.DictField(),
                    help_text='Array of transactions to create'
                ),
                'file_id': drf_serializers.IntegerField(
                    required=False,
                    help_text='Optional: Link to uploaded file record'
                ),
            }
        ),
        examples=[
            OpenApiExample(
                'Bulk create example',
                value={
                    'transactions': [
                        {
                            'date': '2024-01-15',
                            'amount': 50.00,
                            'transaction_type': 'DEBIT',
                            'description': 'Uber ride',
                            'category': 2
                        },
                        {
                            'date': '2024-01-16',
                            'amount': 1500.00,
                            'transaction_type': 'CREDIT',
                            'description': 'Salary',
                            'category': 7
                        }
                    ]
                },
                request_only=True
            )
        ],
        responses={
            201: OpenApiResponse(
                response=inline_serializer(
                    name='BulkCreateResponse',
                    fields={
                        'created': drf_serializers.IntegerField(
                            help_text='Number of expenses created'
                        ),
                        'errors': drf_serializers.ListField(
                            child=drf_serializers.DictField(),
                            help_text='List of errors for failed transactions'
                        ),
                        'expenses': ExpenseSerializer(many=True),
                    }
                ),
                description='Expenses created successfully',
                examples=[
                    OpenApiExample(
                        'Success response',
                        value={
                            'created': 2,
                            'errors': [],
                            'expenses': [
                                {
                                    'id': 1,
                                    'date': '2024-01-15',
                                    'amount': '50.00',
                                    'transaction_type': 'DEBIT',
                                    'description': 'Uber ride',
                                    'category': 2,
                                    'category_name': 'Transportation',
                                    'notes': '',
                                    'created_at': '2024-01-20T10:30:00Z',
                                    'updated_at': '2024-01-20T10:30:00Z'
                                }
                            ]
                        }
                    )
                ]
            ),
            400: OpenApiResponse(
                description='Invalid request',
                examples=[
                    OpenApiExample(
                        'No transactions',
                        value={'error': 'No transactions provided'}
                    ),
                    OpenApiExample(
                        'Partial failure',
                        value={
                            'created': 1,
                            'errors': [
                                {
                                    'index': 1,
                                    'error': 'Invalid date format',
                                    'transaction': {'date': 'invalid', 'amount': 50}
                                }
                            ],
                            'expenses': []
                        }
                    )
                ]
            ),
        }
    )
    @action(detail=False, methods=['post'])
    def bulk_create(self, request):
        """Save multiple expenses (ETL Load)"""
        transactions = request.data.get('transactions', [])
        
        if not transactions:
            return Response(
                {'error': 'No transactions provided'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        created_expenses = []
        errors = []
        
        for idx, transaction in enumerate(transactions):
            try:
                expense = Expense.objects.create(
                    user=request.user,
                    date=transaction['date'],
                    amount=transaction['amount'],
                    transaction_type=transaction['transaction_type'],
                    description=transaction['description'],
                    category_id=transaction.get('category'),
                )
                created_expenses.append(expense)
            except Exception as e:
                errors.append({
                    'index': idx,
                    'error': str(e),
                    'transaction': transaction
                })
        
        return Response({
            'created': len(created_expenses),
            'errors': errors,
            'expenses': ExpenseSerializer(created_expenses, many=True).data
        }, status=status.HTTP_201_CREATED)
    
    @extend_schema(
        summary='Update expense category',
        description="""
        Update the category of an expense and optionally create a smart categorization rule.
        
        ## Smart Categorization
        When `create_rule=true` (default), the system will:
        1. Extract keywords from the expense description
        2. Create a category rule for future matching
        3. Automatically apply the rule to other uncategorized expenses with similar descriptions
        
        ## Example
        If you categorize "Uber ride downtown" as "Transportation":
        - A rule is created: "Uber" → Transportation
        - All other expenses with "Uber" in description get auto-categorized
        """,
        request=inline_serializer(
            name='UpdateCategoryRequest',
            fields={
                'category': drf_serializers.IntegerField(help_text='Category ID'),
                'create_rule': drf_serializers.BooleanField(
                    default=True,
                    help_text='Create smart categorization rule'
                ),
            }
        ),
        examples=[
            OpenApiExample(
                'Update with rule creation',
                value={
                    'category': 2,
                    'create_rule': True
                },
                request_only=True
            ),
            OpenApiExample(
                'Update without rule',
                value={
                    'category': 2,
                    'create_rule': False
                },
                request_only=True
            )
        ],
        responses={
            200: ExpenseSerializer,
            400: OpenApiResponse(
                description='Invalid request',
                examples=[
                    OpenApiExample(
                        'Missing category',
                        value={'error': 'Category ID required'}
                    )
                ]
            ),
            404: OpenApiResponse(
                description='Not found',
                examples=[
                    OpenApiExample(
                        'Expense not found',
                        value={'detail': 'Not found.'}
                    ),
                    OpenApiExample(
                        'Category not found',
                        value={'error': 'Category not found'}
                    )
                ]
            ),
        }
    )
    @action(detail=True, methods=['patch'])
    def update_category(self, request, pk=None):
        """Update expense category and create/update rule"""
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
            
            # Create or update category rule
            if create_rule:
                # Extract keyword from description (first 2-3 words)
                words = expense.description.split()[:2]
                keyword = ' '.join(words) if words else expense.description[:20]
                
                CategoryRule.objects.update_or_create(
                    user=request.user,
                    description_keyword=keyword,
                    defaults={'category': category}
                )
                
                # Apply rule to similar transactions
                similar = Expense.objects.filter(
                    user=request.user,
                    description__icontains=keyword,
                    category__isnull=True
                )
                similar.update(category=category)
            
            return Response(ExpenseSerializer(expense).data)
        except Category.DoesNotExist:
            return Response(
                {'error': 'Category not found'},
                status=status.HTTP_404_NOT_FOUND
            )
class BudgetViewSet(viewsets.ModelViewSet):
    """Budget CRUD"""
    serializer_class = BudgetSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        return Budget.objects.filter(user=self.request.user)
    
    def perform_create(self, serializer):
        serializer.save(user=self.request.user)


# Analytics Views
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def analytics_summary(request):
    """Overall financial summary"""
    user = request.user
    
    # Get date range from query params or default to current month
    end_date = datetime.now().date()
    start_date = end_date.replace(day=1)
    
    debits = Expense.objects.filter(
        user=user,
        transaction_type='DEBIT',
        date__gte=start_date,
        date__lte=end_date
    ).aggregate(total=Sum('amount'))['total'] or 0
    
    credits = Expense.objects.filter(
        user=user,
        transaction_type='CREDIT',
        date__gte=start_date,
        date__lte=end_date
    ).aggregate(total=Sum('amount'))['total'] or 0
    
    return Response({
        'period': {
            'start': start_date,
            'end': end_date
        },
        'total_income': float(credits),
        'total_expenses': float(debits),
        'net_balance': float(credits - debits),
        'transaction_count': Expense.objects.filter(
            user=user,
            date__gte=start_date,
            date__lte=end_date
        ).count()
    })


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def analytics_by_category(request):
    """Spending breakdown by category"""
    user = request.user
    transaction_type = request.query_params.get('type', 'DEBIT')
    
    data = Expense.objects.filter(
        user=user,
        transaction_type=transaction_type,
        category__isnull=False
    ).values(
        'category__name', 'category__color'
    ).annotate(
        total=Sum('amount'),
        count=Count('id')
    ).order_by('-total')
    
    return Response(list(data))


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def analytics_by_month(request):
    """Monthly trends"""
    user = request.user
    months = int(request.query_params.get('months', 6))
    
    start_date = datetime.now().date() - timedelta(days=months*30)
    
    data = Expense.objects.filter(
        user=user,
        date__gte=start_date
    ).annotate(
        month=TruncMonth('date')
    ).values('month', 'transaction_type').annotate(
        total=Sum('amount')
    ).order_by('month')
    
    return Response(list(data))