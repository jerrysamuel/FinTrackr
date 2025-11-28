
from rest_framework import status, generics
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework_simplejwt.tokens import RefreshToken
from django.contrib.auth.models import User
from .serializers import UserRegistrationSerializer, UserSerializer
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample
from drf_spectacular.types import OpenApiTypes


@extend_schema(
    tags=['Authentication'],
    summary='Register new user',
    description='Create a new user account and receive JWT tokens for immediate login.',
    request=UserRegistrationSerializer,
    responses={
        201: {
            'description': 'User created successfully',
            'content': {
                'application/json': {
                    'example': {
                        'user': {
                            'id': 1,
                            'username': 'john_doe',
                            'email': 'john@example.com',
                            'date_joined': '2024-01-20T10:30:00Z'
                        },
                        'tokens': {
                            'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGc...',
                            'access': 'eyJ0eXAiOiJKV1QiLCJhbGc...'
                        },
                        'message': 'User registered successfully'
                    }
                }
            }
        },
        400: {'description': 'Invalid data or username already exists'}
    }
)
class RegisterView(generics.CreateAPIView):
    """
    POST /api/auth/register/
    Body: {"username": "...", "email": "...", "password": "...", "password2": "..."}
    """
    queryset = User.objects.all()
    permission_classes = [AllowAny]
    serializer_class = UserRegistrationSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        
        # Generate tokens for immediate login
        refresh = RefreshToken.for_user(user)
        
        return Response({
            'user': UserSerializer(user).data,
            'tokens': {
                'refresh': str(refresh),
                'access': str(refresh.access_token),
            },
            'message': 'User registered successfully'
        }, status=status.HTTP_201_CREATED)



@extend_schema(
    tags=['Authentication'],
    summary='Get current user profile',
    description='Retrieve the authenticated user\'s profile information.',
    responses={
        200: UserSerializer,
        401: {'description': 'Authentication credentials not provided'}
    }
)
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def current_user(request):
    """
    GET /api/auth/me/
    Headers: Authorization: Bearer <access_token>
    """
    serializer = UserSerializer(request.user)
    return Response(serializer.data)


@extend_schema(
    tags=['Authentication'],
    summary='Update user profile',
    description='Update the authenticated user\'s profile information.',
    request=UserSerializer,
    responses={
        200: UserSerializer,
        400: {'description': 'Invalid data'},
        401: {'description': 'Authentication credentials not provided'}
    }
)
@api_view(['PATCH'])
@permission_classes([IsAuthenticated])
def update_profile(request):
    """
    PATCH /api/auth/me/
    Headers: Authorization: Bearer <access_token>
    Body: {"email": "newemail@example.com"}
    """
    serializer = UserSerializer(request.user, data=request.data, partial=True)
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
