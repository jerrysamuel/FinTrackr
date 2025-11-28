from django.urls import path
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
)
from . import views

urlpatterns = [
    # Registration
    path('register/', views.RegisterView.as_view(), name='register'),
    
    # JWT Token endpoints
    path('login/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    
    # User profile
    path('me/', views.current_user, name='current_user'),
    path('me/update/', views.update_profile, name='update_profile'),
]
