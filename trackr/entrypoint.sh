#!/bin/sh

# entrypoint.sh - Enhanced initialization script

set -e  # Exit on error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo "${RED}[ERROR]${NC} $1"
}

# Display startup banner
echo "========================================"
echo "  Django Application Startup"
echo "========================================"
log_info "Environment: ${DJANGO_ENV:-development}"
log_info "Debug mode: ${DEBUG:-True}"

# Wait for database with timeout
log_info "Waiting for PostgreSQL..."
DB_HOST="${DB_HOST:-neondb}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
MAX_ATTEMPTS=30
ATTEMPT=0

until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" > /dev/null 2>&1; do
    ATTEMPT=$((ATTEMPT + 1))
    if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
        log_error "PostgreSQL did not become ready in time!"
        exit 1
    fi
    log_warn "PostgreSQL is unavailable - attempt $ATTEMPT/$MAX_ATTEMPTS"
    sleep 2
done
log_info "PostgreSQL is ready!"

# Test database connection
log_info "Testing database connection..."
if python manage.py check --database default > /dev/null 2>&1; then
    log_info "Database connection successful!"
else
    log_error "Database connection failed!"
    exit 1
fi

# Run migrations
log_info "Running database migrations..."
if python manage.py migrate --noinput; then
    log_info "Migrations completed successfully!"
else
    log_error "Migration failed!"
    exit 1
fi

# Collect static files (production only)
if [ "$DEBUG" = "False" ] || [ "$DJANGO_ENV" = "production" ]; then
    log_info "Collecting static files..."
    if python manage.py collectstatic --noinput --clear; then
        log_info "Static files collected!"
    else
        log_warn "Static file collection failed (non-critical)"
    fi
else
    log_warn "Skipping collectstatic in development mode"
fi

# Create superuser if environment variables are set
if [ -n "$DJANGO_SUPERUSER_USERNAME" ] && [ -n "$DJANGO_SUPERUSER_PASSWORD" ]; then
    log_info "Checking superuser..."
    python manage.py shell << END
from django.contrib.auth import get_user_model
User = get_user_model()
if not User.objects.filter(username='$DJANGO_SUPERUSER_USERNAME').exists():
    User.objects.create_superuser(
        username='$DJANGO_SUPERUSER_USERNAME',
        email='$DJANGO_SUPERUSER_EMAIL',
        password='$DJANGO_SUPERUSER_PASSWORD'
    )
    print('${GREEN}[INFO]${NC} Superuser created successfully!')
else:
    print('${YELLOW}[WARN]${NC} Superuser already exists')
END
fi

# Show application info
log_info "Application initialized successfully!"
log_info "Starting: $*"
echo "========================================"

# Execute the main command
exec "$@"