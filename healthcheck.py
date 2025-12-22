import sys
from django.db import connection

try:
    connection.ensure_connection()
except Exception:
    sys.exit(1)
