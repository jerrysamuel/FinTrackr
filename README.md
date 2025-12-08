# 💰 Expense Tracker API

A professional, production-ready RESTful API for personal expense tracking with smart categorization, ETL pipeline, and comprehensive analytics.

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![Django](https://img.shields.io/badge/Django-4.2+-green.svg)](https://www.djangoproject.com/)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 🌟 Features

### Core Functionality
- 🔐 **JWT Authentication** - Secure user registration and login
- 📊 **File Upload with ETL** - Import CSV/Excel files with automatic data cleaning
- 🤖 **Smart Categorization** - Auto-learn and apply category rules
- 💰 **Debit/Credit Tracking** - Separate expenses and income
- 📈 **Advanced Analytics** - Financial summaries, trends, and breakdowns
- 💵 **Budget Management** - Set limits and track spending in real-time
- 📚 **Swagger Documentation** - Interactive API documentation

### Technical Features
- 🐳 **Dockerized** - Production-ready containerization
- 🗄️ **PostgreSQL** - Robust database with connection pooling
- 🔒 **Security Hardened** - HTTPS, CORS, rate limiting ready
- 📝 **Comprehensive Logging** - Track errors and usage
- 🚀 **Scalable Architecture** - Ready for cloud deployment
- ✅ **Type Safety** - Clean, well-documented code

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 15+ (or Docker)
- Git

### Local Development Setup

1. **Clone the repository**
```bash
git clone https://github.com/jerrysamuel/expense-tracker-api.git
cd expense-tracker-api/backend
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Setup environment variables**
```bash
cp .env.example .env
# Edit .env with your settings
```

5. **Run migrations**
```bash
python manage.py migrate
```

6. **Create default categories**
```bash
python manage.py shell
>>> from expenses.models import Category
>>> categories = [
...     ('Food & Dining', '#ef4444'),
...     ('Transportation', '#3b82f6'),
...     ('Shopping', '#8b5cf6'),
...     ('Entertainment', '#ec4899'),
...     ('Bills & Utilities', '#f59e0b'),
...     ('Healthcare', '#10b981'),
...     ('Income', '#22c55e'),
... ]
>>> for name, color in categories:
...     Category.objects.get_or_create(name=name, is_default=True, defaults={'color': color})
>>> exit()
```

7. **Create superuser**
```bash
python manage.py createsuperuser
```

8. **Run development server**
```bash
python manage.py runserver
```

9. **Access the API**
- API Documentation: http://localhost:8000/api/docs/
- Admin Panel: http://localhost:8000/admin/

---

## 🐳 Docker Setup

### Using Docker Compose (Recommended)

1. **Build and start services**
```bash
docker-compose up --build
```

2. **Run migrations**
```bash
docker-compose exec web python manage.py migrate
```

3. **Create superuser**
```bash
docker-compose exec web python manage.py createsuperuser
```

4. **Access the application**
- API: http://localhost:8000/api/docs/

### Stop services
```bash
docker-compose down
```

---

## 📚 API Documentation

### Interactive Documentation
Once running, visit:
- **Swagger UI:** http://localhost:8000/api/docs/
- **ReDoc:** http://localhost:8000/api/redoc/

### Quick API Overview

#### Authentication
```bash
# Register
POST /api/auth/register/
{
  "username": "john_doe",
  "email": "john@example.com",
  "password": "SecurePass123",
  "password2": "SecurePass123"
}

# Login
POST /api/auth/login/
{
  "username": "john_doe",
  "password": "SecurePass123"
}

# Response includes JWT tokens
{
  "access": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "refresh": "eyJ0eXAiOiJKV1QiLCJhbGc..."
}
```

#### File Upload Workflow
```bash
# Step 1: Upload CSV/Excel file
POST /api/expenses/upload/
Headers: Authorization: Bearer {access_token}
Body: form-data with 'file' field

# Response: Preview of transactions
{
  "transactions": [...],
  "column_mapping": {...},
  "total_count": 25
}

# Step 2: Save transactions
POST /api/expenses/bulk-create/
Headers: Authorization: Bearer {access_token}
Body: {
  "transactions": [...]
}
```

#### Analytics
```bash
# Financial summary
GET /api/analytics/summary/
Headers: Authorization: Bearer {access_token}

# Category breakdown
GET /api/analytics/by-category/?type=DEBIT
Headers: Authorization: Bearer {access_token}

# Monthly trends
GET /api/analytics/by-month/?months=6
Headers: Authorization: Bearer {access_token}
```

---

## 🏗️ Project Structure

```
expense-tracker-api/
├── backend/
│   ├── config/                 # Django settings
│   │   ├── settings.py        # Main settings
│   │   ├── urls.py            # URL routing
│   │   └── wsgi.py            # WSGI config
│   ├── expenses/              # Expenses app
│   │   ├── models.py          # Database models
│   │   ├── serializers.py     # DRF serializers
│   │   ├── views.py           # API views
│   │   ├── urls.py            # App URLs
│   │   └── services/          # Business logic
│   │       ├── etl_service.py # ETL pipeline
│   │       └── column_detector.py
│   ├── users/                 # Authentication app
│   │   ├── serializers.py
│   │   ├── views.py
│   │   └── urls.py
│   ├── manage.py
│   ├── requirements.txt
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── .env.example
├── .gitignore
└── README.md
```

---

## 🧪 Testing

### Run Tests
```bash
python manage.py test
```

### Test Coverage
```bash
pip install coverage
coverage run --source='.' manage.py test
coverage report
coverage html  # Generate HTML report
```

### Manual Testing with cURL

```bash
# Health check
curl http://localhost:8000/health/

# Register user
curl -X POST http://localhost:8000/api/auth/register/ \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser","email":"test@test.com","password":"Test123456","password2":"Test123456"}'

# Upload file
curl -X POST http://localhost:8000/api/expenses/upload/ \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@test_expenses.csv"
```

---

## 🔒 Security

### Built-in Security Features
- ✅ JWT token authentication
- ✅ Password hashing with Django's PBKDF2
- ✅ CORS configuration
- ✅ CSRF protection
- ✅ SQL injection prevention (Django ORM)
- ✅ XSS protection
- ✅ HTTPS enforcement in production
- ✅ Secure headers (HSTS, X-Frame-Options, etc.)

### Environment Variables
Never commit sensitive data. Use `.env` file:
```bash
SECRET_KEY=your-secret-key
DEBUG=False
DATABASE_URL=postgresql://user:pass@host:5432/db
ALLOWED_HOSTS=yourdomain.com
```

---

## 🚀 Deployment

### Deploy to Render (Free)

1. Push code to GitHub
2. Create account at [Render](https://render.com)
3. Create PostgreSQL database
4. Create Web Service from GitHub repo
5. Set environment variables
6. Deploy!

[Full deployment guide](./docs/DEPLOYMENT.md)

### Deploy to Digital Ocean

1. Create droplet
2. Install Docker
3. Clone repository
4. Setup environment variables
5. Run with Docker Compose
6. Setup Nginx and SSL

[Detailed instructions](./docs/DEPLOYMENT.md)

---

## 📊 Database Schema

### Key Models

**User** (Django built-in)
- Authentication and profile

**Expense**
- date, amount, transaction_type (DEBIT/CREDIT)
- description, category, notes
- Automatic categorization on save

**Category**
- name, color, is_default
- User-custom or system-wide

**CategoryRule**
- Smart categorization patterns
- description_keyword → category mapping
- Auto-applies to similar transactions

**Budget**
- Monthly spending limits per category
- Real-time spent/remaining calculations
- Over-budget alerts

---

## 🔧 Configuration

### Settings Overview

**Development (`DEBUG=True`)**
- SQLite database
- Browsable API enabled
- Detailed error pages
- No security restrictions

**Production (`DEBUG=False`)**
- PostgreSQL database
- JSON-only responses
- Secure cookies and headers
- HTTPS enforcement
- Static files with WhiteNoise

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SECRET_KEY` | Django secret key | Required |
| `DEBUG` | Debug mode | `False` |
| `DATABASE_URL` | PostgreSQL URL | SQLite |
| `ALLOWED_HOSTS` | Allowed domains | `localhost` |
| `CORS_ALLOWED_ORIGINS` | Frontend URLs | None |

---

## 📈 Performance

### Optimization Features
- Database connection pooling
- Query optimization with select_related
- Pagination for large datasets
- Static file compression with WhiteNoise
- Efficient CSV parsing with Pandas

### Scalability
- Stateless API (JWT tokens)
- Docker containerization
- Ready for load balancer
- Database indexes on key fields
- Prepared for Redis caching

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

### Code Style
- Follow PEP 8
- Use type hints where applicable
- Write docstrings for functions
- Add tests for new features

---

## 🐛 Known Issues & Limitations

- Free tier services may spin down after inactivity
- CSV files limited to 5MB (configurable)
- No real-time updates (polling required)
- Single currency support (for now)

---

## 🗺️ Roadmap

### V2 Features (Planned)
- [ ] PDF/DOCX file support (Premium)
- [ ] AI-powered categorization with ML
- [ ] Receipt OCR with Tesseract
- [ ] Recurring expense tracking
- [ ] Multi-currency support
- [ ] Export to PDF reports
- [ ] Email notifications
- [ ] Spending predictions
- [ ] Shared budgets (family/team)
- [ ] Mobile app (React Native)

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👨‍💻 Author

**Your Name**
- GitHub: [@yourusername](https://github.com/jerrysamuel)
- LinkedIn: [Your Profile](https://linkedin.com/in/)
- Email: replyguymail@gmail.com

---

## 🙏 Acknowledgments

- Django & DRF community
- Pandas for powerful data processing
- drf-spectacular for excellent API docs
- All open-source contributors

---

## 📞 Support

- 📧 Email: support@yourapi.com
- 🐛 Issues: [GitHub Issues](https://github.com/jerrysamuel/expense-tracker-api/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/jerrysamuel/expense-tracker-api/discussions)
- 📚 Documentation: [Full Docs](https://your-docs-site.com)

---

## 📊 Project Stats

![GitHub stars](https://img.shields.io/github/stars/yourusername/expense-tracker-api?style=social)
![GitHub forks](https://img.shields.io/github/forks/yourusername/expense-tracker-api?style=social)
![GitHub issues](https://img.shields.io/github/issues/yourusername/expense-tracker-api)

---

Made with ❤️ and Django
