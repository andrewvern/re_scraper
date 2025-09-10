# Real Estate Data Scraping Backend

A robust backend system for a Chrome extension that aggregates real estate data from major housing and apartment websites including Redfin, Apartments.com, and Zillow. The backend serves as the data foundation for existing analytics and frontend infrastructure, focusing exclusively on data acquisition, processing, and modeling capabilities.

## Features

- **Multi-source Data Aggregation**: Unified data collection from major real estate platforms (Redfin, Apartments.com, Zillow)
- **Real-time Scraping Engine**: Automated data extraction with configurable scheduling and rate limiting
- **Data Processing Pipeline**: Standardization, deduplication, and enrichment of raw property data
- **Structured Data Models**: Consistent schema for properties, pricing, location, and market metrics
- **REST API**: Clean FastAPI endpoints for frontend consumption with authentication
- **Data Quality Assurance**: Validation, error handling, and data integrity monitoring
- **Containerized Deployment**: Docker support for easy deployment and scaling
- **Background Processing**: Celery-based task queue for scraping and data processing
- **Monitoring & Alerting**: Comprehensive logging, metrics collection, and alert management

## Architecture

### Technology Stack

- **Backend Framework**: FastAPI with Python 3.11
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Task Queue**: Celery with Redis broker
- **Web Scraping**: BeautifulSoup + Selenium with anti-detection measures
- **Data Processing**: Pandas-based ETL pipeline
- **Containerization**: Docker and Docker Compose
- **API Documentation**: Auto-generated OpenAPI/Swagger docs

### Component Overview

```
├── src/
│   ├── api/              # FastAPI application and routes
│   ├── scrapers/         # Web scraping modules
│   ├── models/           # Database and Pydantic models
│   ├── database/         # Database configuration and CRUD operations
│   ├── etl/              # Data processing pipeline
│   ├── tasks/            # Celery background tasks
│   ├── monitoring/       # Logging, metrics, and alerting
│   └── config/           # Configuration management
├── alembic/              # Database migrations
├── nginx/                # Nginx configuration
├── scripts/              # Database initialization scripts
└── docker-compose.yml   # Container orchestration
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- PostgreSQL 15+ (if running without Docker)
- Redis 7+ (if running without Docker)

### Using Docker (Recommended)

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd real-estate-scraper
   ```

2. **Configure environment**
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

3. **Start the services**
   ```bash
   # Production deployment
   docker-compose up -d

   # Development with hot reload
   docker-compose -f docker-compose.dev.yml up -d
   ```

4. **Initialize the database**
   ```bash
   # Run database migrations
   docker-compose exec api alembic upgrade head
   ```

5. **Create initial user (optional)**
   ```bash
   docker-compose exec api python -c "
   from src.api.auth import get_password_hash
   print('Hashed password:', get_password_hash('your_password'))
   "
   ```

### Local Development Setup

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment variables**
   ```bash
   cp env.example .env
   # Configure database and Redis URLs
   ```

3. **Start external services**
   ```bash
   # PostgreSQL and Redis
   docker-compose up postgres redis -d
   ```

4. **Run database migrations**
   ```bash
   alembic upgrade head
   ```

5. **Start the application**
   ```bash
   # API server
   uvicorn src.api.main:app --reload

   # Celery worker (in another terminal)
   celery -A src.tasks.celery worker --loglevel=info

   # Celery beat scheduler (in another terminal)
   celery -A src.tasks.celery beat --loglevel=info
   ```

## API Documentation

Once the application is running, you can access:

- **API Documentation**: http://localhost:8000/docs (Swagger UI)
- **Alternative Docs**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/api/v1/health

### Authentication

The API uses JWT bearer tokens for authentication. Default users:

- Username: `admin`, Password: `secret`
- Username: `user`, Password: `secret`

**Get access token:**
```bash
curl -X POST "http://localhost:8000/api/v1/auth/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=secret"
```

**Use the token:**
```bash
curl -H "Authorization: Bearer <your-token>" \
  "http://localhost:8000/api/v1/properties/"
```

## Usage Examples

### Start a Scraping Job

```bash
curl -X POST "http://localhost:8000/api/v1/scraping/jobs" \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "data_source": "redfin",
    "search_criteria": {
      "location": "San Francisco, CA",
      "min_price": 500000,
      "max_price": 2000000,
      "bedrooms": 2
    },
    "max_pages": 5
  }'
```

### Search Properties

```bash
curl "http://localhost:8000/api/v1/properties/?city=San%20Francisco&min_price=500000&max_price=1000000" \
  -H "Authorization: Bearer <your-token>"
```

### Get Property Details

```bash
curl "http://localhost:8000/api/v1/properties/123" \
  -H "Authorization: Bearer <your-token>"
```

## Configuration

### Environment Variables

Key configuration options (see `env.example` for complete list):

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | localhost |
| `DB_PORT` | PostgreSQL port | 5432 |
| `REDIS_HOST` | Redis host | localhost |
| `SECRET_KEY` | JWT secret key | (required) |
| `REQUESTS_PER_MINUTE` | Scraping rate limit | 30 |
| `HEADLESS_BROWSER` | Run browser headless | true |
| `LOG_LEVEL` | Logging level | INFO |

### Scraping Configuration

Configure scraping behavior:

- **Rate Limiting**: `REQUESTS_PER_MINUTE`, `DELAY_BETWEEN_REQUESTS`
- **Anti-Detection**: `ROTATE_USER_AGENTS`, `RANDOM_DELAYS`, `USE_PROXY`
- **Browser Settings**: `HEADLESS_BROWSER`, `BROWSER_TIMEOUT`

## Data Processing Pipeline

The ETL pipeline includes:

1. **Data Validation**: Field validation, type checking, business rules
2. **Data Transformation**: Standardization, cleaning, enrichment
3. **Deduplication**: Address-based duplicate detection
4. **Data Loading**: Efficient database storage with error handling

### Data Quality

- **Validation Rules**: Required fields, value ranges, format checks
- **Quality Scoring**: 0-100 score based on completeness and accuracy
- **Error Handling**: Detailed error tracking and reporting

## Monitoring & Alerting

### Metrics Collection

- **System Metrics**: CPU, memory, disk usage
- **Application Metrics**: API response times, error rates
- **Scraping Metrics**: Success rates, processing times
- **Database Metrics**: Connection pools, query performance

### Alerting

Configurable alerts for:

- High error rates (API, scraping)
- System resource usage
- Failed jobs and processing errors
- Data quality issues

### Logging

Structured logging with:

- **JSON Format**: Machine-readable logs
- **Context Tracking**: Request IDs, user info, job IDs
- **Log Levels**: DEBUG, INFO, WARNING, ERROR
- **External Integration**: Sentry support

## Deployment

### Production Deployment

1. **Configure environment**
   ```bash
   # Set production values in .env
   ENVIRONMENT=production
   API_DEBUG=false
   SECRET_KEY=<strong-secret-key>
   ```

2. **Deploy with Docker**
   ```bash
   docker-compose up -d
   ```

3. **Set up SSL (recommended)**
   - Configure SSL certificates in `nginx/ssl/`
   - Uncomment HTTPS server block in `nginx/nginx.conf`

4. **Monitor logs**
   ```bash
   docker-compose logs -f api worker scheduler
   ```

### Scaling

- **Horizontal Scaling**: Run multiple worker containers
- **Database**: Configure connection pooling and read replicas
- **Load Balancing**: Use nginx or external load balancer
- **Caching**: Add Redis caching layer for API responses

## Development

### Code Structure

- **Models**: SQLAlchemy and Pydantic models in `src/models/`
- **API Routes**: FastAPI routers in `src/api/routes/`
- **Scrapers**: Platform-specific scrapers in `src/scrapers/`
- **Background Tasks**: Celery tasks in `src/tasks/`

### Adding New Scrapers

1. Create scraper class inheriting from `BaseScraper`
2. Implement `search_properties()` and `get_property_details()`
3. Add to scraper factory in `src/tasks/scraping_tasks.py`
4. Update data source enum in `src/models/property_models.py`

### Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test file
pytest tests/test_scrapers.py
```

### Database Migrations

```bash
# Create new migration
alembic revision --autogenerate -m "Add new feature"

# Apply migrations
alembic upgrade head

# Rollback migration
alembic downgrade -1
```

## Troubleshooting

### Common Issues

1. **Scraping Blocked**: Implement proxy rotation, adjust delays
2. **Memory Usage**: Tune batch sizes, worker memory limits
3. **Database Locks**: Check for long-running queries, tune timeouts
4. **Rate Limiting**: Adjust `REQUESTS_PER_MINUTE` setting

### Debugging

```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs api
docker-compose logs worker

# Execute commands in container
docker-compose exec api python -c "from src.database.connection import check_db_connection; print(check_db_connection())"

# Monitor Celery tasks
docker-compose exec worker celery -A src.tasks.celery inspect active
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

[Your license here]

## Support

For issues and questions:
- Create an issue in the repository
- Check the documentation
- Review logs for error details

