# Store Monitoring System

backend service that tracks store uptime and downtime, handles large CSV imports (~1.9M records), and generates reports within minutes.

## Tech Stack

**Python** · **FastAPI** · **Postgres** · **SQLAlchemy** · **Celery**

## Performance Stats

- **Report Generation:** ~28 sec

## API Endpoints

### 1. Trigger Report Generation

```http
POST /trigger_report
```

**Response**:

```json
{
  "report_id": "86e11526-1075-4134-89b6-3d99176c1d20",
  "status": "Queued",
  "message": "Report generation started in background"
}
```

### 2. Get Report Status/Download

```http
GET /get_report/{report_id}
```

**Response**:

```json
{
  "status": "Running",
  "message": "Report is still being generated. Please try again later."
}
```

**Response (Completed)**: Downloads CSV file directly

### Local Development Setup

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd store-monitoring-system
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

4. **Start PostgreSQL and Redis**

   ```bash
   docker run -d --name postgres \
     -e POSTGRES_DB=loop_assignment \
     -e POSTGRES_USER=root \
     -e POSTGRES_PASSWORD=root \
     -p 5432:5432 postgres:15

   docker run -d --name redis -p 6379:6379 redis:7
   ```

5. **Ingest data**

   ```bash
   python ingest_csv.py
   ```

6. **Start Celery worker** (in separate terminal)

   ```bash
   celery -A celery_app worker --loglevel=info --concurrency=4
   ```

7. **Start API server**

   ````bash
   uvicorn main:app --reload
   ```s

   ````

8. **Access the API**
   - API: http://localhost:8000
   - API Docs: http://localhost:8000/docs

## Future Work

1. Migrate report generation to dedicated worker services using container orchestration (AWS ECS) and message queues (Redis/Kafka)
2. Implement **DuckDB** for high-performance analytical queries with embedded file-based storage, eliminating external database dependencies
