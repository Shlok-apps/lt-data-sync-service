# 🔁 Auto Data Sync Service (Flask + PostgreSQL)

This project is a Flask-based backend service that:
- Periodically fetches **Task** and **Inspection** data from remote APIs
- Inserts/updates the data into **PostgreSQL** tables
- Maintains a `sync_time` log to ensure reliable incremental sync
- Supports automatic syncing every minute using a background thread

---

## 🚀 Features

- Syncs `TaskDetails` and `InspectionDetails` data from external APIs
- Supports pagination (`limit`, `offset`) with page-wise handling
- Resilient to failures: retries failed syncs in the next run
- Stores last sync timestamp in PostgreSQL
- Skips duplicates using PostgreSQL `ON CONFLICT DO UPDATE`
- Background auto-sync every 60 seconds
- API key authentication support for external APIs

---

## 🛠️ Tech Stack

- **Python 3.x**
- **Flask**
- **PostgreSQL**
- **psycopg2** for DB access
- **requests** for API calls
- **pytz** for timezone handling

---

## 🔧 Configuration

Configuration is set in the script directly:

```python
PORT = 5000
CRON_TIME_IN_SECONDS = 60  # sync every 1 minute

WEB_API_TASK = "http://localhost:54324/api/TaskDetails/GetTaskDetailsList"
WEB_API_INSPECTION = "http://localhost:54324/api/InspectionDetails/GetInspectionDetailsList"
WEB_API_KEY_NAME = "X-API-KEY"
WEB_VALID_API_KEY = "your_api_key_here"

PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5433,
    "dbname": "LT_sync_data",
    "user": "postgres",
    "password": "postgres"
}
