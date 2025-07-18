# 🔁 Auto Data Sync Service (Flask + PostgreSQL)

This project is a Flask-based backend service that:

- Periodically fetches **Task**, **Inspection**, **User**, and **Entity** data from external APIs
- Inserts or updates the data into corresponding **PostgreSQL** tables
- Maintains a `sync_time` table to track and manage last sync timestamps
- Supports **automatic sync every minute**
- Supports **preload sync** (initial bulk load)
- Uses **background scheduling** for regular sync
- Offers configurable sync interval types like hourly, daily, monthly, and yearly

---

## 🚀 Features

- ⏱️ **Auto Sync Every Minute** (via `schedule` library)
- 🔁 **Incremental sync** using `fromTime` and `toTime` timestamps
- 🔃 **Preload mode** to backfill historical data using date intervals
- 📦 **Bulk insert** endpoints for Task, Inspection, User, and Entity data
- ✅ Skips duplicate syncs with PostgreSQL `ON CONFLICT DO UPDATE`
- 🔐 **API key verification** for secure access
- 🧠 Handles data in batches using `limit` and `offset`
- 🗃️ Logs every sync event into the `sync_time` table

---

## 🛠️ Tech Stack

- **Python 3.x**
- **Flask**
- **PostgreSQL**
- **psycopg2** for PostgreSQL DB operations
- **requests** for external API access
- **schedule** for background job scheduling
- **pytz** for timezone-aware datetime handling

---

## 🔧 Configuration

Configuration is defined inside the script:

```python
PORT = 5000
CRON_TIME_IN_SECONDS = 60  # 60 seconds = 1 minute

# External Source APIs
WEB_API_TASK = "http://localhost:54324/api/TaskDetails/GetTaskDetailsListForSync"
WEB_API_INSPECTION = "http://localhost:54324/api/InspectionDetails/GetInspectionDetailsListForSync"
WEB_API_USER = "http://localhost:54324/api/UserDetails/GetUserDetailsForSync"
WEB_API_ENTITY = "http://localhost:54324/api/EntityDetails/GetEntityDetailsForSync"

# Local Bulk Insert APIs
LOCAL_TASK_API = f"http://localhost:{PORT}/insertTaskDetailsBulk"
LOCAL_INSPECTION_API = f"http://localhost:{PORT}/insertInspectionDetailsBulk"
LOCAL_PRELOAD_TASK_API = f"http://localhost:{PORT}/insertPreLoadTaskDetailsBulk"
LOCAL_PRELOAD_INSPECTION_API = f"http://localhost:{PORT}/insertPreLoadInspectionDetailsBulk"
LOCAL_USER_API = f"http://localhost:{PORT}/insertUserDetailsBulk"
LOCAL_ENTITY_API = f"http://localhost:{PORT}/insertEntityDetailsBulk"

# API Key Config
WEB_API_KEY_NAME = "X-API-KEY"
WEB_VALID_API_KEY = "d9f3abcb7f2641ac8e1c90cfa44c2f298f25a7be3341a49c882204b5638c6de2"

# PostgreSQL DB
PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5433,
    "dbname": "LT_sync_data",
    "user": "postgres",
    "password": "postgres"
}

# Sync Table
SYNC_TABLE = "sync_time"

# Sync Time Intervals
DEFAULT_INTERVAL_TYPE = "m"  # 's' (sec), 'm' (min/month), 'h', 'd', 'y'
USER_DETAILS_SCHEDULES = ["00:00", "12:00", "19:14"]
ENTITY_DETAILS_SCHEDULES = ["12:00", "12:00", "19:14"]

# Preload Configs (used to initialize first-time historical sync)
PRELOAD_CONFIGS = [
    ('2024-01-01 00:00:00', 0, 'task', 'mon', True),
    ('2024-01-01 00:00:00', 0, 'inspection', 'y', True)
]


# INTALL DEPENDICES
pip install -r requirements.txt
