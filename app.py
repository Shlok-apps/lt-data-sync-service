from flask import Flask, jsonify, request
import os, logging, threading, time, requests
from datetime import datetime, timedelta
import pytz
import psycopg2
from psycopg2 import sql
import schedule
app = Flask(__name__)
# -------------------- Configuration --------------------
PORT = 5000

# WEB API CONIFGURATION
WEB_API_TASK = "http://localhost:54324/api/TaskDetails/GetTaskDetailsListForSync"
WEB_API_INSPECTION = "http://localhost:54324/api/InspectionDetails/GetInspectionDetailsListForSync"
WEB_API_USER = "http://localhost:54324/api/UserDetails/GetUserDetailsForSync"
WEB_API_ENTITY = "http://localhost:54324/api/EntityDetails/GetEntityDetailsForSync"
WEB_API_KEY_NAME = "X-API-KEY"
WEB_VALID_API_KEY = "d9f3abcb7f2641ac8e1c90cfa44c2f298f25a7be3341a49c882204b5638c6de2"

# LOCAL INSERT TASK/INSPECTION API CONIFGURATION
LOCAL_TASK_API = f"http://localhost:{PORT}/insertTaskDetailsBulk"
LOCAL_INSPECTION_API = f"http://localhost:{PORT}/insertInspectionDetailsBulk"
LOCAL_USER_API = f"http://localhost:{PORT}/insertUserDetailsBulk"
LOCAL_ENTITY_API = f"http://localhost:{PORT}/insertEntityDetailsBulk"

# CRON CONFIGURATION
CRON_TIME_IN_SECONDS = 60 # 60 MEANS 1 MINUTE

# -------------------- Constants --------------------
USER_DETAILS_SCHEDULES = ["00:00", "12:00", "16:31"]
ENTITY_DETAILS_SCHEDULES = ["12:00", "12:05","16:31"]

# PG ADMIN CONNECTION CONFIGURATION
PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5433,
    "dbname": "LT_sync_data",
    "user": "postgres",
    "password": "postgres"
}

# POSTGRESQL TABLE NAME CONFIGURATIONS
TASK_DETAILS_TABLE_NAME = "taskDetails"
INSPECTION_DETAILS_TABLE_NAME = "inspectionDetails"
USER_DETAILS_TABLE_NAME = "userDetails"
ENTITY_DETAILS_TABLE_NAME = "entityDetails"
SYNC_TABLE = "sync_time"

# -------------------- Logging Setup --------------------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "sync.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# -------------------- Table Creation --------------------
def create_tables():
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()

    # Check taskDetails case-sensitive
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = %s
        );
    """, (TASK_DETAILS_TABLE_NAME,))  # exact case match

    if not cur.fetchone()[0]:
        logging.info(f"Creating {TASK_DETAILS_TABLE_NAME} table...")
        cur.execute(sql.SQL("""
    CREATE TABLE {} (
        "TaskID" INTEGER PRIMARY KEY,
        "Description" TEXT,
        "Status" VARCHAR(50),
        "TaskType" VARCHAR(50),
        "CreationDate" TIMESTAMP,
        "ClosedDate" TIMESTAMP,
        "CreatedUserID" INTEGER,
        "CreatedUser" TEXT,
        "AssignedUserID" INTEGER,
        "AssignedTo" TEXT,
        "DueDate" TIMESTAMP,
        "JobsiteCode" VARCHAR(50),
        "Jobsite" TEXT,
        "ProductType" VARCHAR(50),
        "Product" TEXT,
        "Priority" TEXT,
        "Severity" TEXT
    );
""").format(sql.Identifier(TASK_DETAILS_TABLE_NAME)))
        
    # Create inspectionDetails table
    # Check if inspectionDetails table exists
    cur.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = %s
    );
""", (INSPECTION_DETAILS_TABLE_NAME,))

    if not cur.fetchone()[0]:
        logging.info(f"Creating {INSPECTION_DETAILS_TABLE_NAME} table...")
        cur.execute(sql.SQL("""
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            "ProjectId" INTEGER,
            "ProjectCode" TEXT,
            "ScheduleDate" TIMESTAMP,
            "CreationDate" TIMESTAMP,
            "ClosedDate" TIMESTAMP,
            "SectorCode" TEXT,
            "Sector" TEXT,
            "Establishment" TEXT,
            "InspectionStatus" TEXT,
            "Product" TEXT,
            "Checklist" TEXT,
            "CreatedUser" TEXT,
            "PendingWith" TEXT,
            "Score" INTEGER,
            "Result" TEXT
        );
    """).format(sql.Identifier(INSPECTION_DETAILS_TABLE_NAME)))

# Check if sync_time table exists
    cur.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = %s
    );
""", (SYNC_TABLE,))

    if not cur.fetchone()[0]:
        logging.info(f"Creating {SYNC_TABLE} table...")
        cur.execute(sql.SQL("""
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            synced_at TIMESTAMP,
            records_synced INTEGER,
            sync_type TEXT
        );
    """).format(sql.Identifier(SYNC_TABLE)))

# Create userDetails table
    cur.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = %s
    );
""", (USER_DETAILS_TABLE_NAME,))

    if not cur.fetchone()[0]:
        logging.info(f"Creating {USER_DETAILS_TABLE_NAME} table...")
        cur.execute(sql.SQL("""
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            "EmployeeId" INTEGER,
            "EmployeeName" TEXT,
            "Jobsite" TEXT,
            "IC" TEXT,
            "SBG" TEXT,
            "BU" TEXT,
            "Cluster" TEXT
        );
    """).format(sql.Identifier(USER_DETAILS_TABLE_NAME)))

# Create entityDetails table
    cur.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = %s
    );
""", (ENTITY_DETAILS_TABLE_NAME,))

    if not cur.fetchone()[0]:
        logging.info(f"Creating {ENTITY_DETAILS_TABLE_NAME} table...")
        cur.execute(sql.SQL("""
        CREATE TABLE {} (
            "ID" INTEGER PRIMARY KEY,
            "EntityName" TEXT,
            "IC" TEXT,
            "SBG" TEXT,
            "BU" TEXT,
            "Cluster" TEXT
        );
    """).format(sql.Identifier(ENTITY_DETAILS_TABLE_NAME)))


    conn.commit()
    cur.close()
    conn.close()

# -------------------- Utility Functions --------------------
def get_last_sync_time(sync_type: str):
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT synced_at FROM {SYNC_TABLE}
        WHERE sync_type = %s
        ORDER BY synced_at DESC
        LIMIT 1;
        """,
        (sync_type,)
    )
    result = cur.fetchone()
    cur.close()
    conn.close()
    if result:
        utc_time = result[0]  # this is in UTC
        ist = pytz.timezone("Asia/Kolkata")
        return utc_time.astimezone(ist)
    else:
        # fallback: current time minus 1 minute in IST
        ist = pytz.timezone("Asia/Kolkata")
        return datetime.now(ist) - timedelta(seconds=CRON_TIME_IN_SECONDS)


def insert_sync_record(timestamp, count, sync_type):
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {SYNC_TABLE} (synced_at, records_synced, sync_type)
        VALUES (%s, %s, %s);
    """, (timestamp, count, sync_type))
    conn.commit()
    cur.close()
    conn.close()

# -------------------- Fetch from SQL Server --------------------
def fetch_task_data_from_api(from_time, to_time, limit=1000, offset=0):
    api_url = WEB_API_TASK
    headers = {
        WEB_API_KEY_NAME: WEB_VALID_API_KEY
    }
    payload = {
        "limit": limit,
        "offset": offset,
        "fromTime": from_time,
        "toTime": to_time
    }
    response = requests.post(api_url, json=payload,headers=headers,timeout=360)
    response.raise_for_status()
    data = response.json()
    rows = data.get("records", [])
    if not rows:
        return [], []
    columns = list(rows[0].keys())
    row_tuples = [tuple(row.get(col) for col in columns) for row in rows]
    return columns, row_tuples

def fetch_inspection_data_from_api(from_time, to_time, limit=1000, offset=0):
    api_url = WEB_API_INSPECTION
    headers = {
        WEB_API_KEY_NAME: WEB_VALID_API_KEY
    }
    payload = {
        "limit": limit,
        "offset": offset,
        "fromTime": from_time,
        "toTime": to_time
    }
    response = requests.post(api_url, json=payload,headers=headers,timeout=360)
    response.raise_for_status()
    data = response.json()
    rows = data.get("records", [])
    if not rows:
        return [], []
    columns = list(rows[0].keys())
    row_tuples = [tuple(row.get(col) for col in columns) for row in rows]
    return columns, row_tuples


# -------------------- Insert into PostgreSQL --------------------
def insert_into_postgres(columns, rows):
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()

    insert_query = sql.SQL("""
        INSERT INTO {} ({fields})
        VALUES ({placeholders})
        ON CONFLICT ("TaskID") DO UPDATE SET
        {updates}
    """).format(
        sql.Identifier(TASK_DETAILS_TABLE_NAME),
        fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns)),
        updates=sql.SQL(', ').join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
            for col in columns if col != "TaskID"
        )
    )

    inserted_count = 0
    for row in rows:
        row_data = [None if val == '' else val for val in row]
        try:
            cur.execute(insert_query, row_data)
            inserted_count += 1
        except Exception as e:
            logging.error(f"Insert/Update failed for TaskID={row[0]}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    return inserted_count

def insert_inspection_data(columns, rows):
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()

    insert_query = sql.SQL("""
        INSERT INTO {} ({fields})
        VALUES ({placeholders})
    """).format(
        sql.Identifier(INSPECTION_DETAILS_TABLE_NAME),
        fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )

    count = 0
    for row in rows:
        row_data = [None if val == '' else val for val in row]
        try:
            cur.execute(insert_query, row_data)
            count += 1
        except Exception as e:
            logging.error(f"Insert failed for ProjectId={row[0]}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    return count

def insert_user_details(columns, rows):
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()

    insert_query = sql.SQL("""
        INSERT INTO {} ({fields})
        VALUES ({placeholders})
    """).format(
        sql.Identifier(USER_DETAILS_TABLE_NAME),
        fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )

    count = 0
    for row in rows:
        try:
            cur.execute(insert_query, row)
            count += 1
        except Exception as e:
            logging.error(f"Insert failed for row={row}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    return count

def insert_entity_details(columns, rows):
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()

    insert_query = sql.SQL("""
        INSERT INTO {} ({fields})
        VALUES ({placeholders})
        ON CONFLICT ("ID") DO UPDATE SET
        {updates}
    """).format(
        sql.Identifier(ENTITY_DETAILS_TABLE_NAME),
        fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns)),
        updates=sql.SQL(', ').join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
            for col in columns if col != "ID"
        )
    )

    count = 0
    for row in rows:
        try:
            cur.execute(insert_query, row)
            count += 1
        except Exception as e:
            logging.error(f"Insert failed for Entity ID={row[0]}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    return count


def get_next_hour_mark(dt: datetime):
    """Round up to next hour (IST)."""
    return (dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))

def get_current_hour_mark():
    """Return the current hour mark (IST) with minute, second, microsecond set to 0."""
    return datetime.now().replace(minute=0, second=0, microsecond=0)
# -------------------- API Endpoint --------------------

@app.route('/insertTaskDetailsBulk', methods=['POST'])
def sync_task_data():
    try:
        create_tables()  # Ensure tables exist

        from_time = get_last_sync_time("task")
        to_time = datetime.now(pytz.timezone("Asia/Kolkata")).replace(tzinfo=None)

        from_str = from_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        to_str = to_time.strftime("%Y-%m-%d %H:%M:%S")

        all_rows = []
        columns = []
        limit = 100
        page = 0
        total = None

        while True:
            payload = {
                "limit": limit,
                "offset": page,
                "fromTime": from_str,
                "toTime": to_str
            }
            headers = {
                WEB_API_KEY_NAME: WEB_VALID_API_KEY
            }
            logging.info(f"📄 {TASK_DETAILS_TABLE_NAME} Fetching page {page + 1} with limit {limit}...")
            try:
                response = requests.post(WEB_API_TASK, json=payload, headers=headers,timeout=360)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                logging.error(f"Page {page} fetch failed: {e}")
                return jsonify({
                    "status": "error",
                    "message": f"Failed to fetch page {page}: {str(e)}"
                }), 500

            records = data.get("Data", [])
            total = data.get("TotalRecordCount", 0) if total is None else total

            if not records:
                break

            if not columns:
                columns = list(records[0].keys())

            row_tuples = [tuple(row.get(col) for col in columns) for row in records]
            all_rows.extend(row_tuples)

            page += 1
            if len(all_rows) >= total:
                break

        # All pages fetched successfully – insert into DB
        count = insert_into_postgres(columns, all_rows)

        if count > 0:
            next_hour = get_next_hour_mark(to_time)
            insert_sync_record(next_hour, count, "task")
            logging.info(f"✅ Inserted sync_time record at {to_time} with {count} records.")
        else:
            logging.info("ℹ️ No new records synced. Skipping sync_time insertion.")

        logging.info(f"✅ TASK SYNC DONE | from: {from_str} | to: {to_str} | count: {count}")

        return jsonify({
            "status": "success",
            "inserted": count,
            "fromTime": from_str,
            "toTime": to_str,
            "message": f"Synced {count} records."
        }), 200

    except Exception as e:
        logging.error(f"❌ SYNC FAILED: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
        
@app.route('/insertInspectionDetailsBulk', methods=['POST'])
def sync_inspection_data():
    try:
        create_tables()

        from_time = get_last_sync_time("inspection")
        to_time = datetime.now(pytz.timezone("Asia/Kolkata")).replace(tzinfo=None)

        from_str = from_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        to_str = to_time.strftime("%Y-%m-%d %H:%M:%S")

        all_rows = []
        columns = []
        limit = 100
        page = 0
        total = None

        while True:
            payload = {
                "limit": limit,
                "offset": page,
                "fromTime": from_str,
                "toTime": to_str
            }
            headers = {
                WEB_API_KEY_NAME: WEB_VALID_API_KEY
            }
            logging.info(f"📄 {INSPECTION_DETAILS_TABLE_NAME} Fetching page {page + 1} with limit {limit}...")
            try:
                response = requests.post(WEB_API_INSPECTION, json=payload, headers=headers,timeout=360)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                logging.error(f"Page {page} fetch failed: {e}")
                return jsonify({
                    "status": "error",
                    "message": f"Failed to fetch page {page}: {str(e)}"
                }), 500

            records = data.get("Data", [])
            total = data.get("TotalRecordCount", 0) if total is None else total

            if not records:
                break

            if not columns:
                columns = list(records[0].keys())

            row_tuples = [tuple(row.get(col) for col in columns) for row in records]
            all_rows.extend(row_tuples)

            page += 1
            if len(all_rows) >= total:
                break

        # Insert only if all pages succeeded
        count = insert_inspection_data(columns, all_rows)

        if count > 0:
            next_hour = get_next_hour_mark(to_time)
            insert_sync_record(next_hour, count, "inspection")
            logging.info(f"✅ Inserted sync_time record at {to_time} with {count} records.")
        else:
            logging.info("ℹ️ No new inspection records synced. Skipping sync_time insertion.")

        logging.info(f"INSPECTION SYNC DONE | from: {from_str} | to: {to_str} | count: {count}")

        return jsonify({
            "status": "success",
            "inserted": count,
            "fromTime": from_str,
            "toTime": to_str
        }), 200

    except Exception as e:
        logging.error(f"❌ Inspection Sync Failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/insertUserDetailsBulk', methods=['POST'])
def insert_user_details_bulk():
    try:
        create_tables()

        all_rows = []
        columns = []
        limit = 1000
        page = 0
        total = None

        while True:
            payload = {"limit": limit, "offset": page}
            headers = {WEB_API_KEY_NAME: WEB_VALID_API_KEY}
            logging.info(f"📄 {USER_DETAILS_TABLE_NAME} Fetching page {page + 1} with limit {limit}...")
            try:
                response = requests.post(WEB_API_USER, json=payload, headers=headers,timeout=360)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                logging.error(f"Page {page} fetch failed: {e}")
                return jsonify({
                    "status": "error",
                    "message": f"Failed to fetch page {page}: {str(e)}"
                }), 500

            records = data.get("Data", [])
            total = data.get("TotalRecordCount", 0) if total is None else total

            if not records:
                break

            if not columns:
                columns = list(records[0].keys())

            row_tuples = [tuple(row.get(col) for col in columns) for row in records]
            all_rows.extend(row_tuples)

            page += 1
            if len(all_rows) >= total:
                break

        # Insert only if all pages succeeded
        count = insert_user_details(columns, all_rows)
        
        if count > 0:
            current_hour = get_current_hour_mark()
            insert_sync_record(current_hour, count, "user")
            logging.info(f"✅ Inserted sync_time record at {current_hour} with {count} records.")
        else:
            logging.info("ℹ️ No new user records synced. Skipping sync_time insertion.")
            
            
        logging.info(f"✅ USER SYNC DONE | from: { datetime.now().strftime('%I:%M %p')}  | count: {count}")
        return jsonify({
            "status": "success",
            "inserted": count,
            "message": f"Inserted {count} user records"
        }), 200

    except Exception as e:
        logging.error(f"UserDetails Sync Failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/insertEntityDetailsBulk', methods=['POST'])
def insert_entity_details_bulk():
    try:
        create_tables()

        all_rows = []
        columns = []
        limit = 100  # recommended value for performance
        page = 0
        total = None

        while True:
            payload = {"limit": limit, "offset": page}
            headers = {WEB_API_KEY_NAME: WEB_VALID_API_KEY}
            logging.info(f"📄 {ENTITY_DETAILS_TABLE_NAME} Fetching page {page + 1} with limit {limit}...")
            try:
                response = requests.post(WEB_API_ENTITY, json=payload, headers=headers,timeout=360)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                logging.error(f"Page {page} fetch failed: {e}")
                return jsonify({
                    "status": "error",
                    "message": f"Failed to fetch page {page}: {str(e)}"
                }), 500

            records = data.get("Data", [])
            total = data.get("TotalRecordCount", 0) if total is None else total

            if not records:
                break

            if not columns:
                columns = list(records[0].keys())

            row_tuples = [tuple(row.get(col) for col in columns) for row in records]
            all_rows.extend(row_tuples)

            page += 1
            if (page * limit) >= total:
                break

        # Only insert if all pages fetched successfully
        count = insert_entity_details(columns, all_rows)
        if count > 0:
            current_hour = get_current_hour_mark()
            insert_sync_record(current_hour, count, "entity")
            logging.info(f"✅ Inserted sync_time record at {current_hour} with {count} records.")
        else:
            logging.info("ℹ️ No new entity records synced. Skipping sync_time insertion.")
            
        logging.info(f"✅ ENTITY SYNC DONE | from: { datetime.now().strftime('%I:%M %p')}  | count: {count}")
        
        return jsonify({
            "status": "success",
            "inserted": count,
            "message": f"Inserted {count} entity records"
        }), 200

    except Exception as e:
        logging.error(f"EntityDetails Sync Failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
        
def truncate_table(table_name):
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name)))
        conn.commit()
        logging.info(f"✅ Cleared existing records from {table_name}")
    except Exception as e:
        logging.error(f"❌ Failed to truncate {table_name}: {e}")
    finally:
        cur.close()
        conn.close()

def call_user_details_api():
    logging.info(f"🔁 [SCHEDULED] Triggered UserDetails API at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        truncate_table(USER_DETAILS_TABLE_NAME)
    except Exception as e:
        logging.error(f"❌ Failed to truncate {USER_DETAILS_TABLE_NAME}: {e}")
        return

    try:
        response = requests.post(LOCAL_USER_API, timeout=3600)
        logging.info(f"✅ UserDetails API called. Status: {response.status_code}")
    except Exception as e:
        logging.error(f"❌ Error calling UserDetails API: {e}")


def call_entity_details_api():
    logging.info(f"🔁 [SCHEDULED] Triggered EntityDetails API at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        truncate_table(ENTITY_DETAILS_TABLE_NAME)
    except Exception as e:
        logging.error(f"❌ Failed to truncate {ENTITY_DETAILS_TABLE_NAME}: {e}")
        return
    
    try:
        response = requests.post(LOCAL_ENTITY_API, timeout=3600)
        logging.info(f"✅ EntityDetails API called. Status: {response.status_code}")
    except Exception as e:
        logging.error(f"❌ Error calling EntityDetails API: {e}")

        
def run_in_thread(job_func):
    threading.Thread(target=job_func).start()

def schedule_sync_jobs():
    # Schedule User Details sync jobs
    for time_str in USER_DETAILS_SCHEDULES:
        schedule.every().day.at(time_str).do(run_in_thread, call_user_details_api)

    # Schedule Entity Details sync jobs
    for time_str in ENTITY_DETAILS_SCHEDULES:
        schedule.every().day.at(time_str).do(run_in_thread, call_entity_details_api)

    while True:
        schedule.run_pending()
        time.sleep(60)
# -------------------- Background Thread for Auto Sync --------------------
def auto_sync_loop():
    time.sleep(5)  # Wait for Flask to start
    while True:
        try:
            res = requests.post(LOCAL_TASK_API, timeout=3600)
            logging.info(f"Auto sync triggered. Status: {res.status_code}")
               # Call /insertInspectionDetailsBulk
            inspection_res = requests.post(LOCAL_INSPECTION_API, timeout=3600)
            logging.info(f"[Auto Sync] {INSPECTION_DETAILS_TABLE_NAME} triggered - Status: {inspection_res.status_code}")
        except Exception as e:
            logging.error(f"Auto sync error: {e}")
        time.sleep(CRON_TIME_IN_SECONDS)

# -------------------- Start Server --------------------

def start_flask_app():
    app.run(port=PORT, debug=True, use_reloader=False)  # Disable reloader to avoid duplicate threads

if __name__ == '__main__':
    # Only run threads if this is the main process
    threading.Thread(target=auto_sync_loop, daemon=True).start()
    threading.Thread(target=schedule_sync_jobs, daemon=True).start()
    threading.Thread(target=start_flask_app, daemon=False).start()  # Flask runs i
