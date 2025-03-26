import pyodbc
import requests
import logging
import My_PAT_SaaS_DWH_Tests # type: ignore
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load sensitive credentials
USER_PAT = My_PAT_SaaS_DWH_Tests.MY_PAT
SYS_PAT = My_PAT_SaaS_DWH_Tests.SYS_PAT

if not SYS_PAT:
    logging.error("Missing credentials in environment variables.")
    raise ValueError("Missing credentials in environment variables.")

# Connection details (you already have these)
EXASOL_CONNECTION_PARAMS = {
    'dsn': 'zmrfa5r5hzcwjazdshh6mj7ati.clusters-staging.exasol.com',
    'user': 'sys',
    'password': SYS_PAT,
    "superconnection": "Y"
}

# Establish initial connection
def connect_to_db():
    """Connect to the Exasol database without SUPERCONNECTION parameter."""
    try:
        logging.info("Attempting to connect to Exasol...")
        
        conn = pyodbc.connect(
            "DRIVER={EXASolution Driver};"
            f"EXAHOST={EXASOL_CONNECTION_PARAMS['dsn']};"
            f"UID={EXASOL_CONNECTION_PARAMS['user']};"
            f"PWD={EXASOL_CONNECTION_PARAMS['password']};"
            f"SUPERCONNECTION={EXASOL_CONNECTION_PARAMS['superconnection']};"
        )
        
        logging.info("Successfully connected to the database.")
        return conn
    except pyodbc.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        return None
# Query to fetch session data
query = """
WITH SESSION_START_TIME AS (
SELECT CLUSTER_NAME, SESSION_ID, MAX (START_TIME) as START_TIME
FROM EXA_DBA_AUDIT_SQL
GROUP BY CLUSTER_NAME, SESSION_ID
order by start_time desc
),

SESSION_WORKLOADS AS (
SELECT 
CLUSTER_NAME, 
SESSION_ID, 
STATUS, 
TEMP_DB_RAM
FROM EXA_DBA_SESSIONS
WHERE TEMP_DB_RAM > 0)

SELECT 
EXA_CLUSTERS.CLUSTER_NAME, 
SESSION_START_TIME.CLUSTER_NAME,
SESSION_WORKLOADS.SESSION_ID, 
SESSION_WORKLOADS.STATUS, 
SESSION_WORKLOADS.TEMP_DB_RAM,
START_TIME,
DB_RAM
        FROM EXA_CLUSTERS 
        LEFT JOIN SESSION_START_TIME ON EXA_CLUSTERS.CLUSTER_NAME = SESSION_START_TIME.CLUSTER_NAME
        JOIN SESSION_WORKLOADS ON SESSION_WORKLOADS.SESSION_ID = SESSION_START_TIME.SESSION_ID
        ORDER BY 6 desc;
"""

# API Details
account_id = My_PAT_SaaS_DWH_Tests.account_id
db_id = My_PAT_SaaS_DWH_Tests.db_id
list_clusters_url = f"https://cloud-staging.exasol.com/api/v1/accounts/{account_id}/databases/{db_id}/clusters"
headers = {"Authorization": f"Bearer {USER_PAT}"}

# Track the last started cluster
last_started_cluster = None

# Reconnect to the database if the connection fails
def reconnect_to_db():
    """Reconnect to the Exasol database."""
    current_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')   
    logging.info("Attempting to reconnect to the database...")
    logging.info(f"Current time: {current_time}")
    try:
        conn = pyodbc.connect(
            dsn='zmrfa5r5hzcwjazdshh6mj7ati.clusters-staging.exasol.com',
            user='sys',
            password=SYS_PAT
        )
        # logging.info("Database connection re-established.")
        return conn
    except pyodbc.Error as e:
        logging.error(f"Failed to reconnect to the database: {e}")
        raise  # Re-raise the exception to handle it in the calling function

def is_connection_active(conn):
    """Check if the Exasol connection is active."""
    try:
        conn.execute('SELECT 1')  # Run a simple query to check the connection
        return True
    except pyodbc.Error as e:
        logging.error(f"Connection error: {e}")
        return False

# Initial connection when the script starts
conn = connect_to_db()

# Function to fetch clusters from API
def fetch_clusters():
    try:
        response = requests.get(list_clusters_url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to retrieve clusters: {response.status_code}")
            return None
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None


# Function to calculate cluster loads
def calculate_cluster_loads(sessions, cluster_ids):
    cluster_loads = {}
    for session in sessions:
        cluster_id = session[0]  # Use the cluster's actual ID for non-main clusters
        
        # Handle None, empty strings, and invalid values gracefully
        raw_temp_dbram = session[3]
        if raw_temp_dbram in (None, "", "NULL"):  # Treat None, empty string, and "NULL" as 0
            temp_dbram = 0.0
        else:
            try:
                temp_dbram = float(raw_temp_dbram)
            except ValueError:
                logging.error(f"Invalid session data {session}: could not convert to float")
                temp_dbram = 0.0  # Fallback to 0.0 for invalid cases
        
        db_ram = session[5]
        
        if cluster_id not in cluster_loads:
            cluster_loads[cluster_id] = {
                "session_count": 0, 
                "temp_dbram": 0.0,
                "db_ram": db_ram,
                "cluster_name_from_api": cluster_ids.get(cluster_id, "Unknown"),
            }
        
        cluster_loads[cluster_id]["session_count"] += 1
        cluster_loads[cluster_id]["temp_dbram"] += temp_dbram

    logging.debug(f"Cluster loads calculated: {cluster_loads}")
    return cluster_loads

# Function to find the least loaded running cluster
def get_least_loaded_cluster(clusters, sessions):
    # Filter running clusters from the API response
    running_clusters = [c for c in clusters if c.get('status', '').lower() == 'running']
    
    if len(running_clusters) <= 1:
        logging.info("Only one running cluster found. Skipping session movement.")
        return None, {}

    # Create a mapping of cluster ID -> cluster name
    cluster_id_to_name = {c['id']: c['name'] for c in running_clusters}

    # Now pass both `sessions` and `cluster_id_to_name` to calculate_cluster_loads
    cluster_loads = calculate_cluster_loads(sessions, cluster_id_to_name)

    # Log all running clusters and their session counts after calculating the cluster loads
    logging.info("Current cluster session loads:")
    for cluster in running_clusters:
        cluster_name = cluster['name']
        cluster_id = cluster['id'] if cluster.get('mainCluster', False) else cluster['id']
        cluster_id_Main = "MAIN" if cluster.get('mainCluster', False) else cluster['id']
        session_count = cluster_loads.get(cluster_id_Main, {}).get("session_count", 0)
        logging.info(f"  - {cluster_name} (ID: {cluster_id}): {session_count} sessions")

    # Find the least loaded cluster by comparing session counts
    least_loaded = min(running_clusters, key=lambda c: cluster_loads.get(c['id'], {}).get("session_count", float('inf')))
    
    logging.info(f"Least loaded cluster: {least_loaded['name']} (ID: {least_loaded['id']})")
    return least_loaded, cluster_id_to_name

# Function to move sessions to the least loaded cluster
def move_sessions(sessions, least_loaded_cluster, cluster_id_to_name):
    # Calculate session counts for each cluster
    cluster_session_counts = {}
    for session in sessions:
        cluster_id = session[0]
        if cluster_id not in cluster_session_counts:
            cluster_session_counts[cluster_id] = 0
        cluster_session_counts[cluster_id] += 1
    
    # Find the cluster with the fewest sessions (least connections)
    least_loaded_cluster_id = min(cluster_session_counts, key=cluster_session_counts.get)
    least_loaded_cluster_name = cluster_id_to_name.get(least_loaded_cluster_id, "Unknown")

    # Get the most loaded clusters (those that have more sessions than the least loaded one)
    most_loaded_clusters = [
        cluster_id for cluster_id, count in cluster_session_counts.items() 
        if count > cluster_session_counts[least_loaded_cluster_id]
    ]

    if not most_loaded_clusters:
        # logging.info("No clusters to move sessions from.")
        return 0  # Return 0 if no moves occur

    # Check if the difference in session count is meaningful
    # Only move sessions if the difference is greater than 1 session
    load_diff = cluster_session_counts[most_loaded_clusters[0]] - cluster_session_counts[least_loaded_cluster_id]
    if load_diff <= 1:
        logging.info("The load difference between clusters is too small. Skipping the session moving.")
        return 0  # No move because the difference is too small

    moved_count = 0  # Counter for successfully moved sessions

    # Move one session from each most loaded cluster to the least loaded cluster
    for cluster_id in most_loaded_clusters:
        sessions_in_cluster = [s for s in sessions if s[0] == cluster_id]
        
        # Move the first session in this cluster to the least loaded cluster
        session_to_move = sessions_in_cluster[0]  # First session to move
        session_id = session_to_move[1]
        
        # Prepare the move query
        move_query = f"CONTROL MOVE SESSION {session_id} TO '{least_loaded_cluster_id}';"
        
        try:
            conn.execute(move_query)
            check_query = f"SELECT session_id, cluster_name FROM EXA_ALL_SESSIONS WHERE session_id = {session_id};"
            check_result = conn.execute(check_query).fetchall()

            for row in check_result:
                if row[1] != cluster_id:  # If the session was moved
                    current_cluster_name = cluster_id_to_name.get(cluster_id, "Unknown")
                    logging.info(f"Moving session {session_id} from {current_cluster_name} (ID: {cluster_id}) "
                                 f"to {least_loaded_cluster_name} (ID: {least_loaded_cluster_id})")
                    logging.info(f"Session {session_id} is now in cluster {least_loaded_cluster_name} (ID: {row[1]})")
                    moved_count += 1  # Increment successful move counter

        except pyodbc.Error as e:
            # logging.error(f"Failed to move session {session_id} Status: {session_status}: {e}")
            continue

    return moved_count  # Return the count of moved sessions

# Continuous monitoring function
def monitor_and_manage_clusters():
    global conn
    while True:
        clusters = fetch_clusters()
        if not clusters:
            logging.error("Failed to fetch clusters.")
            continue  # Skip the rest of the loop if fetching clusters fails

        # Ensure the connection is active before proceeding
        if conn is None or not is_connection_active(conn):
            logging.error("Database connection is not active. Reconnecting...")
            try:
                conn = reconnect_to_db()  # Reconnect to the database if needed
                logging.info("Database connection is re-established.")
            except Exception as e:
                logging.error(f"Failed to reconnect to the database: {e}")
                time.sleep(5)  # Wait and retry if reconnection fails
                continue  # Skip the rest of the loop if reconnection fails    

        # Check if there is only one running cluster
        running_clusters = [c for c in clusters if c.get('status', '').lower() == 'running']
        if len(running_clusters) == 1:
            logging.info("Only one running cluster found. Skipping SQL queries and session management.")
            time.sleep(5)  # Sleep for 5 seconds before checking again
            continue  # Skip executing SQL queries and session management

        # Execute queries only if more than one running cluster is found
        check_query = f"SELECT CURRENT_SESSION;"
        check_result = conn.cursor().execute(check_query).fetchone()
        if check_result:
            session_id = check_result[0]  # Extract the first value
            logging.info(f"Current Session: {session_id}")  # No parentheses!
        else:
            logging.info("No active session found.")

        try:
            # Fetch the sessions from the database using a cursor
            cursor = conn.cursor()
            cursor.execute(query)
            sessions = cursor.fetchall()  # Fetch all results
        except pyodbc.Error as e:
            logging.error(f"Failed to execute query: {e}")
            continue  # Skip the rest of the loop if the query execution fails

        if sessions:
            least_loaded_cluster, cluster_id_to_name = get_least_loaded_cluster(clusters, sessions)
            moved_count = move_sessions(sessions, least_loaded_cluster, cluster_id_to_name)
            
            # Log total sessions moved with current time
            current_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            logging.info(f"Current time: {current_time}")
            logging.info(f"Total sessions moved in this cycle: {moved_count}\n")
        else:
            logging.info("No active sessions detected.")

# Run the script
if __name__ == "__main__":
    monitor_and_manage_clusters()

# this version is the last one for moving sessions