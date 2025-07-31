import pyodbc
import requests
import logging
import time
import My_PAT_SaaS_DWH_Tests # type: ignore
from datetime import datetime
import websocket

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load sensitive credentials
USER_PAT = My_PAT_SaaS_DWH_Tests.MY_PAT
SYS_PAT = "exa_pat_c4CqxtbypBarvT8rUAGrDwOHscwTxW1Ux4KJaPdB1aD6gE"

if not SYS_PAT:
    logging.error("Missing credentials in environment variables.")
    raise ValueError("Missing credentials in environment variables.")

# Connection details (you already have these)
EXASOL_CONNECTION_PARAMS = {
    'dsn': My_PAT_SaaS_DWH_Tests.exa_dsn,
    'user': 'farkhod_giyasov',
    'password': SYS_PAT
    #"superconnection": "Y"
}

# Establish initial connection
def connect_to_db():
    """Connect to the Exasol database."""
    try:
        conn = pyodbc.connect(
            "DRIVER={EXASolution Driver};"
            f"EXAHOST={EXASOL_CONNECTION_PARAMS['dsn']};"
            f"UID={EXASOL_CONNECTION_PARAMS['user']};"
            f"PWD={EXASOL_CONNECTION_PARAMS['password']};"
          #  f"SUPERCONNECTION={EXASOL_CONNECTION_PARAMS['superconnection']};"
        )
        logging.info("Successfully connected to the database.")
        return conn
    except pyodbc.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        exit()

# Query to fetch session data
query = """
WITH SESSION_START_TIME AS (
SELECT SESSION_ID, MAX (START_TIME) as START_TIME
FROM EXA_DBA_AUDIT_SQL
GROUP BY SESSION_ID),

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
SESSION_WORKLOADS.SESSION_ID, 
SESSION_WORKLOADS.STATUS, 
SESSION_WORKLOADS.TEMP_DB_RAM,
START_TIME,
DB_RAM
        FROM EXA_CLUSTERS 
        LEFT JOIN SESSION_WORKLOADS ON EXA_CLUSTERS.CLUSTER_NAME = SESSION_WORKLOADS.CLUSTER_NAME
        LEFT JOIN SESSION_START_TIME ON SESSION_WORKLOADS.SESSION_ID = SESSION_START_TIME.SESSION_ID
        ORDER BY 6 desc;
"""

# API Details
account_id = My_PAT_SaaS_DWH_Tests.account_id
db_id = My_PAT_SaaS_DWH_Tests.db_id
list_clusters_url = f"https://cloud.exasol.com/api/v1/accounts/{account_id}/databases/{db_id}/clusters"
headers = {"Authorization": f"Bearer {SYS_PAT}"}

# Track the last started cluster
last_started_cluster = None

def is_connection_active(conn):
    """Check if the Exasol connection is active."""
    try:
        conn.execute('SELECT 1')  # Run a simple query to check the connection
        return True
    except (pyodbc.Error, websocket._exceptions.WebSocketConnectionClosedException) as e:
        logging.error(f"Connection error: {e}")
        return False

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

# Function to count queued sessions
def count_queued_sessions(sessions):
    """Counts the number of queued sessions."""
    return sum(1 for session in sessions if session[2] == "QUEUED")

# Function to get clusters from the API
def fetch_clusters():
    """Fetch clusters from the API."""
    try:
        logging.debug(f"Fetching clusters from API: {list_clusters_url}\n")
        
        response = requests.get(list_clusters_url, headers=headers)
        if response.status_code == 200:
            clusters = response.json()
            logging.debug(f"Successfully fetched {len(clusters)} clusters.")
            return clusters
        else:
            logging.error(f"Failed to retrieve clusters: {response.status_code}")
            return None
    except requests.RequestException as e:
        logging.error(f"API request to retrieve clusters failed: {e}")
        return None

def get_active_session_slots(clusters):
    total_slots = 0

    for cluster in clusters:
        if cluster.get('status', '').lower() == 'running':  # Check if the cluster is running
            total_slots += 100  # Every running cluster has 100 slots

    return total_slots

# Function to get the first stopped cluster
def get_first_stopped_cluster(clusters):
    """Return the first stopped cluster from the cluster list, excluding the main cluster."""
    stopped_clusters = [c for c in clusters if c.get('status', '').lower() == 'stopped' and not c.get('mainCluster', False)]
    stopped_clusters.sort(key=lambda c: c.get('name', '').lower())  # Sort alphabetically by name
    return stopped_clusters[0] if stopped_clusters else None

# Function to start a specific cluster
def start_cluster(cluster):
    """Start a specific cluster by its ID."""
    if cluster.get('mainCluster', False):  # Skip the main cluster
        logging.info(f"Skipping the main cluster: {cluster['name']} (ID: {cluster['id']})")
        return False  # Return False to indicate that the main cluster wasn't started
    cluster_id = cluster['id']
    cluster_name = cluster['name']
    start_cluster_url = f"{list_clusters_url}/{cluster_id}/start"
    current_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    logging.info(f"The cluster: {cluster_name} (ID: {cluster_id}) will be started at: {current_time}")
    try:
        start_response = requests.put(start_cluster_url, headers=headers)
        if start_response.status_code == 204:
            return True
        else:
            logging.error(f"Failed to start cluster {cluster_name}: {start_response.status_code}")
            return False
    except requests.RequestException as e:
        logging.error(f"API request to start cluster {cluster_name} failed: {e}")
        return False

# Function to stop a specific cluster
def stop_cluster(cluster):
    """Stop a specific cluster by its ID."""
    if cluster.get('mainCluster', False):  # Skip the main cluster
        logging.info(f"Skipping the main cluster: {cluster['name']} (ID: {cluster['id']})")
        return False  # Return False to indicate that the main cluster wasn't stopped
    cluster_id = cluster['id']
    cluster_name = cluster['name']
    stop_cluster_url = f"{list_clusters_url}/{cluster_id}/stop"
    current_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    logging.info(f"Stopping the running cluster: {cluster_name} (ID: {cluster_id})")
    logging.info(f"Current time: {current_time}")
    try:
        stop_response = requests.put(stop_cluster_url, headers=headers)
        if stop_response.status_code == 204:
            # logging.info(f"Cluster {cluster_name} will be stopped.")
            return True
        else:
            logging.error(f"Failed to stop cluster {cluster_name}: {stop_response.status_code}")
            return False
    except requests.RequestException as e:
        logging.error(f"API request to stop cluster {cluster_name} failed: {e}")
        return False

# Function to check cluster stability
def wait_for_cluster_stability():
    """Wait until no clusters are in 'starting', 'stopping', 'scaling', or 'error' status."""
    while True:
        response = requests.get(list_clusters_url, headers=headers)
        if response.status_code == 200:
            clusters = response.json()
            # Filter clusters that are still in transitional states
            unstable_clusters = [
                c for c in clusters 
                if c.get('status', '').lower() in ['starting', 'stopping', 'scaling', 'toscale', 'tostart', 'tostop']
            ]
            if not unstable_clusters:
                logging.info("All clusters are stable")
                current_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
                logging.info(f"Current time: {current_time}")
                return True
            else:
                logging.info(f"Waiting for clusters to stabilize. Currently unstable clusters: {[(c.get('name', 'Unknown'), c.get('status', 'Unknown')) for c in unstable_clusters]}")
        else:
            logging.error(f"Failed to retrieve clusters: {response.status_code}")
        
        time.sleep(60)  # Wait 120 seconds before retrying

# Function to check if clusters are in a transitional state
def is_any_cluster_in_transitional_state(clusters):
    """Check if any cluster is in a transitional state (starting, stopping, scaling, etc.)."""
    transitional_states = ['starting', 'stopping', 'scaling', 'toscale', 'tostart', 'tostop']
    return any(c.get('status', '').lower() in transitional_states for c in clusters)

threshold_exceeded_time = None  # Timer for sustained high TEMP_DB_RAM
cluster_readiness_end_time = None  # Timer for 30-minute after a cluster start

# Initial connection when the script starts
conn = connect_to_db()

# Main logic to monitor, start, and stop clusters
def monitor_and_manage_clusters():
    """Continuously monitor and manage clusters based on TEMP_DB_RAM."""
    global last_started_cluster, threshold_exceeded_time, cluster_readiness_end_time, conn  

    while True:
        clusters = fetch_clusters()
        
        if cluster_readiness_end_time and time.time() < cluster_readiness_end_time:
            # Only log the message once when the warm-up period starts
            if not hasattr(monitor_and_manage_clusters, "warm_up_logged"):
                logging.info("The worker cluster has started. Waiting for 5 minutes for warm-up before resuming monitoring.")
                monitor_and_manage_clusters.warm_up_logged = True  # Set a flag to prevent logging again

            # Sleep for the remaining time
            time.sleep(cluster_readiness_end_time - time.time())  

            logging.info("The worker cluster has warmed up. Now we proceed with the monitoring.")

            # Reset the flag after warm-up is over, so it's ready for the next warm-up
            delattr(monitor_and_manage_clusters, "warm_up_logged")  

        # Proceed with re-checking after the warm-up period
        # logging.info("Getting ready for re-checking...\n")
        # time.sleep(5)
        
        if not clusters:
            logging.error("Failed to fetch clusters. Exiting...")
            return  

        response = requests.get(list_clusters_url, headers=headers)
        if response.status_code == 200:
            clusters = response.json()
            running_clusters = sum(1 for cluster in clusters if cluster.get('status', '').lower() == 'running')
            
            # Log total sessions moved with current time
            current_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            logging.info(f"Current time: {current_time}")

            logging.info(f"There are {running_clusters} running clusters out of {len(clusters)}.")

            # Check for unstable clusters
            unstable_clusters = [
                c for c in clusters 
                if c.get('status', '').lower() in ['starting', 'stopping', 'scaling', 'toscale', 'tostart', 'tostop']
            ]
            if unstable_clusters:
                logging.info(f"Unstable clusters detected: {[(c.get('name', 'Unknown'), c.get('status', 'Unknown')) for c in unstable_clusters]}")
                if wait_for_cluster_stability():
                    logging.info("Proceeding with monitoring and management.")
                else:
                    logging.error("Cluster stability check failed. Retrying...")
                    continue  # Skip this iteration if stability check fails

            cluster_status = {}
            cluster_ids = {}
            for cluster in clusters:
                cluster_id = "MAIN" if cluster.get('mainCluster', False) else cluster.get('id', 'Unknown')
                status = cluster.get('status', 'Unknown')
                cluster_name = cluster.get('name', 'Unknown')
                cluster_status[cluster_id] = status
                cluster_ids[cluster_id] = cluster_name  

            # Ensure the connection is active before proceeding
            if conn is None or not is_connection_active(conn):
                logging.error("Database connection is not active. Reconnecting...")
                try:
                    conn = connect_to_db()  # Reconnect to the database if needed
                    logging.info("Database connection is re-established.")
                except Exception as e:
                    logging.error(f"Failed to reconnect to the database: {e}")
                    time.sleep(5)  # Wait and retry if reconnection fails
                    continue  # Skip the rest of the loop if reconnection fails        

            try:
                cursor = conn.cursor()
                cursor.execute(query)
                sessions = cursor.fetchall()
                logging.info(f"Fetched {len(sessions)} sessions from the database.\n")
                
            except pyodbc.Error as e:
                logging.error(f"Failed to execute query: {e}")
                sessions = []
                continue  

            if sessions:
                cluster_loads = calculate_cluster_loads(sessions, cluster_ids)

                for cluster_id in cluster_ids:
                    status = cluster_status.get(cluster_id, 'Unknown')
                    if cluster_id in cluster_loads:
                        data = cluster_loads[cluster_id]
                        if status.lower() == "running":
                            logging.info(f"Cluster Name: {data['cluster_name_from_api']}")
                            logging.info(f"Cluster ID: {cluster_id}")
                            logging.info(f"Cluster Status: {status}")
                            logging.info(f"Sessions: {data['session_count']}")
                            logging.info(f"TEMP_DB_RAM: {data['temp_dbram']}")
                            logging.info(f"DB_RAM: {data['db_ram']}")
                            logging.info(f"Number of queued sessions is {count_queued_sessions(sessions)}\n")
                    #else:
                    #    logging.info(f"Cluster Name: {cluster_ids[cluster_id]}")
                    #    logging.info(f"Cluster ID: {cluster_id}")
                    #    logging.info(f"Cluster Status: {status}")
                    #    logging.info("No session data\n")                        

                # Calculate total_temp_dbram and total_db_ram from session data
                total_temp_dbram = 0.0
                total_db_ram = 0
                added_clusters = set()
                for session in sessions:
                    try:
                        cluster_id = session[0]  # Cluster ID

                        # Handle None or empty string as 0.0 for temp_db_ram
                        raw_temp_db_ram = session[3]
                        temp_db_ram = float(raw_temp_db_ram) if raw_temp_db_ram not in (None, "", "NULL") else 0.0

                        # Handle None safely for db_ram
                        raw_db_ram = session[5]
                        db_ram = int(raw_db_ram) if raw_db_ram not in (None, "", "NULL") else 0

                    except (ValueError, IndexError) as e:
                        logging.error(f"Invalid session data {session}: {e}")
                        continue

                    total_temp_dbram += temp_db_ram

                    if cluster_id not in added_clusters:
                        total_db_ram += db_ram
                        added_clusters.add(cluster_id)

                logging.info("METRICS:")
                logging.info(f"Total Temp DB RAM: {round(total_temp_dbram,1)}")
                logging.info(f"Total DB RAM: {total_db_ram}")

                number_of_sessions = len(sessions)
                logging.info(f"Active Sessions: {number_of_sessions}\n")
                # logging.info(f"Checking thresholds with Total DB RAM: {round(total_db_ram,1)}; Temp DB RAM: {round(total_temp_dbram,1)}; Sessions: {number_of_sessions}")
                
                if total_temp_dbram is not None:
                    upper_threshold = 0.5 * total_db_ram
                    lower_threshold = 0.2 * (total_db_ram - float(data['db_ram']))
                    upper_threshold_sessions = 0.5 * get_active_session_slots(clusters)
                    lower_threshold_sessions = 0.3 * (get_active_session_slots(clusters)-100)

                    logging.info("THRESHOLDS:")
                    logging.info(f"Temp DB RAM Upper Threshold: {upper_threshold}")
                    logging.info(f"Temp DB RAM Lower Threshold: {lower_threshold}")
                    logging.info(f"Active Sessions Upper Threshold: {upper_threshold_sessions}")
                    logging.info(f"Active Sessions Lower Threshold: {lower_threshold_sessions}\n")

                    if total_temp_dbram > upper_threshold or number_of_sessions > upper_threshold_sessions:
                        if total_temp_dbram > upper_threshold and number_of_sessions < upper_threshold_sessions:
                            logging.info(f"Total TEMP_DB_RAM ({round(total_temp_dbram,1)}) is above the upper threshold ({upper_threshold}).")
                        elif number_of_sessions > upper_threshold_sessions and total_temp_dbram < upper_threshold:
                            logging.info(f"Number of sessions ({number_of_sessions}) is above the upper threshold for sessions ({upper_threshold_sessions}).")
                        else:
                            logging.info(f"Total TEMP_DB_RAM and Number of sessions are above the thresholds.")

                        if threshold_exceeded_time is None:
                            threshold_exceeded_time = time.time()
                            logging.info("Threshold exceeded. Timer of 60 seconds starts now.")
                        else:
                            elapsed_time = time.time() - threshold_exceeded_time
                            logging.info(f"Threshold exceeded for {elapsed_time:.2f} seconds.")

                            if elapsed_time >= (1 * 60):
                                logging.info("Initiating cluster start process...")
                                first_stopped_cluster = get_first_stopped_cluster(clusters)
                                if first_stopped_cluster:
                                    if start_cluster(first_stopped_cluster):
                                        last_started_cluster = first_stopped_cluster
                                        threshold_exceeded_time = None  
                                        # logging.info("Resetting threshold_exceeded_time after cluster start.")

                                        if wait_for_cluster_stability():
                                            cluster_readiness_end_time = time.time() + (10 * 60)  # Set warm-up period

                                            logging.info("The cluster is stable. Wait for 10 minutes for the cluster to warm up. No action will be taken during this time.")

                                            # Wait for the warm-up period **here** instead of later in the loop
                                            time.sleep(10 * 60)  

                                            logging.info("The worker cluster has warmed up. Now we proceed with the monitoring.")
                                        else:
                                            logging.error("Cluster stability check failed after starting.")
                                            continue
                    elif total_temp_dbram < lower_threshold and number_of_sessions < lower_threshold_sessions and count_queued_sessions(sessions) == 0:
                        logging.info(f"Total TEMP_DB_RAM {round(total_temp_dbram,1)} and number of sessions {number_of_sessions} are below the threshold.")

                        running_clusters = [c for c in clusters if c['status'].lower() == 'running' and not c.get('mainCluster', False)]
                        if not running_clusters:
                            logging.info("No running worker cluster was found. Skipping cluster stop operation.")
                        else:
                            running_clusters.sort(key=lambda c: c.get('name', ''), reverse=True)
                            cluster_to_stop = running_clusters[0]
                            if stop_cluster(cluster_to_stop):
                                last_started_cluster = None
                                cluster_readiness_end_time = None  # This prevents unnecessary waiting
                                # logging.info("Cluster stopped. Waiting for stabilization...")
                                if not wait_for_cluster_stability():
                                    logging.error("Cluster stability check failed after stopping. Retrying...")
                                    continue
                    else:
                        logging.info(f"TEMP_DB_RAM and the number of sessions are within thresholds. No action required.")
                else:
                    logging.warning("Failed to calculate TEMP_DB_RAM. Skipping this iteration.")
            else:
                logging.warning("No sessions fetched. Skipping this iteration.")
        else:
            logging.error(f"Failed to retrieve clusters: {response.status_code} - {response.text}")

        logging.info("Getting ready for re-checking...\n")
        time.sleep(5)

if __name__ == "__main__":
    monitor_and_manage_clusters()
