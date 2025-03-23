import pyodbc
from datetime import datetime, timedelta, timezone
import time
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Source database connection details (to fetch data)
source_db_host = "carlitodatabase.database.windows.net"
source_db_port = "1433"
source_db_name = "DataStorage"
source_db_user = "admin-owner"
source_db_password = "carlito99!!"

# Target database connection details (to insert data)
target_db_host = "mqttmomentum.database.windows.net"
target_db_name = "mqttmomentum"
target_db_user = "amilath"
target_db_password = "Atth617QAQA"

# Construct connection strings
source_conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={source_db_host},{source_db_port};"
    f"DATABASE={source_db_name};"
    f"UID={source_db_user};"
    f"PWD={source_db_password};"
)

target_conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={target_db_host};"
    f"DATABASE={target_db_name};"
    f"UID={target_db_user};"
    f"PWD={target_db_password};"
)

# Process name (used to identify the process in the metadata table)
process_name = "mqtt_raw_data_processor"

# Function to parse the data field
def parse_data(data):
    parsed_data = {}
    try:
        # Extract total energy consumed (1-0:1.8.0)
        match = re.search(r"1-0:1\.8\.0\((\d+\.\d+)\*kWh\)", data)
        if match:
            parsed_data["total_energy_consumed"] = float(match.group(1))
        else:
            parsed_data["total_energy_consumed"] = None  # Handle missing data

        # Extract current power consumption (1-0:1.7.0)
        match = re.search(r"1-0:1\.7\.0\((\d+\.\d+)\*kW\)", data)
        if match:
            parsed_data["current_power_consumption"] = float(match.group(1))
        else:
            parsed_data["current_power_consumption"] = None

        # Extract phase 1, 2, and 3 energy (1-0:21.7.0, 1-0:41.7.0, 1-0:61.7.0)
        match = re.search(r"1-0:21\.7\.0\((\d+\.\d+)\*kW\)", data)
        if match:
            parsed_data["phase1_energy"] = float(match.group(1))
        else:
            parsed_data["phase1_energy"] = None

        match = re.search(r"1-0:41\.7\.0\((\d+\.\d+)\*kW\)", data)
        if match:
            parsed_data["phase2_energy"] = float(match.group(1))
        else:
            parsed_data["phase2_energy"] = None

        match = re.search(r"1-0:61\.7\.0\((\d+\.\d+)\*kW\)", data)
        if match:
            parsed_data["phase3_energy"] = float(match.group(1))
        else:
            parsed_data["phase3_energy"] = None

        # Extract phase 1, 2, and 3 voltage (1-0:32.7.0, 1-0:52.7.0, 1-0:72.7.0)
        match = re.search(r"1-0:32\.7\.0\((\d+\.\d+)\*V\)", data)
        if match:
            parsed_data["phase1_voltage"] = float(match.group(1))
        else:
            parsed_data["phase1_voltage"] = None

        match = re.search(r"1-0:52\.7\.0\((\d+\.\d+)\*V\)", data)
        if match:
            parsed_data["phase2_voltage"] = float(match.group(1))
        else:
            parsed_data["phase2_voltage"] = None

        match = re.search(r"1-0:72\.7\.0\((\d+\.\d+)\*V\)", data)
        if match:
            parsed_data["phase3_voltage"] = float(match.group(1))
        else:
            parsed_data["phase3_voltage"] = None

        # Extract phase 1, 2, and 3 current (1-0:31.7.0, 1-0:51.7.0, 1-0:71.7.0)
        match = re.search(r"1-0:31\.7\.0\((\d+\.\d+)\*A\)", data)
        if match:
            parsed_data["phase1_current"] = float(match.group(1))
        else:
            parsed_data["phase1_current"] = None

        match = re.search(r"1-0:51\.7\.0\((\d+\.\d+)\*A\)", data)
        if match:
            parsed_data["phase2_current"] = float(match.group(1))
        else:
            parsed_data["phase2_current"] = None

        match = re.search(r"1-0:71\.7\.0\((\d+\.\d+)\*A\)", data)
        if match:
            parsed_data["phase3_current"] = float(match.group(1))
        else:
            parsed_data["phase3_current"] = None

        # Extract reactive energy consumed (1-0:3.8.0)
        match = re.search(r"1-0:3\.8\.0\((\d+\.\d+)\*kVArh\)", data)
        if match:
            parsed_data["reactive_energy_consumed"] = float(match.group(1))
        else:
            parsed_data["reactive_energy_consumed"] = None

        # Extract tariff 1 and tariff 2 energy (1-0:1.8.0, 1-0:2.8.0)
        match = re.search(r"1-0:1\.8\.0\((\d+\.\d+)\*kWh\)", data)
        if match:
            parsed_data["tariff1_energy"] = float(match.group(1))
        else:
            parsed_data["tariff1_energy"] = None

        match = re.search(r"1-0:2\.8\.0\((\d+\.\d+)\*kWh\)", data)
        if match:
            parsed_data["tariff2_energy"] = float(match.group(1))
        else:
            parsed_data["tariff2_energy"] = None

    except Exception as e:
        logging.error(f"Error parsing data: {e}")
        parsed_data = {}  # Return an empty dict if parsing fails

    return parsed_data

# Function to fetch new data from the source database (100 rows at a time)
def fetch_new_data(last_fetched_timestamp):
    try:
        conn = pyodbc.connect(source_conn_str)
        cursor = conn.cursor()

        query = """
            SELECT id, device_id, timestamp, processed, data
            FROM mqtt_raw_data
            WHERE timestamp > ?
            ORDER BY timestamp
            OFFSET 0 ROWS
            FETCH NEXT 1000 ROWS ONLY
        """
        cursor.execute(query, last_fetched_timestamp)
        rows = cursor.fetchall()

        # Convert rows to a list of dictionaries
        data = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
        return data
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return []
    finally:
        if conn:
            conn.close()

# Function to insert parsed data into the target database
def insert_parsed_data(data):
    if not data:
        logging.info("No new data to insert.")
        return

    try:
        conn = pyodbc.connect(target_conn_str)
        cursor = conn.cursor()

        # Prepare the insert query
        insert_query = """
            INSERT INTO mqtt_raw_data (
                id, device_id, timestamp, processed,
                total_energy_consumed, current_power_consumption,
                phase1_energy, phase2_energy, phase3_energy,
                phase1_voltage, phase2_voltage, phase3_voltage,
                phase1_current, phase2_current, phase3_current,
                reactive_energy_consumed, tariff1_energy, tariff2_energy
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Prepare the data for batch insertion
        insert_data = [
            (
                item["id"],
                item["device_id"],
                item["timestamp"],
                item["processed"],
                item["total_energy_consumed"],
                item["current_power_consumption"],
                item["phase1_energy"],
                item["phase2_energy"],
                item["phase3_energy"],
                item["phase1_voltage"],
                item["phase2_voltage"],
                item["phase3_voltage"],
                item["phase1_current"],
                item["phase2_current"],
                item["phase3_current"],
                item["reactive_energy_consumed"],
                item["tariff1_energy"],
                item["tariff2_energy"]
            )
            for item in data
        ]

        # Execute the batch insert
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        logging.info(f"Inserted {len(data)} records successfully.")
    except pyodbc.IntegrityError as e:
        logging.warning(f"Skipping duplicate records: {e}")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
    finally:
        if conn:
            conn.close()

# Function to get the last fetched timestamp from the metadata table
def get_last_fetched_timestamp():
    try:
        conn = pyodbc.connect(target_conn_str)
        cursor = conn.cursor()

        query = """
            SELECT last_fetched_timestamp
            FROM process_metadata
            WHERE process_name = ?
        """
        cursor.execute(query, process_name)
        row = cursor.fetchone()

        if row:
            return row[0]
        else:
            # If no record exists, insert a default timestamp (e.g., 1 hour ago)
            default_timestamp = datetime.now(timezone.utc) - timedelta(hours=1)
            cursor.execute("""
                INSERT INTO process_metadata (process_name, last_fetched_timestamp)
                VALUES (?, ?)
            """, process_name, default_timestamp)
            conn.commit()
            return default_timestamp
    except Exception as e:
        logging.error(f"Error fetching last timestamp: {e}")
        return datetime.now(timezone.utc) - timedelta(hours=1)
    finally:
        if conn:
            conn.close()

# Function to update the last fetched timestamp in the metadata table
def update_last_fetched_timestamp(new_timestamp):
    try:
        conn = pyodbc.connect(target_conn_str)
        cursor = conn.cursor()

        query = """
            UPDATE process_metadata
            SET last_fetched_timestamp = ?
            WHERE process_name = ?
        """
        cursor.execute(query, new_timestamp, process_name)
        conn.commit()
        logging.info(f"Updated last_fetched_timestamp to {new_timestamp}.")
    except Exception as e:
        logging.error(f"Error updating last timestamp: {e}")
    finally:
        if conn:
            conn.close()

# Main function to run the batch process
def run_batch_process():
    while True:
        try:
            # Get the last fetched timestamp
            last_fetched_timestamp = get_last_fetched_timestamp()

            # Fetch new data (100 rows at a time)
            new_data = fetch_new_data(last_fetched_timestamp)

            # Parse the data
            parsed_data = []
            for item in new_data:
                parsed_item = parse_data(item["data"])
                if parsed_item:
                    parsed_data.append({
                        **item,
                        **parsed_item
                    })

            # Insert parsed data
            insert_parsed_data(parsed_data)

            # Update the last fetched timestamp
            if parsed_data:
                new_timestamp = max(item["timestamp"] for item in parsed_data)
                update_last_fetched_timestamp(new_timestamp)
        except Exception as e:
            logging.error(f"Error in batch process: {e}")

        # Wait for 1 minute before the next run
        time.sleep(20)

# Run the batch process
if __name__ == "__main__":
    logging.info("Starting batch process...")
    run_batch_process()