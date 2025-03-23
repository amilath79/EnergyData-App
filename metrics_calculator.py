import pyodbc
from datetime import datetime, timedelta, timezone
import time
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Target database connection details (to fetch data and insert metrics)
target_db_host = "mqttmomentum.database.windows.net"
target_db_name = "mqttmomentum"
target_db_user = "amilath"
target_db_password = "Atth617QAQA"

# Construct connection string
target_conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={target_db_host};"
    f"DATABASE={target_db_name};"
    f"UID={target_db_user};"
    f"PWD={target_db_password};"
)

# Process name (used to identify the process in the metadata table)
process_name = "metrics_calculator"

# Function to fetch data from the target database
def fetch_data(last_processed_timestamp):
    try:
        conn = pyodbc.connect(target_conn_str)
        cursor = conn.cursor()

        query = """
            SELECT id, device_id, timestamp, processed,
                   total_energy_consumed, current_power_consumption,
                   phase1_energy, phase2_energy, phase3_energy,
                   phase1_voltage, phase2_voltage, phase3_voltage,
                   phase1_current, phase2_current, phase3_current,
                   reactive_energy_consumed, tariff1_energy, tariff2_energy
            FROM mqtt_raw_data
            WHERE timestamp > ?
            ORDER BY timestamp
        """
        cursor.execute(query, last_processed_timestamp)
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

# Function to calculate metrics and insert into the new table
def calculate_and_insert_metrics(data):
    if not data:
        logging.info("No data to process.")
        return

    # Group data by device_id, day, and 10-minute intervals
    grouped_data = defaultdict(list)
    for item in data:
        timestamp = item["timestamp"]  # Already a datetime object
        day = timestamp.date()
        hour = timestamp.hour
        interval_start = (timestamp.minute // 10) * 10  # 0, 10, 20, etc.
        key = (item["device_id"], day, hour, interval_start)
        grouped_data[key].append(item)

    # Calculate metrics for each group
    metrics = []
    for key, group in grouped_data.items():
        device_id, day, hour, interval_start = key

        # Sort group by timestamp
        group.sort(key=lambda x: x["timestamp"])

        # Skip groups with NULL values in required fields
        if (
            group[0]["total_energy_consumed"] is None or
            group[-1]["total_energy_consumed"] is None or
            group[0]["reactive_energy_consumed"] is None or
            group[-1]["reactive_energy_consumed"] is None or
            group[0]["tariff1_energy"] is None or
            group[-1]["tariff1_energy"] is None or
            group[0]["tariff2_energy"] is None or
            group[-1]["tariff2_energy"] is None
        ):
            logging.warning(f"Skipping group {key} due to NULL values.")
            continue

        # Calculate consumption (last value - first value)
        power_consumption = group[-1]["total_energy_consumed"] - group[0]["total_energy_consumed"]

        # Debug print to log the values
        print(f"Device: {device_id}, Day: {day}, Hour: {hour}, Interval Start: {interval_start}")
        print(f"First Current Power Consumption: {group[0]['total_energy_consumed']}")
        print(f"Last Current Power Consumption: {group[-1]['total_energy_consumed']}")
        print(f"Calculated Power Consumption: {round(power_consumption,3)}")
        print("-" * 50)  # Separator for readability

        # Handle potential meter resets (assuming a maximum value of 9999)
        max_meter_value = 9999  # Adjust this based on your meter's maximum value
        if power_consumption < 0:
            power_consumption += max_meter_value  # Adjust for meter reset
            print(f"Adjusted Power Consumption (after reset handling): {power_consumption}")
            print("-" * 50)  # Separator for readability

        reactive_energy_consumed = group[-1]["reactive_energy_consumed"] - group[0]["reactive_energy_consumed"]
        if reactive_energy_consumed < 0:
            reactive_energy_consumed += max_meter_value  # Adjust for meter reset

        tariff1_energy = group[-1]["tariff1_energy"] - group[0]["tariff1_energy"]
        if tariff1_energy < 0:
            tariff1_energy += max_meter_value  # Adjust for meter reset

        tariff2_energy = group[-1]["tariff2_energy"] - group[0]["tariff2_energy"]
        if tariff2_energy < 0:
            tariff2_energy += max_meter_value  # Adjust for meter reset

        # Calculate averages (skip NULL values)
        avg_phase1_voltage = sum(item["phase1_voltage"] for item in group if item["phase1_voltage"] is not None) / len(group)
        avg_phase2_voltage = sum(item["phase2_voltage"] for item in group if item["phase2_voltage"] is not None) / len(group)
        avg_phase3_voltage = sum(item["phase3_voltage"] for item in group if item["phase3_voltage"] is not None) / len(group)
        avg_phase1_current = sum(item["phase1_current"] for item in group if item["phase1_current"] is not None) / len(group)
        avg_phase2_current = sum(item["phase2_current"] for item in group if item["phase2_current"] is not None) / len(group)
        avg_phase3_current = sum(item["phase3_current"] for item in group if item["phase3_current"] is not None) / len(group)

        # Append metrics
        metrics.append({
        "device_id": device_id,
        "day": day,
        "hour": hour,
        "interval_start": interval_start,
        "power_consumption": round(power_consumption, 3),
        "reactive_energy_consumed": round(reactive_energy_consumed, 3),
        "tariff1_energy": round(tariff1_energy, 3),
        "tariff2_energy": round(tariff2_energy, 3),
        "avg_phase1_voltage": round(avg_phase1_voltage, 3),
        "avg_phase2_voltage": round(avg_phase2_voltage, 3),
        "avg_phase3_voltage": round(avg_phase3_voltage, 3),
        "avg_phase1_current": round(avg_phase1_current, 3),
        "avg_phase2_current": round(avg_phase2_current, 3),
        "avg_phase3_current": round(avg_phase3_current, 3),
    })

    # Insert metrics into the new table
    try:
        conn = pyodbc.connect(target_conn_str)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO device_consumption_metrics (
                device_id, day, hour, interval_start,
                power_consumption, reactive_energy_consumed,
                tariff1_energy, tariff2_energy,
                avg_phase1_voltage, avg_phase2_voltage, avg_phase3_voltage,
                avg_phase1_current, avg_phase2_current, avg_phase3_current
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Prepare the data for batch insertion
        insert_data = [
            (
                item["device_id"],
                item["day"],
                item["hour"],
                item["interval_start"],
                item["power_consumption"],
                item["reactive_energy_consumed"],
                item["tariff1_energy"],
                item["tariff2_energy"],
                item["avg_phase1_voltage"],
                item["avg_phase2_voltage"],
                item["avg_phase3_voltage"],
                item["avg_phase1_current"],
                item["avg_phase2_current"],
                item["avg_phase3_current"]
            )
            for item in metrics
        ]

        # Execute the batch insert
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        logging.info(f"Inserted {len(metrics)} metrics records successfully.")
    except Exception as e:
        logging.error(f"Error inserting metrics: {e}")
    finally:
        if conn:
            conn.close()




# Function to get the last processed timestamp from the metadata table
def get_last_processed_timestamp():
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
        logging.error(f"Error fetching last processed timestamp: {e}")
        return datetime.now(timezone.utc) - timedelta(hours=1)
    finally:
        if conn:
            conn.close()

# Function to update the last processed timestamp in the metadata table
def update_last_processed_timestamp(new_timestamp):
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
        logging.error(f"Error updating last processed timestamp: {e}")
    finally:
        if conn:
            conn.close()

# Main function to run the metrics calculation process
def run_metrics_process():
    while True:
        try:
            # Get the last processed timestamp
            last_processed_timestamp = get_last_processed_timestamp()

            # Fetch data from the target database
            data = fetch_data(last_processed_timestamp)

            # Calculate metrics and insert into the new table
            calculate_and_insert_metrics(data)

            # Update the last processed timestamp
            if data:
                new_timestamp = max(item["timestamp"] for item in data)
                update_last_processed_timestamp(new_timestamp)
        except Exception as e:
            logging.error(f"Error in metrics process: {e}")

        # Wait for 10 minutes before the next run
        time.sleep(600)

# Run the metrics process
if __name__ == "__main__":
    logging.info("Starting metrics calculation process...")
    run_metrics_process()