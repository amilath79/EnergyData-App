import requests
import pyodbc
from datetime import datetime

# Function to format timestamp
def format_timestamp(timestamp_str):
    dt = datetime.fromisoformat(timestamp_str)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# Fetch data from API
api_url = "http://localhost:8001/data"
response = requests.get(api_url)
data = response.json()

# Print the fetched data for debugging
print(data)  # Add this line to debug and verify the fetched data

# Connect to Azure SQL Database
conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=mqttmomentum.database.windows.net;"
    "Database=mqttmomentum;"
    "UID=amilath;"
    "PWD=Atth617QAQA;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Insert data into the table
for item in data:
    try:
        cursor.execute("""
            INSERT INTO mqtt_raw_data (
                id, device_id, timestamp, processed,
                total_energy_consumed, current_power_consumption,
                phase1_energy, phase2_energy, phase3_energy,
                phase1_voltage, phase2_voltage, phase3_voltage,
                phase1_current, phase2_current, phase3_current,
                reactive_energy_consumed, tariff1_energy, tariff2_energy
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        item['id'], item['device_id'], format_timestamp(item['timestamp']), item['processed'],
        item['total_energy_consumed'], item['current_power_consumption'],
        item['phase1_energy'], item['phase2_energy'], item['phase3_energy'],
        item['phase1_voltage'], item['phase2_voltage'], item['phase3_voltage'],
        item['phase1_current'], item['phase2_current'], item['phase3_current'],
        item['reactive_energy_consumed'], item['tariff1_energy'], item['tariff2_energy']
        )
    except Exception as e:
        print(f"Error inserting item: {e}")

conn.commit()
cursor.close()
conn.close()