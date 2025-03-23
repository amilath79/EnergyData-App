from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import pyodbc
import os
from datetime import date  # Import date for type checking

app = FastAPI()

# Define a Pydantic model for your data
class EnergyData(BaseModel):
    id: int
    device_id: str
    day: str  # Ensure this is a string
    hour: int
    interval_start: int
    power_consumption: float
    reactive_energy_consumed: float
    tariff1_energy: float
    tariff2_energy: float
    avg_phase1_voltage: float
    avg_phase2_voltage: float
    avg_phase3_voltage: float
    avg_phase1_current: float
    avg_phase2_current: float
    avg_phase3_current: float

# Database connection details from environment variables
db_host = os.getenv("DB_HOST", "mqttmomentum.database.windows.net")
db_port = os.getenv("DB_PORT", "1433")
db_name = os.getenv("DB_NAME", "mqttmomentum")
db_user = os.getenv("DB_USER", "amilath")
db_password = os.getenv("DB_PASSWORD", "Atth617QAQA")

# Construct the connection string
connection_string = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"  # Updated from 17 to 18
    f"SERVER={db_host},{db_port};"
    f"DATABASE={db_name};"
    f"UID={db_user};"
    f"PWD={db_password};"
)

# Simple health check endpoint
@app.get("/")
async def root():
    return {"message": "Energy API is running", "status": "online"}

# Function to fetch data from the database
def fetch_data_from_db():
    try:
        # Connect to the database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Query to fetch data
        query = "SELECT * FROM device_consumption_metrics"
        cursor.execute(query)

        # Fetch all rows
        rows = cursor.fetchall()

        # Convert rows to a list of dictionaries
        data = []
        for row in rows:
            # Convert datetime.date to string if necessary
            day_value = row.day
            if isinstance(day_value, date):
                day_value = day_value.isoformat()  # Convert to string in YYYY-MM-DD format

            data.append({
                "id": row.id,
                "device_id": row.device_id,
                "day": day_value,  # Use the converted value
                "hour": row.hour,
                "interval_start": row.interval_start,
                "power_consumption": row.power_consumption,
                "reactive_energy_consumed": row.reactive_energy_consumed,
                "tariff1_energy": row.tariff1_energy,
                "tariff2_energy": row.tariff2_energy,
                "avg_phase1_voltage": row.avg_phase1_voltage,
                "avg_phase2_voltage": row.avg_phase2_voltage,
                "avg_phase3_voltage": row.avg_phase3_voltage,
                "avg_phase1_current": row.avg_phase1_current,
                "avg_phase2_current": row.avg_phase2_current,
                "avg_phase3_current": row.avg_phase3_current
            })

        # Close the connection
        cursor.close()
        conn.close()

        return data

    except Exception as e:
        print(f"Database connection error: {str(e)}")  # Improved error logging
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Endpoint to get all data
@app.get("/data", response_model=List[EnergyData])
async def get_data():
    try:
        data = fetch_data_from_db()
        return data
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error in get_data endpoint: {str(e)}")  # Improved error logging
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to get data by ID
@app.get("/data/{data_id}", response_model=EnergyData)
async def get_data_by_id(data_id: int):
    try:
        data = fetch_data_from_db()
        for item in data:
            if item["id"] == data_id:
                return item
        raise HTTPException(status_code=404, detail="Data not found")
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error in get_data_by_id endpoint: {str(e)}")  # Improved error logging
        raise HTTPException(status_code=500, detail=str(e))

# Run the API
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)