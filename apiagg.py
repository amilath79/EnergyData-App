from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import pyodbc
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


# Database connection details
db_host = "mqttmomentum.database.windows.net"  # Replace with your Azure SQL server name
db_port = "1433"  # Default port for Azure SQL
db_name = "mqttmomentum"  # Replace with your database name
db_user = "amilath"  # Replace with your database username
db_password = "Atth617QAQA"  # Replace with your database password

# Construct the connection string
connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={db_host},{db_port};"
    f"DATABASE={db_name};"
    f"UID={db_user};"
    f"PWD={db_password};"
)

# Function to fetch data from the database with optional pagination
def fetch_data_from_db(skip: int = 0, limit: int = 100):
    try:
        # Connect to the database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Query to fetch data (with pagination)
        query = f"SELECT * FROM device_consumption_metrics ORDER BY id OFFSET {skip} ROWS FETCH NEXT {limit} ROWS ONLY"
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
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Endpoint to get all data with pagination
@app.get("/data", response_model=List[EnergyData])
async def get_data(skip: int = 0, limit: int = 100):
    try:
        data = fetch_data_from_db(skip=skip, limit=limit)
        return data
    except HTTPException as e:
        raise e
    except Exception as e:
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
        raise HTTPException(status_code=500, detail=str(e))

# Run the API
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
