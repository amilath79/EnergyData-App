from fastapi import FastAPI
import pyodbc
import pandas as pd
import re

# Hardcoded database connection details
db_host = "carlitodatabase.database.windows.net"
db_port = "1433"
db_name = "DataStorage"
db_user = "admin-owner"
db_password = "carlito99!!"

# Construct the connection string
connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={db_host},{db_port};"
    f"DATABASE={db_name};"
    f"UID={db_user};"
    f"PWD={db_password};"
)

# Initialize FastAPI app
app = FastAPI()

# Function to parse the data field
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
        print(f"Error parsing data: {e}")
        parsed_data = {}  # Return an empty dict if parsing fails

    return parsed_data


# Endpoint to fetch data
@app.get("/data")
def get_data():
    conn = None
    try:
        # Connect to the database
        conn = pyodbc.connect(connection_string)
        query = "SELECT id, device_id, timestamp, processed, data FROM mqtt_raw_data ORDER BY id DESC OFFSET 0 ROWS FETCH NEXT 10000 ROWS ONLY;"
        df = pd.read_sql(query, conn)

        # Parse the data field
        df["parsed_data"] = df["data"].apply(parse_data)
        parsed_df = pd.json_normalize(df["parsed_data"])

        # Combine the parsed data with the original DataFrame
        result_df = pd.concat([df[["id", "device_id", "timestamp", "processed"]], parsed_df], axis=1)

        # Convert DataFrame to JSON
        return result_df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
    finally:
        if conn:
            conn.close()

# Run the API
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)