import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt

# Read credentials from secrets
db_user = st.secrets["database"]["user"]
db_password = st.secrets["database"]["password"]
db_server = st.secrets["database"]["server"]
db_name = st.secrets["database"]["database"]
db_driver = st.secrets["database"]["driver"]

# Database connection string
connection_string = (
    f"mssql+pyodbc://{db_user}:{db_password}@{db_server}:1433/{db_name}?"
    f"driver={db_driver}"
)

# Function to fetch data
def fetch_data():
    try:
        engine = create_engine(connection_string)
        query = "SELECT * FROM mqtt_processed_data"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()

# Streamlit app
st.title("MQTT Processed Data Dashboard")

# Fetch data
data = fetch_data()

if not data.empty:
    # Convert timestamp to datetime
    data["timestamp"] = pd.to_datetime(data["timestamp"])

    # Get unique device_ids
    device_ids = data["device_id"].unique()

    # Add dropdown for device_id
    selected_device = st.selectbox("Select Device ID", device_ids)

    # Filter data for the selected device
    device_data = data[data["device_id"] == selected_device]

    if not device_data.empty:
        # Resample data to get average energy consumption per hour
        device_data.set_index("timestamp", inplace=True)
        hourly_avg = device_data.resample("H")["energy_consumption"].mean()

        # Forward-fill missing hours with the last available value
        hourly_avg = hourly_avg.fillna(method="ffill")

        # Create a DataFrame for plotting
        hourly_avg_df = hourly_avg.reset_index()
        hourly_avg_df.columns = ["Timestamp", "Average Energy Consumption"]

        # Display the hourly average data
        st.write(f"### Hourly Average Energy Consumption for {selected_device}")
        st.dataframe(hourly_avg_df)

        # Plot the time series with date and time on the x-axis
        st.write("### Time Series of Hourly Average Energy Consumption")
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(hourly_avg_df["Timestamp"], hourly_avg_df["Average Energy Consumption"], marker="o")
        ax.set_xlabel("Date and Hour")
        ax.set_ylabel("Average Energy Consumption")
        ax.set_title(f"Hourly Average Energy Consumption for {selected_device}")
        ax.grid(True)

        # Format x-axis to show date and time
        plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
        plt.tight_layout()  # Adjust layout to prevent label overlap
        st.pyplot(fig)
    else:
        st.warning(f"No data found for Device ID: {selected_device}")
else:
    st.warning("No data found in the database.")

# Refresh button
if st.button("Refresh Data"):
    data = fetch_data()