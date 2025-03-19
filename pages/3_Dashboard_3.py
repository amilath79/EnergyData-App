import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# API endpoint
API_URL = "http://localhost:8001/data"

# Function to fetch data from the API
def fetch_data():
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        else:
            st.error(f"Failed to fetch data: {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# Sidebar Navigation
def sidebar_navigation():
    st.sidebar.title("Navigation")
    if st.sidebar.button("Main Dashboard"):
        st.session_state["current_page"] = "Main Dashboard"
    if st.sidebar.button("Dashboard 1"):
        st.session_state["current_page"] = "Dashboard 1"
    if st.sidebar.button("Dashboard 2"):
        st.session_state["current_page"] = "Dashboard 2"
    if st.sidebar.button("Dashboard 3"):
        st.session_state["current_page"] = "Dashboard 3"

# Function to resample data based on the selected interval
def resample_data(data, interval):
    # Convert timestamp to datetime
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    data.set_index("timestamp", inplace=True)

    # Resample based on the selected interval
    if interval == "30 minutes":
        resampled_data = data.resample("30T").sum()
    elif interval == "1 hour":
        resampled_data = data.resample("1H").sum()
    elif interval == "1 day":
        resampled_data = data.resample("1D").sum()
    elif interval == "1 week":
        resampled_data = data.resample("1W").sum()
    else:
        resampled_data = data  # Default to raw data

    return resampled_data.reset_index()

# Streamlit app for Dashboard 3
def dashboard_3():
    st.title("Energy Consumption Aggregated by Time Intervals")

    # Display the sidebar navigation
    # sidebar_navigation()

    # Fetch data from the API
    st.write("Fetching data from the database...")
    data = fetch_data()

    # Check if data is available
    if not data.empty:
        # Add a dropdown to select the time interval
        interval = st.selectbox(
            "Select Time Interval",
            ["30 minutes", "1 hour", "1 day", "1 week"],
            index=0,  # Default to "30 minutes"
        )

        # Resample the data based on the selected interval
        resampled_data = resample_data(data, interval)

        # Display the resampled data
        st.subheader(f"Energy Consumption Aggregated by {interval}")
        st.write(resampled_data)

        # Create a time series graph for total energy consumed
        st.subheader(f"Total Energy Consumed Over Time ({interval})")
        fig = px.line(
            resampled_data,
            x="timestamp",
            y="total_energy_consumed",
            title=f"Total Energy Consumed Over Time ({interval})",
            labels={"timestamp": "Time", "total_energy_consumed": "Total Energy Consumed (kWh)"},
        )
        st.plotly_chart(fig)

        # Create a time series graph for current power consumption
        st.subheader(f"Current Power Consumption Over Time ({interval})")
        fig = px.line(
            resampled_data,
            x="timestamp",
            y="current_power_consumption",
            title=f"Current Power Consumption Over Time ({interval})",
            labels={"timestamp": "Time", "current_power_consumption": "Current Power Consumption (kW)"},
        )
        st.plotly_chart(fig)

        # Create a time series graph for phase-wise energy consumption
        st.subheader(f"Phase-wise Energy Consumption Over Time ({interval})")
        fig = px.line(
            resampled_data,
            x="timestamp",
            y=["phase1_energy", "phase2_energy", "phase3_energy"],
            title=f"Phase-wise Energy Consumption Over Time ({interval})",
            labels={"timestamp": "Time", "value": "Energy Consumption (kW)"},
        )
        st.plotly_chart(fig)

    else:
        st.write("No data available.")

# Run the Streamlit app
if __name__ == "__main__":
    dashboard_3()