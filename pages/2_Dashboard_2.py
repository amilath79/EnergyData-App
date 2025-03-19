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

# Streamlit app for Dashboard 2
def dashboard_2():
    st.title("Dashboard 2")
    st.write("Time Series Energy Consumption Graph")

    # Display the sidebar navigation
    sidebar_navigation()

    # Fetch data from the API
    st.write("Fetching data from the database...")
    data = fetch_data()

    # Check if data is available
    if not data.empty:
        # Convert timestamp to datetime
        data["timestamp"] = pd.to_datetime(data["timestamp"])

        # Create a time series graph for total energy consumed
        st.subheader("Total Energy Consumed Over Time")
        fig = px.line(
            data,
            x="timestamp",
            y="total_energy_consumed",
            title="Total Energy Consumed Over Time",
            labels={"timestamp": "Time", "total_energy_consumed": "Total Energy Consumed (kWh)"},
        )
        st.plotly_chart(fig)

        # Create a time series graph for current power consumption
        st.subheader("Current Power Consumption Over Time")
        fig = px.line(
            data,
            x="timestamp",
            y="current_power_consumption",
            title="Current Power Consumption Over Time",
            labels={"timestamp": "Time", "current_power_consumption": "Current Power Consumption (kW)"},
        )
        st.plotly_chart(fig)

        # Create a time series graph for phase-wise energy consumption
        st.subheader("Phase-wise Energy Consumption Over Time")
        fig = px.line(
            data,
            x="timestamp",
            y=["phase1_energy", "phase2_energy", "phase3_energy"],
            title="Phase-wise Energy Consumption Over Time",
            labels={"timestamp": "Time", "value": "Energy Consumption (kW)"},
        )
        st.plotly_chart(fig)

    else:
        st.write("No data available.")

# Run the Streamlit app
if __name__ == "__main__":
    dashboard_2()