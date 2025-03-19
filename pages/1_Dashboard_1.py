import streamlit as st
import requests
import pandas as pd

# API endpoint
API_URL = "http://localhost:8001/data"  # Change port to 8001

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


# Streamlit app
def dashboard_1():
    st.title("Dashboard 1")
    st.write("This is Dashboard 1.")

    # Fetch data from the API
    st.write("Fetching data from the database...")
    data = fetch_data()

    # Display the data in a table
    if not data.empty:
        st.write("Data from the database:")
        st.dataframe(data)
    else:
        st.write("No data available.")

# Run the Streamlit app
if __name__ == "__main__":
    dashboard_1()