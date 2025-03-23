import streamlit as st
import pandas as pd
import requests
import numpy as np
import plotly.express as px

# Fetch data from the API
def fetch_data():
    url = "https://energy-api-momentum-fnc2e5cseaezerh9.swedencentral-01.azurewebsites.net/data"
    response = requests.get(url)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        st.error("Failed to fetch data from the API")
        return pd.DataFrame()

# Function to detect anomalies using Z-score
def detect_anomalies(data, column, threshold=3):
    mean = data[column].mean()
    std = data[column].std()
    data['z_score'] = (data[column] - mean) / std
    anomalies = data[abs(data['z_score']) > threshold]
    return anomalies

# Load data
data = fetch_data()

# Check if data is empty
if data.empty:
    st.warning("No data available. Please check the API connection.")
else:
    # Title for the dashboard
    st.title("Anomaly Detector Dashboard")

    # Convert 'day' column to datetime
    data['day'] = pd.to_datetime(data['day'])

    # Add a date range selector
    date_range = st.date_input(
        "Select Date Range",
        value=[data['day'].min(), data['day'].max()],  # Default range: min and max dates in the data
        min_value=data['day'].min(),
        max_value=data['day'].max()
    )

    # Initialize start_date and end_date
    start_date = data['day'].min()
    end_date = data['day'].max()

    # Filter data based on the selected date range
    if len(date_range) == 2:  # Ensure both start and end dates are selected
        start_date, end_date = date_range
        if start_date > end_date:
            st.error("Error: Start date cannot be after end date.")
        else:
            filtered_data = data[(data['day'] >= pd.to_datetime(start_date)) & (data['day'] <= pd.to_datetime(end_date))]
    else:
        st.warning("Please select a valid date range.")
        filtered_data = data  # Fallback to full data if no valid range is selected

    # Add an hour range selector with a nice label
    hour_range = st.slider(
        "Select Hour Range",
        min_value=0,  # Minimum hour (0 for midnight)
        max_value=23,  # Maximum hour (23 for 11 PM)
        value=(0, 23),  # Default range (0 to 23)
        help="Select the range of hours to filter the data."
    )

    # Filter data based on the selected hour range
    filtered_data = filtered_data[
        (filtered_data['hour'] >= hour_range[0]) & 
        (filtered_data['hour'] <= hour_range[1])
    ]

    # Display the selected date and hour range
    st.write(f"Selected Date Range: **{start_date} to {end_date}**")
    st.write(f"Selected Hour Range: **{hour_range[0]} to {hour_range[1]}**")

    # Check if filtered data is empty
    if filtered_data.empty:
        st.warning("No data available for the selected date and hour range.")
    else:
        # Anomaly Detection
        st.subheader("Anomaly Detection")

        # Select column for anomaly detection
        column = st.selectbox(
            "Select Column for Anomaly Detection",
            options=["power_consumption", "avg_phase1_voltage", "avg_phase2_voltage", "avg_phase3_voltage",
                     "avg_phase1_current", "avg_phase2_current", "avg_phase3_current"],
            help="Select the column to detect anomalies in."
        )

        # Add a threshold slider
        threshold = st.slider(
            "Select Z-Score Threshold",
            min_value=1.0,  # Minimum threshold
            max_value=5.0,  # Maximum threshold
            value=3.0,  # Default threshold
            step=0.1,  # Increment by 0.1
            help="Adjust the Z-score threshold for anomaly detection. Higher values detect fewer anomalies."
        )

        # Detect anomalies
        anomalies = detect_anomalies(filtered_data, column, threshold)

        # Display anomalies
        st.write(f"**Anomalies Detected in {column} (Threshold: {threshold}):**")
        st.dataframe(anomalies)

        # Visualize anomalies
        st.markdown("#### Anomaly Visualization")
        fig = px.scatter(
            filtered_data,
            x="day",
            y=column,
            title=f"Anomalies in {column} (Threshold: {threshold})",
            labels={"day": "Date", column: column},
            color=abs(filtered_data['z_score']) > threshold,  # Highlight anomalies based on the selected threshold
            color_discrete_sequence=["blue", "red"]  # Blue for normal, red for anomalies
        )

        # Update layout for better readability
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=column,
            showlegend=True,
            legend_title="Anomaly"
        )

        # Display the scatter plot
        st.plotly_chart(fig)