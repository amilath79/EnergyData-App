import streamlit as st
import pandas as pd
import requests
import plotly.express as px

# Fetch data from the API
def fetch_data():
    url = "http://127.0.0.1:8000/data"
    response = requests.get(url)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        st.error("Failed to fetch data from the API")
        return pd.DataFrame()

# Load data
data = fetch_data()

# Check if data is empty
if data.empty:
    st.warning("No data available. Please check the API connection.")
else:
    # Title for the dashboard
    st.title("Phase Analysis Dashboard")

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
        # Phase Analysis: Voltage and Current Trends
        st.subheader("Phase Analysis: Voltage and Current Trends")

        # Box Plot: Voltage Distribution by Phase
        st.markdown("#### Voltage Distribution by Phase")
        fig_voltage_box = px.box(
            filtered_data,
            y=["avg_phase1_voltage", "avg_phase2_voltage", "avg_phase3_voltage"],
            labels={"value": "Voltage (V)", "variable": "Phase"},
            title="Voltage Distribution by Phase",
            color_discrete_sequence=["red", "green", "blue"]  # Different colors for each phase
        )

        # Update layout for better readability
        fig_voltage_box.update_layout(
            yaxis_title="Voltage (V)",
            xaxis_title="Phase",
            showlegend=False
        )

        # Display the voltage box plot
        st.plotly_chart(fig_voltage_box)

        # Box Plot: Current Distribution by Phase
        st.markdown("#### Current Distribution by Phase")
        fig_current_box = px.box(
            filtered_data,
            y=["avg_phase1_current", "avg_phase2_current", "avg_phase3_current"],
            labels={"value": "Current (A)", "variable": "Phase"},
            title="Current Distribution by Phase",
            color_discrete_sequence=["orange", "purple", "cyan"]  # Different colors for each phase
        )

        # Update layout for better readability
        fig_current_box.update_layout(
            yaxis_title="Current (A)",
            xaxis_title="Phase",
            showlegend=False
        )

        # Display the current box plot
        st.plotly_chart(fig_current_box)

        # Line Chart: Voltage Trends Over Time for Each Phase
        st.markdown("#### Voltage Trends Over Time")
        fig_voltage = px.box(
            filtered_data,
            x="day",
            y=["avg_phase1_voltage", "avg_phase2_voltage", "avg_phase3_voltage"],
            labels={"value": "Voltage (V)", "day": "Date"},
            title="Voltage Trends Over Time for Each Phase",
            color_discrete_sequence=["red", "green", "blue"]  # Different colors for each phase
        )

        # Update layout for better readability
        fig_voltage.update_layout(
            xaxis_title="Date",
            yaxis_title="Voltage (V)",
            legend_title="Phase",
            showlegend=True
        )

        # Display the voltage line chart
        st.plotly_chart(fig_voltage)

        # Line Chart: Current Trends Over Time for Each Phase
        st.markdown("#### Current Trends Over Time")
        fig_current = px.box(
            filtered_data,
            x="day",
            y=["avg_phase1_current", "avg_phase2_current", "avg_phase3_current"],
            labels={"value": "Current (A)", "day": "Date"},
            title="Current Trends Over Time for Each Phase",
            color_discrete_sequence=["orange", "purple", "cyan"]  # Different colors for each phase
        )

        # Update layout for better readability
        fig_current.update_layout(
            xaxis_title="Date",
            yaxis_title="Current (A)",
            legend_title="Phase",
            showlegend=True
        )

        # Display the current line chart
        st.plotly_chart(fig_current)