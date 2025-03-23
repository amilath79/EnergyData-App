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
    # Heatmap: Energy Consumption by Day and Hour
    st.title("Energy Consumption by Day and Hour")

    # Aggregate energy consumption by day and hour
    heatmap_data = data.groupby(["day", "hour"])["power_consumption"].mean().reset_index()

    # Pivot the data for the heatmap
    heatmap_pivot = heatmap_data.pivot(index="hour", columns="day", values="power_consumption")

    # Create the heatmap with a custom color scale (red to green)
    fig_heatmap = px.imshow(
        heatmap_pivot,
        labels=dict(x="Day", y="Hour", color="Energy Consumption (kWh)"),
        x=heatmap_pivot.columns,
        y=heatmap_pivot.index,
        title="Energy Consumption by Day and Hour",
        color_continuous_scale=[[0, "green"], [0.5, "yellow"], [1, "red"]]  # Custom color scale
    )

    # Update layout for better readability
    fig_heatmap.update_layout(
        xaxis_title="Day",
        yaxis_title="Hour",
        coloraxis_colorbar=dict(title="Energy Consumption (kWh)")
    )

    # Display the heatmap
    st.plotly_chart(fig_heatmap)

    # Visualization 2: Energy Consumption Over Time (Bar Chart with Date Dragger)
    st.title("Energy Consumption Over Time")

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

    # Display the selected hour range
    st.write(f"Selected Hour Range: **{hour_range[0]} to {hour_range[1]}**")

    # Check if filtered data is empty
    if filtered_data.empty:
        st.warning("No data available for the selected date and hour range.")
    else:
        # Aggregate energy consumption by hour for the filtered data
        bar_data = filtered_data.groupby("hour")["power_consumption"].mean().reset_index()

        # Create the bar chart
        fig_bar = px.bar(
            bar_data,
            x="hour",
            y="power_consumption",
            title=f"Average Energy Consumption in 24 Hours - {start_date} to {end_date}",
            labels={"hour": "Hour of the Day", "power_consumption": "Energy Consumption (kWh)"},  # Add units
        )

        # Update layout for better readability
        fig_bar.update_layout(
            xaxis_title="Hour of the Day",
            yaxis_title="Average Energy Consumption (kWh)",  # Add units
            xaxis=dict(tickmode="linear", tick0=0, dtick=1),  # Show every hour on x-axis
            showlegend=False  # Hide legend (not needed for this chart)
        )

        # Display the bar chart
        st.plotly_chart(fig_bar)

        # Aggregate energy consumption by day
        daily_data = filtered_data.groupby("day")["power_consumption"].sum().reset_index()

        # Create the line