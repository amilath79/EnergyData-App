import streamlit as st
import pandas as pd
import requests
import plotly.express as px

# Fetch data from the API
def fetch_data():
    url = "http://127.0.0.1:8002/data"
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
    st.title("📊 Energy Monitoring Dashboard")

    # Home Page: Summary of Useful Information
    st.header("📈 Summary of Key Metrics")

    # Convert 'day' column to datetime
    data['day'] = pd.to_datetime(data['day'])

    # Aggregate data for summary metrics
    total_energy = data['power_consumption'].sum()
    total_reactive_energy = data['reactive_energy_consumed'].sum()
    avg_energy_per_day = data.groupby('day')['power_consumption'].sum().mean()
    max_energy_day = data.groupby('day')['power_consumption'].sum().idxmax()
    max_energy = data.groupby('day')['power_consumption'].sum().max()
    total_tariff1_energy = data['tariff1_energy'].sum()
    total_tariff2_energy = data['tariff2_energy'].sum()
    avg_voltage_phase1 = data['avg_phase1_voltage'].mean()
    avg_voltage_phase2 = data['avg_phase2_voltage'].mean()
    avg_voltage_phase3 = data['avg_phase3_voltage'].mean()
    avg_current_phase1 = data['avg_phase1_current'].mean()
    avg_current_phase2 = data['avg_phase2_current'].mean()
    avg_current_phase3 = data['avg_phase3_current'].mean()
    peak_hourly_energy = data.groupby('hour')['power_consumption'].sum().max()
    peak_hour = data.groupby('hour')['power_consumption'].sum().idxmax()

    # Highlighted Metrics Section
    with st.container():
        st.markdown("### 🔋 Key Energy Metrics")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Energy Consumed", f"{total_energy:.2f} kWh")
        col2.metric("Total Reactive Energy", f"{total_reactive_energy:.2f} kWh")
        col3.metric("Average Energy per Day", f"{avg_energy_per_day:.2f} kWh")

        st.markdown("---")  # Divider

        st.markdown("### 🚀 Peak Energy Day")
        st.metric("Peak Energy Day", f"{max_energy_day.strftime('%Y-%m-%d')} ({max_energy:.2f} kWh)")

        st.markdown("---")  # Divider

        st.markdown("### 💡 Tariff Energy Consumption")
        col4, col5 = st.columns(2)
        col4.metric("Total Tariff 1 Energy", f"{total_tariff1_energy:.2f} kWh")
        col5.metric("Total Tariff 2 Energy", f"{total_tariff2_energy:.2f} kWh")

    # Highlighted Voltage and Current Metrics
    with st.container():
        st.markdown("### ⚡ Voltage and Current Metrics")
        col6, col7, col8 = st.columns(3)
        col6.metric("Avg Voltage (Phase 1)", f"{avg_voltage_phase1:.2f} V")
        col7.metric("Avg Voltage (Phase 2)", f"{avg_voltage_phase2:.2f} V")
        col8.metric("Avg Voltage (Phase 3)", f"{avg_voltage_phase3:.2f} V")

        col9, col10, col11 = st.columns(3)
        col9.metric("Avg Current (Phase 1)", f"{avg_current_phase1:.2f} A")
        col10.metric("Avg Current (Phase 2)", f"{avg_current_phase2:.2f} A")
        col11.metric("Avg Current (Phase 3)", f"{avg_current_phase3:.2f} A")

    # Highlighted Peak Hour Metrics
    with st.container():
        st.markdown("### 🕒 Peak Hour Metrics")
        col12, col13 = st.columns(2)
        col12.metric("Peak Hourly Energy", f"{peak_hourly_energy:.2f} kWh")
        col13.metric("Peak Hour", f"{peak_hour}:00")

    # Visualizations in Expanders
    with st.expander("📅 Daily Energy Consumption"):
        daily_data = data.groupby('day')['power_consumption'].sum().reset_index()
        fig_daily = px.line(
            daily_data,
            x='day',
            y='power_consumption',
            title="Daily Energy Consumption Over Time",
            labels={"day": "Date", "power_consumption": "Energy Consumption (kWh)"}
        )
        st.plotly_chart(fig_daily)

    with st.expander("⏰ Average Hourly Energy Consumption"):
        hourly_data = data.groupby('hour')['power_consumption'].mean().reset_index()
        fig_hourly = px.bar(
            hourly_data,
            x='hour',
            y='power_consumption',
            title="Average Energy Consumption by Hour",
            labels={"hour": "Hour of the Day", "power_consumption": "Energy Consumption (kWh)"}
        )
        st.plotly_chart(fig_hourly)

    with st.expander("🔌 Voltage and Current Trends"):
        voltage_data = data[['day', 'avg_phase1_voltage', 'avg_phase2_voltage', 'avg_phase3_voltage']].melt(id_vars='day', var_name='phase', value_name='voltage')
        current_data = data[['day', 'avg_phase1_current', 'avg_phase2_current', 'avg_phase3_current']].melt(id_vars='day', var_name='phase', value_name='current')

        fig_voltage = px.line(
            voltage_data,
            x='day',
            y='voltage',
            color='phase',
            title="Voltage Trends Over Time",
            labels={"day": "Date", "voltage": "Voltage (V)"}
        )

        fig_current = px.line(
            current_data,
            x='day',
            y='current',
            color='phase',
            title="Current Trends Over Time",
            labels={"day": "Date", "current": "Current (A)"}
        )

        st.plotly_chart(fig_voltage)
        st.plotly_chart(fig_current)