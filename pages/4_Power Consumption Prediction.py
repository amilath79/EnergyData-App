import streamlit as st
import pandas as pd
import requests
import numpy as np
import plotly.express as px
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

# Fetch data from the API
def fetch_data():
    url = "https://energy-api-momentum-fnc2e5cseaezerh9.swedencentral-01.azurewebsites.net/data"
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
    st.title("Next Day Power Consumption Prediction")

    # Convert 'day' column to datetime
    data['day'] = pd.to_datetime(data['day'])

    # Aggregate power consumption by day
    daily_data = data.groupby('day')['power_consumption'].sum().reset_index()

    # Check if daily_data is empty
    if daily_data.empty:
        st.warning("No daily power consumption data available.")
    else:
        # Feature Engineering: Use the day number as a feature
        daily_data['day_num'] = (daily_data['day'] - daily_data['day'].min()).dt.days

        # Prepare features (X) and target (y)
        X = daily_data[['day_num']]  # Feature: Day number
        y = daily_data['power_consumption']  # Target: Power consumption

        # Train-test split (use 80% of the data for training)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Check if train or test data is empty
        if X_train.empty or X_test.empty:
            st.warning("Insufficient data for training and testing.")
        else:
            # Train Linear Regression model
            model = LinearRegression()
            model.fit(X_train, y_train)

            # Predict on test data
            y_pred = model.predict(X_test)

            # Calculate Mean Absolute Error (MAE)
            mae = mean_absolute_error(y_test, y_pred)
            st.write(f"Mean Absolute Error (MAE): **{mae:.2f}**")

            # Predict next day's power consumption
            next_day_num = daily_data['day_num'].max() + 1
            next_day_consumption = model.predict([[next_day_num]])[0]

            # Display next day's prediction
            st.write(f"### Predicted Power Consumption for Next Day: **{next_day_consumption:.2f} kWh**")

            # Plot historical data and prediction
            fig = px.line(
                daily_data,
                x='day',
                y='power_consumption',
                title="Historical Power Consumption",
                labels={"day": "Date", "power_consumption": "Power Consumption (kWh)"}
            )

            # Add next day's prediction to the plot
            next_day = daily_data['day'].max() + pd.Timedelta(days=1)
            fig.add_scatter(
                x=[next_day],
                y=[next_day_consumption],
                mode='markers',
                marker=dict(color='red', size=10),
                name="Next Day Prediction"
            )

            # Display the plot
            st.plotly_chart(fig)