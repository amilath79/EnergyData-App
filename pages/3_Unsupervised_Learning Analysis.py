import streamlit as st
import pandas as pd
import requests
import numpy as np
import plotly.express as px
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

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
    st.title("Unsupervised Learning Analysis")

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
        # Unsupervised Learning Analysis
        st.subheader("Unsupervised Learning Analysis")

        # Select features for analysis
        features = st.multiselect(
            "Select Features for Analysis",
            options=["power_consumption", "avg_phase1_voltage", "avg_phase2_voltage", "avg_phase3_voltage",
                     "avg_phase1_current", "avg_phase2_current", "avg_phase3_current"],
            default=["power_consumption", "avg_phase1_voltage", "avg_phase1_current"],
            help="Select features to use for unsupervised learning."
        )

        # Standardize the data
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(filtered_data[features])

        # Select unsupervised learning technique
        analysis_type = st.selectbox(
            "Select Analysis Type",
            options=["Clustering (K-Means)", "Dimensionality Reduction (PCA)", "Anomaly Detection (Isolation Forest)"],
            help="Select the type of unsupervised learning analysis to perform."
        )

        if analysis_type == "Clustering (K-Means)":
            # K-Means Clustering
            st.markdown("#### K-Means Clustering")
            n_clusters = st.slider(
                "Select Number of Clusters",
                min_value=2,  # Minimum number of clusters
                max_value=10,  # Maximum number of clusters
                value=3,  # Default number of clusters
                help="Select the number of clusters for K-Means."
            )

            # Perform K-Means clustering
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            filtered_data['cluster'] = kmeans.fit_predict(scaled_data)

            # Visualize clusters
            fig_clusters = px.scatter(
                filtered_data,
                x="power_consumption",
                y="avg_phase1_voltage",
                color="cluster",
                title=f"K-Means Clustering (Number of Clusters: {n_clusters})",
                labels={"power_consumption": "Power Consumption", "avg_phase1_voltage": "Phase 1 Voltage"}
            )

            # Display the scatter plot
            st.plotly_chart(fig_clusters)

        elif analysis_type == "Dimensionality Reduction (PCA)":
            # PCA for Dimensionality Reduction
            st.markdown("#### PCA for Dimensionality Reduction")
            pca = PCA(n_components=2)
            pca_result = pca.fit_transform(scaled_data)
            filtered_data['pca_1'] = pca_result[:, 0]
            filtered_data['pca_2'] = pca_result[:, 1]

            # Visualize PCA results
            fig_pca = px.scatter(
                filtered_data,
                x="pca_1",
                y="pca_2",
                title="PCA for Dimensionality Reduction",
                labels={"pca_1": "PCA Component 1", "pca_2": "PCA Component 2"}
            )

            # Display the scatter plot
            st.plotly_chart(fig_pca)

        elif analysis_type == "Anomaly Detection (Isolation Forest)":
            # Isolation Forest for Anomaly Detection
            st.markdown("#### Anomaly Detection (Isolation Forest)")
            isolation_forest = IsolationForest(contamination=0.05, random_state=42)
            filtered_data['anomaly'] = isolation_forest.fit_predict(scaled_data)
            filtered_data['anomaly'] = filtered_data['anomaly'].apply(lambda x: 1 if x == -1 else 0)  # Convert to binary

            # Visualize anomalies
            fig_anomalies = px.scatter(
                filtered_data,
                x="power_consumption",
                y="avg_phase1_voltage",
                color="anomaly",
                title="Anomaly Detection (Isolation Forest)",
                labels={"power_consumption": "Power Consumption", "avg_phase1_voltage": "Phase 1 Voltage"},
                color_discrete_sequence=["blue", "red"]  # Blue for normal, red for anomalies
            )

            # Display the scatter plot
            st.plotly_chart(fig_anomalies)