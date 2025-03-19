# Home.py
import streamlit as st

# Initialize session state for login and page navigation
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False
if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Login"

# Simple login system
def login():
    st.title("Login Page")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    
    if st.button("Login"):
        if username == "admin" and password == "password":  # Replace with your authentication logic
            st.session_state["logged_in"] = True
            st.session_state["current_page"] = "Main Dashboard"
            st.success("Logged in successfully!")
        else:
            st.error("Invalid username or password")

# Main Dashboard
def main_dashboard():
    st.title("Main Dashboard")
    st.write("Welcome to the main dashboard!")

# Dashboard 1
def dashboard_1():
    st.title("Dashboard 1")
    st.write("This is Dashboard 1.")



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

# Render the appropriate page based on session state
if not st.session_state["logged_in"]:
    login()
else:
    # Display sidebar navigation
    sidebar_navigation()
    
    # Render the current page
    if st.session_state["current_page"] == "Main Dashboard":
        main_dashboard()
    elif st.session_state["current_page"] == "Dashboard 1":
        dashboard_1()
    elif st.session_state["current_page"] == "Dashboard 2":
        dashboard_2()
    elif st.session_state["current_page"] == "Dashboard 3":
        dashboard_3()