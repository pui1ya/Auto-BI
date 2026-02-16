import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="Auto BI Dashboard")


page = st.sidebar.selectbox(
    "Select View",
    ["Overview", "Customers"]
)


def show_overview():
    st.title("Daily Revenue Overview")

    API_URL = "http://127.0.0.1:8000/metrics/daily"
    response = requests.get(API_URL)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)

        st.subheader("Daily Sales Data")
        st.dataframe(df)

        st.subheader("Revenue Trend")
        st.line_chart(df.set_index("order_date")["total_sales"])
    else:
        st.error("Failed to fetch data")


def show_customers():
    st.title("Customer Analytics")

    API_URL = "http://127.0.0.1:8000/metrics/top-customers"
    response = requests.get(API_URL)

    if response.status_code == 200:
        df_top = pd.DataFrame(response.json())
        st.bar_chart(df_top.set_index("name")["total_sales"])
    else:
        st.error("Failed to fetch customer data")


if page == "Overview":
    show_overview()

elif page == "Customers":
    show_customers()

