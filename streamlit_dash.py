import streamlit as st
import pandas as pd
import time
import os

st.title("📊 Real-Time Streaming Dashboard")

# Function to read latest streaming data
def load_data():
    files = sorted([f for f in os.listdir("stream_data/") if f.endswith(".csv")])
    if not files:
        return pd.DataFrame(columns=["id", "value", "timestamp"])

    latest_file = os.path.join("stream_data/", files[-1])
    df = pd.read_csv(latest_file)
    return df

# Auto-refresh mechanism
placeholder = st.empty()

while True:
    df = load_data()
    
    if not df.empty:
        with placeholder.container():
            st.subheader("Latest Data")
            st.dataframe(df)

            st.subheader("Live Value Plot")
            st.line_chart(data=df, x='id', y='value')

    time.sleep(2)  # Refresh every 2 seconds