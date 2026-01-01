import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import glob
from datetime import datetime

# Page Config
st.set_page_config(page_title="NIFTY Futures Visualizer", layout="wide")

# Title
st.title("📈 NIFTY Futures Data Visualizer")

# Path to Data (Relative to this script in Visualizer folder)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "Data")

# 1. File Selection
st.sidebar.header("📁 Select File")

if not os.path.exists(DATA_DIR):
    st.error(f"Data directory not found: {DATA_DIR}")
    st.stop()

# Get list of CSV files
files = glob.glob(os.path.join(DATA_DIR, "NIFTY_Futures_*.csv"))
files = sorted([os.path.basename(f) for f in files], reverse=True)

if not files:
    st.warning("No CSV files found in Data directory.")
    st.stop()

selected_file = st.sidebar.selectbox("Choose Expiry Contract", files)

# 2. Load Data
file_path = os.path.join(DATA_DIR, selected_file)

@st.cache_data
def load_data(path):
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
    else:
        st.error("Column 'datetime' not found in CSV.")
        return None
    return df

with st.spinner(f"Loading {selected_file}..."):
    df = load_data(file_path)

if df is not None:
    # 3. Date Filter
    st.sidebar.header("📅 Time Range")
    
    min_date = df['datetime'].min().date()
    max_date = df['datetime'].max().date()
    
    try:
        start_date, end_date = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
    except ValueError:
        st.sidebar.warning("Not enough data for range selection")
        start_date, end_date = min_date, max_date

    # Filter DF
    mask = (df['datetime'].dt.date >= start_date) & (df['datetime'].dt.date <= end_date)
    filtered_df = df.loc[mask]
    
    st.write(f"**Showing Data**: {start_date} to {end_date} ({len(filtered_df)} rows)")

    # 4. Candlestick Chart
    # GAP FIX: Pass 'datetime' as strings/categories to Plotly to remove weekend gaps?
    # Better: Use rangebreaks.
    
    fig = go.Figure(data=[go.Candlestick(x=filtered_df['datetime'],
                open=filtered_df['open'],
                high=filtered_df['high'],
                low=filtered_df['low'],
                close=filtered_df['close'])])

    # Configure X-Axis to remove gaps (Concept: Rangebreaks for weekends/hours)
    # However, NIFTY holidays vary.
    # Simple workaround for gaps: Use 'category' axis type if we just want sequential candles?
    # Let's try simpler first: Just basic range slider off.
    
    # Adding Rangebreaks for standard Indian Market gaps
    # Market Hours: 9:15 to 15:30. 
    # Overnight gap: 15:30 to 9:15 next day.
    # Weekends.
    
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["Sat", "Mon"]), # hide weekends
            dict(values=["2020-01-01", "2020-01-02"]) # Just example, hard to know all holidays dynamically
            # dict(bounds=[15.5, 9.25], pattern="hour"), # hide overnight
        ]
    )
    
    # Actually, simpler visual debugging: 
    # If the user sees "White Space" between candles, it is usually nights/weekends.
    # If using 'category' axis (no gaps), we lose time linearity but see continuity.
    
    # Let's allow user to toggle "Remove Gaps"
    remove_gaps = st.sidebar.checkbox("Remove Non-Trading Gaps (Category Axis)", value=True)
    
    if remove_gaps:
        fig.update_xaxes(type='category')
        # We need to limit tick labels if using category, otherwise too crowded
        # Plotly handles auto-ticks usually ok.

    fig.update_layout(xaxis_rangeslider_visible=False, height=600, title=f"{selected_file} Price Chart")
    st.plotly_chart(fig, use_container_width=True)

    # 5. Data Table
    with st.expander("See Raw Data"):
        st.dataframe(filtered_df.sort_values(by='datetime', ascending=False))
