#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 20:54:44 2025

@author: charlesbeck
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import altair as alt
import plotly.graph_objects as go
from datetime import datetime, timedelta

supabase_url = "https://fzkeftdzgseugijplhsh.supabase.co"
supabase_key = st.secrets["supabase_key"]

st.set_page_config(
    page_title="Tristero's Mach Exchange",  # Sets the headline/title in the browser tab
    page_icon=":rocket:",           # Optional: Adds an icon to the tab
    layout="wide"                   # Optional: Adjusts layout
)

st.cache_data.clear()
st.cache_resource.clear()

@st.cache_data
def execute_sql(query):
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json"
    }
    # Endpoint for the RPC function
    rpc_endpoint = f"{supabase_url}/rest/v1/rpc/execute_sql"
        
    # Payload with the SQL query
    payload = {"query": query}
        
    # Make the POST request to the RPC function
    response = requests.post(rpc_endpoint, headers=headers, json=payload)
        
    # Handle response
    if response.status_code == 200:
        data = response.json()
            
        df = pd.DataFrame(data)
            
        print("Query executed successfully, returning DataFrame.")
        return(df)
    else:
        print("Error executing query:", response.status_code, response.json())

time_query = """
SELECT MIN(op.block_timestamp) AS oldest_time
FROM order_placed op
INNER JOIN match_executed me
ON op.order_uuid = me.order_uuid
"""

time_point = execute_sql(time_query)
time_point = pd.json_normalize(time_point['result'])

# Time range options
time_ranges = {
    "All Time": None,  # Special case for no date filter
    "Last Week": 7,
    "Last Month": 30,
    "Last 3 Months": 90,
    "Last 6 Months": 180
}

day_list = [7,30,90,180]

# Add custom CSS to adjust width
st.markdown(
    """
    <style>
    /* Style the main content area */
    .main {
        max-width: 95%;
        margin: auto;
        padding-top: 0px; /* Adjust the top padding */
        background-color: black; /* Set the background color to black */
        color: white; /* Ensure text is visible on black background */
    }

    /* Custom header styling */
    header {
        display: flex;
        justify-content: center;
        align-items: center;
        height: 60px;
        background-color: white; /* Slightly lighter black for header */
        border-bottom: 1px solid #333; /* Subtle border for separation */
    }
    header h1 {
        font-size: 20px;
        margin: 0;
        padding: 0;
        color: green; /* White text for header title */
    }
    </style>
    <header>
        <h1>Mach By Tristero</h1>
    </header>
    """,
    unsafe_allow_html=True,
)
st.title("Mach Exchange Volume Dashboard")
# Get today's date
today = datetime.now()


@st.cache_data
def chain_fetch():
    chain_query = f"""
    WITH consolidated_volumes AS (
    SELECT
        chain,
        SUM(volume) AS total_volume
    FROM (
        SELECT
            source_chain AS chain,
            SUM(source_volume) AS volume
        FROM main_volume_table
        GROUP BY source_chain
        UNION ALL
        SELECT
            dest_chain AS chain,
            SUM(dest_volume) AS volume
        FROM main_volume_table
        GROUP BY dest_chain
    ) combined
    GROUP BY chain
    )
    SELECT chain
    FROM consolidated_volumes
    WHERE chain <> ''
    ORDER BY total_volume DESC
    """
    
    chain_list = execute_sql(chain_query)
    chain_list = pd.json_normalize(chain_list['result'])['chain'].tolist()
    return(chain_list)


@st.cache_data
def chain_fetch_day():

    chain_query_day = f"""
    WITH consolidated_volumes AS (
    SELECT
        chain,
        SUM(volume) AS total_volume
    FROM (
        SELECT
            source_chain AS chain,
            SUM(source_volume) AS volume
        FROM main_volume_table
        WHERE DATE(block_timestamp) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY source_chain
        UNION ALL
        SELECT
            dest_chain AS chain,
            SUM(dest_volume) AS volume
        FROM main_volume_table
        WHERE DATE(block_timestamp) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY dest_chain
    ) combined
    GROUP BY chain
    )
    SELECT chain
    FROM consolidated_volumes
    WHERE chain <> ''
    ORDER BY total_volume DESC
    """
    
    chain_list = execute_sql(chain_query_day)
    chain_list = pd.json_normalize(chain_list['result'])['chain'].tolist()
    return(chain_list)

@st.cache_data
def get_volume_vs_date_chain(chain_id, sd):

    if chain_id != 'Total':

        query = f"""
        SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_id = 'solana' AND svt.dest_id = 'solana' THEN svt.total_volume / 2
                    ELSE svt.total_volume
                END
            ), 0) AS total_daily_volume,
            '{chain_id}' AS chain
        FROM main_volume_table svt
        WHERE svt.source_chain = '{chain_id}' OR svt.dest_chain = '{chain_id}'
        GROUP BY DATE_TRUNC('day', svt.block_timestamp)
        ORDER BY DATE_TRUNC('day', svt.block_timestamp)
        """

    else:

        query = f"""
        SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(svt.total_volume), 0) AS total_daily_volume,
            'Total' AS chain
        FROM main_volume_table svt
        GROUP BY DATE_TRUNC('day', svt.block_timestamp)
        ORDER BY DATE_TRUNC('day', svt.block_timestamp)
        """

    return pd.json_normalize(execute_sql(query)['result'])

@st.cache_data
def get_last_day_chain(chain_id, sd):

    if chain_id != 'Total':

        query = f"""
       WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(block_timestamp)) AS max_date
            FROM main_volume_table
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.block_timestamp), 'HH12 AM') AS hour,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_chain = '{chain_id}' AND svt.dest_chain = '{chain_id}' THEN svt.total_volume / 2
                    ELSE svt.total_volume
                END
            ), 0) AS total_hourly_volume,
            '{chain_id}' AS chain
        FROM main_volume_table svt
        WHERE (svt.source_chain = '{chain_id}' OR svt.dest_chain = '{chain_id}')
        AND svt.block_timestamp >= (
            SELECT max_date - INTERVAL '1 day' 
            FROM latest_date
        )
        AND svt.block_timestamp < (
            SELECT max_date 
            FROM latest_date
        )
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp)
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp)
        """
    
    else:

        query = f"""
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'), 'HH12 AM') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            '{chain_id}' AS chain
        FROM main_volume_table svt
        WHERE svt.block_timestamp >= (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
            )
        AND svt.block_timestamp < (NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        """

    return pd.json_normalize(execute_sql(query)['result'])

def assign_dates_to_df(hours_series):

    assigned_dates = []
    current_date = today - timedelta(days=1)  # Start with "yesterday"
    prev_time = None  # To track the previous time
    
    for hour_str in hours_series:
        # Parse the hour string into a time object
        current_time = datetime.strptime(hour_str, "%I %p").time()

        # If it's a transition from PM to AM, increment the date
        if prev_time and current_time < prev_time:
            current_date += timedelta(days=1)

        # Combine the current date with the parsed time and store it
        assigned_dates.append(datetime.combine(current_date, current_time))
        prev_time = current_time  # Update the previous time tracker

    return assigned_dates

time_ranges_chain = {
    "Day" : None,
    "Week": 7,
    "Month": 30
}
chain_list = chain_fetch()
#chain_list = chain_list[:8]
chain_list = ['Total'] + chain_list

chain_list_day = chain_fetch_day()

chain_list_day = ['Total']+ chain_list_day

chain_number_list = [7,30]

if "preloaded_chain" not in st.session_state:
    preloaded_chain = {}
    for chain in chain_list:

        
        #daily_vol_ch = get_last_day_chain(chain, time_point['oldest_time'][0])
        chain_vol = get_volume_vs_date_chain(chain, time_point['oldest_time'][0])

        #preloaded_chain[chain + " Day Volume"] = daily_vol_ch
        preloaded_chain[chain + " Volume Data"] = chain_vol

    for chain in chain_list_day:


        daily_vol_ch = get_last_day_chain(chain, time_point['oldest_time'][0])
        preloaded_chain[chain + " Day Volume"] = daily_vol_ch

    st.session_state["preloaded_chain"] = preloaded_chain
    

selected_range_chain = st.selectbox("Select a time range for the chain display:", list(time_ranges_chain.keys()))
if time_ranges_chain[selected_range_chain] is not None:
    selected_chain = st.selectbox("Select a chain for the chain display:", chain_list)
    
else:
    selected_chain = st.selectbox("Select a chain for the chain display:", chain_list_day)



if time_ranges_chain[selected_range_chain] is not None:

    data = st.session_state["preloaded_chain"][selected_chain + " Volume Data"]
    
    date = today - timedelta(days=time_ranges_chain[selected_range_chain])
    date = date.strftime('%Y-%m-%dT%H:%M:%S')
    
    data = data[pd.to_datetime(data['day']) > pd.to_datetime(date)]

    if data.empty:
        
        st.warning(f"No data available for {selected_chain}!")
        
    else:
        # Add the 'asset' column (asset name is already included in 'data')

        data['day'] = pd.to_datetime(data['day'])
        # Pivot the data to have separate columns for each asset
        pivot_data = data.pivot(index='day', columns='chain', values='total_daily_volume')
        pivot_data = pivot_data.fillna(0)
        pivot_data = pivot_data.reset_index()
        # Reset index to make it Plotly-compatible
    
        # Melt the data back into long format for Plotly
        melted_data = pivot_data.melt(id_vars='day', var_name='chain', value_name='Total Daily Volume')
    
        # Create an interactive bar chart with Plotly
        fig = px.bar(
            melted_data,
            x='day',
            y='Total Daily Volume',
            color='chain',
            title="Volume In The Last Week",
            labels={'day': 'Date', 'Total Daily Volume': 'Volume'},
            hover_data={'day': '|%Y-%m-%d', 'Total Daily Volume': True, 'chain': True},
        )
    
        # Update layout for better readability
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Volume",
            legend_title="Chain",
            hovermode="x unified",
        )
    
        # Render the chart in Streamlit
        st.plotly_chart(fig, use_container_width=True)
    

else:

    data = st.session_state["preloaded_chain"][selected_chain + " Day Volume"]
    # Apply the function to the 'hour' column
    #data['date'] = data['hour'].apply(apply_datetime_conversion)
    
    if data.empty:
        st.warning(f"No data available for {selected_chain}!")
    else:
        # Add the 'asset' column (asset name is already included in 'data')
        data['date'] = assign_dates_to_df(data['hour'])
    
    #all_assets_data_hour['hour'] = pd.to_datetime(all_assets_data_hour['hour'])
    # Pivot the data to have separate columns for each asset
    
        pivot_data = data.pivot(index='date', columns='chain', values='total_hourly_volume')
        pivot_data = pivot_data.fillna(0)
        pivot_data = pivot_data.reset_index()
        # Melt the data back into long format for Plotly
        
        
        melted_data = pivot_data.melt(id_vars=['date'], var_name='chain', value_name='total_hourly_volume')
    
        # Create an interactive bar chart with Plotly
        fig = px.bar(
            melted_data,
            x='date',
            y='total_hourly_volume',
            color='chain',
            title="Volume By Hour For Latest Calendar Day of Active Trading",
            labels={'date': 'Date & Time', 'total_hourly_volume': 'Volume'},
            hover_data={'date': '|%Y-%m-%d %H:%M:%S', 'total_hourly_volume': True, 'chain': True},
        )
    
        # Update layout for better readability
        fig.update_layout(
            xaxis_title="Date & Time",
            yaxis_title="Volume",
            legend_title="Chain",
            hovermode="x unified",
        )
    
        # Render the chart in Streamlit
        st.plotly_chart(fig, use_container_width=True)
