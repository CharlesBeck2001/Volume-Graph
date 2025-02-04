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
        -- Aggregate source chain volumes over the last 24 hours (Eastern Time)
        SELECT
            source_chain AS chain,
            SUM(source_volume) AS volume
        FROM main_volume_table
        WHERE (block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') >= (
                  NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
              )
          AND (block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') < (
                  NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
              )
        GROUP BY source_chain

        UNION ALL

        -- Aggregate destination chain volumes over the last 24 hours (Eastern Time)
        SELECT
            dest_chain AS chain,
            SUM(dest_volume) AS volume
        FROM main_volume_table
        WHERE (block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') >= (
                  NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
              )
          AND (block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') < (
                  NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
              )
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
def asset_fetch():
    asset_query = f"""
    WITH consolidated_volumes AS (
        SELECT
            id,
            SUM(volume) AS total_volume
        FROM (
            SELECT
                source_id AS id,
                SUM(source_volume) AS volume
            FROM main_volume_table
            GROUP BY source_id
            UNION ALL
            SELECT
                dest_id AS id,
                SUM(dest_volume) AS volume
            FROM main_volume_table
            GROUP BY dest_id
        ) combined
        GROUP BY id
    )
    SELECT id
    FROM consolidated_volumes
    ORDER BY total_volume DESC
    """

    asset_list = execute_sql(asset_query)
    asset_list = pd.json_normalize(asset_list['result'])['id'].tolist()
    return(asset_list)

@st.cache_data
def asset_fetch_day():
    asset_query = f"""
    WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(block_timestamp)) AS max_date
            FROM main_volume_table
    ),
    consolidated_volumes AS (
        SELECT
            id,
            SUM(volume) AS total_volume
        FROM (
            SELECT
                source_id AS id,
                SUM(source_volume) AS volume
            FROM main_volume_table
            WHERE block_timestamp >= (
                SELECT max_date - INTERVAL '1 day' 
                FROM latest_date
            )
            AND block_timestamp < (
                SELECT max_date 
                FROM latest_date
            )     
            GROUP BY source_id
            UNION ALL
            SELECT
                dest_id AS id,
                SUM(dest_volume) AS volume
            FROM main_volume_table
            WHERE block_timestamp >= (
                SELECT max_date - INTERVAL '1 day' 
                FROM latest_date
            )
            AND block_timestamp < (
                SELECT max_date 
                FROM latest_date
            )     
            GROUP BY dest_id
        ) combined
        GROUP BY id
    )
    SELECT id
    FROM consolidated_volumes
    ORDER BY total_volume DESC
    """

    asset_query_2 = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(created_at)) AS max_date
            FROM mm_order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.created_at >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.created_at < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                svt.source_volume AS total_volume
            FROM source_volume_table svt
        ),
        consolidated_volumes AS (
        SELECT
            id,
            SUM(volume) AS total_volume
        FROM (
            SELECT
                source_id AS id,
                SUM(source_volume) AS volume
            FROM overall_volume_table_2
            GROUP BY source_id
        ) combined
        GROUP BY id
    )
    SELECT id
    FROM consolidated_volumes
    ORDER BY total_volume DESC
    """

    asset_query_3 = f"""
    WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(created_at)) AS max_date
            FROM mm_order_placed
        ),
        source_volume_table_0 AS (
        SELECT DISTINCT
          op.*,
          ti.decimals AS source_decimal,
          cal.id AS source_id,
          cal.chain AS source_chain,
          cmd.current_price::FLOAT AS source_price,
          (cmd.current_price::FLOAT * op.source_quantity) / POWER(10, ti.decimals) AS source_volume
        FROM order_placed op
        INNER JOIN match_executed me
          ON op.order_uuid = me.order_uuid
        INNER JOIN token_info ti
          ON op.source_asset = ti.address  -- Get source asset decimals
        INNER JOIN coingecko_assets_list cal
          ON op.source_asset = cal.address
        INNER JOIN coingecko_market_data cmd 
          ON cal.id = cmd.id
        WHERE op.block_timestamp >= (
                SELECT max_date - INTERVAL '1 day'
                FROM latest_date
            )
        AND op.block_timestamp < (
                SELECT max_date
                FROM latest_date
            )
        ),
        dest_volume_table_0 AS (
            SELECT DISTINCT
              op.*,
              me.maker_address AS maker_address,  -- Explicitly include maker_address
              ti.decimals AS dest_decimal,
              cal.id AS dest_id,
              cal.chain AS dest_chain,
              cmd.current_price::FLOAT AS dest_price,
              (cmd.current_price::FLOAT * op.dest_quantity) / POWER(10, ti.decimals) AS dest_volume
            FROM order_placed op
            INNER JOIN match_executed me
              ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
              ON op.dest_asset = ti.address  -- Get source asset decimals
            INNER JOIN coingecko_assets_list cal
              ON op.dest_asset = cal.address
            INNER JOIN coingecko_market_data cmd 
              ON cal.id = cmd.id
            WHERE op.block_timestamp >= (
                SELECT max_date - INTERVAL '1 day'
                FROM latest_date
            )
            AND op.block_timestamp < (
                SELECT max_date
                FROM latest_date
            )
        ),
        overall_volume_table_3 AS (
            SELECT DISTINCT
              svt.*,
              COALESCE(dvt.maker_address, '') AS maker_address,
              COALESCE(dvt.dest_id, '') AS dest_id,
              COALESCE(dvt.dest_chain, '') AS dest_chain,
              COALESCE(dvt.dest_decimal, 0) AS dest_decimal,
              COALESCE(dvt.dest_price, 0) AS dest_price,
              COALESCE(dvt.dest_volume, 0) AS dest_volume,
              CASE 
                WHEN dvt.dest_id IS NULL THEN svt.source_volume * 2
                ELSE (dvt.dest_volume + svt.source_volume)
              END AS total_volume
            FROM source_volume_table_0 svt
            LEFT JOIN dest_volume_table_0 dvt
              ON svt.order_uuid = dvt.order_uuid
        ),
    consolidated_volumes AS (
    SELECT
        id,
        SUM(volume) AS total_volume
    FROM (
        SELECT
            source_id AS id,
            SUM(total_volume) AS volume
        FROM overall_volume_table_3
        GROUP BY source_id
    ) combined
    GROUP BY id
    )
    SELECT id
    FROM consolidated_volumes
    ORDER BY total_volume DESC
    """

    asset_query = f"""
    WITH consolidated_volumes AS (
        SELECT
            id,
            SUM(volume) AS total_volume
        FROM (
            -- Volumes where the asset is the source
            SELECT
                source_id AS id,
                SUM(source_volume) AS volume
            FROM main_volume_table
            WHERE (block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') >= (
                    NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
                )
              AND (block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') < (
                    NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
                )
            GROUP BY source_id
    
            UNION ALL
    
            -- Volumes where the asset is the destination
            SELECT
                dest_id AS id,
                SUM(dest_volume) AS volume
            FROM main_volume_table
            WHERE (block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') >= (
                    NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
                )
              AND (block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') < (
                    NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
                )
            GROUP BY dest_id
        ) combined
        WHERE id IS NOT NULL  -- ✅ Exclude NULL ids before aggregation
        GROUP BY id
    )
    SELECT
        id
    FROM consolidated_volumes
    WHERE id <> ''  -- ✅ Exclude empty string ids
    ORDER BY total_volume DESC
    """
    
    # Execute the query and process results
    asset_list = execute_sql(asset_query)
    asset_list = pd.json_normalize(asset_list['result'])['id'].tolist()
    return asset_list

@st.cache_data
def get_volume_vs_date_chain(chain_id, sd):

    if chain_id != 'Total':

        query = f"""
        SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_chain = '{chain_id}' AND svt.dest_chain = '{chain_id}' 
                        THEN svt.total_volume  -- Count full total volume if both source and dest are the same chain
                    WHEN svt.source_chain = '{chain_id}' 
                        THEN svt.source_volume  -- Count only source volume if it's the source chain
                    WHEN svt.dest_chain = '{chain_id}' 
                        THEN svt.dest_volume  -- Count only dest volume if it's the destination chain
                    ELSE 0
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

# Function to execute query and retrieve data
@st.cache_data
def get_volume_vs_date(asset_id, sd):
    """
    Query the Supabase database to get total volume vs date for a specific asset.

    Args:
        asset_id (str): The asset ID for which to retrieve the volume data.

    Returns:
        pd.DataFrame: A DataFrame containing dates and their corresponding total volumes.
    """
    # SQL query to retrieve volume vs date for the given asset_id
    if asset_id != 'Total':
        query = f"""
        WITH source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.source_quantity) / POWER(10, ti.decimals) AS source_volume
            FROM order_placed op
            INNER JOIN match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.source_asset = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.source_asset = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
        ),
        dest_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS dest_decimal,
                cal.id AS dest_id,
                cal.chain AS dest_chain,
                cmd.current_price::FLOAT AS dest_price,
                (cmd.current_price::FLOAT * op.dest_quantity) / POWER(10, ti.decimals) AS dest_volume
            FROM order_placed op
            INNER JOIN match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.dest_asset = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.dest_asset = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.order_uuid AS order_id,
                (dvt.dest_volume + svt.source_volume) AS total_volume,
                svt.block_timestamp AS date,
                svt.source_id AS source_asset,
                dvt.dest_id AS dest_asset
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        ),
        source_volume_table_3 AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
        ),
        overall_volume_table_3 AS (
            SELECT DISTINCT
                svt.order_uuid AS order_id,
                2*svt.source_volume AS total_volume,
                svt.created_at AS date,
                svt.source_id AS source_asset,
                '' AS dest_asset
            FROM source_volume_table_3 svt
        ),
        combined_volume_table AS (
            SELECT DISTINCT
                * 
            FROM overall_volume_table_2
            UNION
            SELECT DISTINCT
                * 
            FROM overall_volume_table_3
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.date), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(svt.total_volume), 0) AS total_daily_volume,
            '{asset_id}' AS asset
        FROM combined_volume_table svt
        WHERE svt.source_asset = '{asset_id}' OR svt.dest_asset = '{asset_id}'
        GROUP BY DATE_TRUNC('day', svt.date)
        ORDER BY DATE_TRUNC('day', svt.date)
        """

        query_2 = f"""
        SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(svt.total_volume), 0) AS total_daily_volume,
            '{asset_id}' AS asset
        FROM main_volume_table svt
        WHERE svt.source_id = '{asset_id}' OR svt.dest_id = '{asset_id}'
        GROUP BY DATE_TRUNC('day', svt.block_timestamp)
        ORDER BY DATE_TRUNC('day', svt.block_timestamp)
        """

        query_3 = f"""
         SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_id = '{asset_id}' AND svt.dest_id = '{asset_id}' 
                        THEN svt.total_volume  -- Entire volume is counted once
                    WHEN svt.source_id = '{asset_id}' 
                        THEN svt.source_volume  -- Only count source volume
                    WHEN svt.dest_id = '{asset_id}' 
                        THEN svt.dest_volume  -- Only count destination volume
                    ELSE 0
                END
            ), 0) AS total_daily_volume,
            '{asset_id}' AS asset
        FROM main_volume_table svt
        WHERE svt.source_id = '{asset_id}' OR svt.dest_id = '{asset_id}'
        GROUP BY DATE_TRUNC('day', svt.block_timestamp)
        ORDER BY DATE_TRUNC('day', svt.block_timestamp)
        """
        
    else:

        query = f"""
        WITH source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.source_quantity) / POWER(10, ti.decimals) AS source_volume
            FROM order_placed op
            INNER JOIN match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.source_asset = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.source_asset = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
        ),
        dest_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS dest_decimal,
                cal.id AS dest_id,
                cal.chain AS dest_chain,
                cmd.current_price::FLOAT AS dest_price,
                (cmd.current_price::FLOAT * op.dest_quantity) / POWER(10, ti.decimals) AS dest_volume
            FROM order_placed op
            INNER JOIN match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.dest_asset = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.dest_asset = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.order_uuid AS order_id,
                (dvt.dest_volume + svt.source_volume) AS total_volume,
                svt.block_timestamp AS date,
                svt.source_id AS source_asset,
                dvt.dest_id AS dest_asset
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        ),
        source_volume_table_3 AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
        ),
        overall_volume_table_3 AS (
            SELECT DISTINCT
                svt.order_uuid AS order_id,
                2*svt.source_volume AS total_volume,
                svt.created_at AS date,
                svt.source_id AS source_asset,
                '' AS dest_asset
            FROM source_volume_table_3 svt
        ),
        combined_volume_table AS (
            SELECT DISTINCT
                * 
            FROM overall_volume_table_2
            UNION
            SELECT DISTINCT
                * 
            FROM overall_volume_table_3
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.date), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(svt.total_volume), 0) AS total_daily_volume,
            '{asset_id}' AS asset
        FROM combined_volume_table svt
        GROUP BY DATE_TRUNC('day', svt.date)
        ORDER BY DATE_TRUNC('day', svt.date)
        """

        query_2 = f"""
        SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(svt.total_volume), 0) AS total_daily_volume,
            '{asset_id}' AS asset
        FROM main_volume_table svt
        GROUP BY DATE_TRUNC('day', svt.block_timestamp)
        ORDER BY DATE_TRUNC('day', svt.block_timestamp)
        """

        query_3 = f"""
        SELECT 
            TO_CHAR(DATE_TRUNC('day', svt.block_timestamp), 'FMMonth FMDD, YYYY') AS day,
            COALESCE(SUM(svt.total_volume), 0) AS total_daily_volume,
            '{asset_id}' AS asset
        FROM main_volume_table svt
        GROUP BY DATE_TRUNC('day', svt.block_timestamp)
        ORDER BY DATE_TRUNC('day', svt.block_timestamp)
        """

    #st.write(asset_id)
    # Execute the query and return the result as a DataFrame
    return pd.json_normalize(execute_sql(query_3)['result'])

def get_last_day_chain(chain_id, sd):

    if chain_id != 'Total':

        query_0 = f"""
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

        query_1 = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')) AS max_date
            FROM main_volume_table
        )
        SELECT 
            TO_CHAR(
                DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'),
                'HH12 AM'
            ) AS hour,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_chain = '{chain_id}' AND svt.dest_chain = '{chain_id}' 
                        THEN svt.total_volume / 2
                    ELSE svt.total_volume
                END
            ), 0) AS total_hourly_volume,
            '{chain_id}' AS chain
        FROM main_volume_table svt
        WHERE (svt.source_chain = '{chain_id}' OR svt.dest_chain = '{chain_id}')
          AND (svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') >= (
                SELECT max_date - INTERVAL '1 day'
                FROM latest_date
            )
          AND (svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') < (
                SELECT max_date
                FROM latest_date
            )
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        """

        query_1 = f"""
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'), 'HH12 AM') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            '{chain_id}' AS chain
        FROM main_volume_table svt
        WHERE (svt.source_chain = '{chain_id}' OR svt.dest_chain = '{chain_id}')
          AND svt.block_timestamp >= (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
            )
        AND svt.block_timestamp < (NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        """

        query_old = f"""
        SELECT 
            TO_CHAR(
                DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'),
                'HH12 AM'
            ) AS hour,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_id = '{chain_id}' AND svt.dest_id = '{chain_id}' 
                        THEN svt.total_volume / 2
                    ELSE svt.total_volume
                END
            ), 0) AS total_hourly_volume,
            '{chain_id}' AS chain
        FROM main_volume_table svt
        WHERE (svt.source_chain = '{chain_id}' OR svt.dest_chain = '{chain_id}')
          AND svt.block_timestamp >= (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
            )
          AND svt.block_timestamp < (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
            )
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        """

        query_old_2 = f"""
        SELECT 
            TO_CHAR(
                DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'),
                'HH12 AM'
            ) AS hour,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_chain = '{chain_id}' AND svt.dest_chain = '{chain_id}' 
                        THEN svt.total_volume  -- Entire volume is counted once
                    WHEN svt.source_chain = '{chain_id}' 
                        THEN svt.source_volume  -- Only count source volume
                    WHEN svt.dest_chain = '{chain_id}' 
                        THEN svt.dest_volume  -- Only count destination volume
                    ELSE 0
                END
            ), 0) AS total_hourly_volume,
            '{chain_id}' AS chain
        FROM main_volume_table svt
        WHERE (svt.source_chain = '{chain_id}' OR svt.dest_chain = '{chain_id}')
          AND svt.block_timestamp >= (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
            )
          AND svt.block_timestamp < (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
            )
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        """

        query = f"""
        SELECT 
            DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS hour,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_chain = '{chain_id}' AND svt.dest_chain = '{chain_id}' 
                        THEN svt.total_volume  -- Entire volume is counted once
                    WHEN svt.source_chain = '{chain_id}' 
                        THEN svt.source_volume  -- Only count source volume
                    WHEN svt.dest_chain = '{chain_id}' 
                        THEN svt.dest_volume  -- Only count destination volume
                    ELSE 0
                END
            ), 0) AS total_hourly_volume,
            '{chain_id}' AS chain
        FROM main_volume_table svt
        WHERE (svt.source_chain = '{chain_id}' OR svt.dest_chain = '{chain_id}')
          AND svt.block_timestamp >= (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
            )
          AND svt.block_timestamp < (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
            )
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY hour
        """
    
    else:

        query = f"""
        SELECT 
            DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            'Total' AS chain
        FROM main_volume_table svt
        WHERE svt.block_timestamp >= (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
            )
          AND svt.block_timestamp < (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
            )
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        """
        
        query_old_2 = f"""
        SELECT 
            TO_CHAR(
                DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'),
                'HH12 AM'
            ) AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            'Total' AS chain
        FROM main_volume_table svt
        WHERE svt.block_timestamp >= (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
            )
          AND svt.block_timestamp < (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
            )
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        """

        query_old = f"""
        SELECT 
            TO_CHAR(
                DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'),
                'HH12 AM'
            ) AS hour,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_chain = svt.dest_chain THEN svt.total_volume / 2
                    ELSE svt.total_volume
                END
            ), 0) AS total_hourly_volume,
            'Total' AS chain
        FROM main_volume_table svt
        WHERE svt.block_timestamp >= (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
            )
          AND svt.block_timestamp < (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
            )
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        """

    return pd.json_normalize(execute_sql(query)['result'])
        

def get_last_day(asset_id, sd):

    if asset_id != 'Total':

        query = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(block_timestamp)) AS max_date
            FROM order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.source_quantity) / POWER(10, ti.decimals) AS source_volume
            FROM order_placed op
            INNER JOIN match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.source_asset = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.source_asset = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.block_timestamp >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.block_timestamp < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        dest_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS dest_decimal,
                cal.id AS dest_id,
                cal.chain AS dest_chain,
                cmd.current_price::FLOAT AS dest_price,
                (cmd.current_price::FLOAT * op.dest_quantity) / POWER(10, ti.decimals) AS dest_volume
            FROM order_placed op
            INNER JOIN match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.dest_asset = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.dest_asset = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.block_timestamp >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.block_timestamp < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                dvt.dest_id AS dest_id,
                dvt.dest_chain AS dest_chain,
                dvt.dest_decimal AS dest_decimal,
                dvt.dest_price AS dest_price,
                dvt.dest_volume AS dest_volume,
                (dvt.dest_volume + svt.source_volume) AS total_volume
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.block_timestamp), 'HH12 AM') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            '{asset_id}' AS asset
        FROM overall_volume_table_2 svt
        WHERE svt.source_id = '{asset_id}' OR svt.dest_id = '{asset_id}'
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp)
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp)
        """

        query_2 = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(created_at)) AS max_date
            FROM mm_order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.created_at >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.created_at < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                2*svt.source_volume AS total_volume
            FROM source_volume_table svt
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.created_at), 'HH12 AM') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            '{asset_id}' AS asset
        FROM overall_volume_table_2 svt
        WHERE svt.source_id = '{asset_id}'
        GROUP BY DATE_TRUNC('hour', svt.created_at)
        ORDER BY DATE_TRUNC('hour', svt.created_at)
        """

        query_4 = f"""
       WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(block_timestamp)) AS max_date
            FROM main_volume_table
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.block_timestamp), 'HH12 AM') AS hour,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_id = '{asset_id}' AND svt.dest_id = '{asset_id}' THEN svt.total_volume / 2
                    ELSE svt.total_volume
                END
            ), 0) AS total_hourly_volume,
            '{asset_id}' AS asset
        FROM main_volume_table svt
        WHERE (svt.source_id = '{asset_id}' OR svt.dest_id = '{asset_id}')
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

        query_3 = f"""
        SELECT 
            DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS hour,
            COALESCE(SUM(
                CASE 
                    WHEN svt.source_id = '{asset_id}' AND svt.dest_id = '{asset_id}' 
                        THEN svt.total_volume  -- Entire volume is counted once
                    WHEN svt.source_id = '{asset_id}' 
                        THEN svt.source_volume  -- Only count source volume
                    WHEN svt.dest_id = '{asset_id}' 
                        THEN svt.dest_volume  -- Only count destination volume
                    ELSE 0
                END
            ), 0) AS total_hourly_volume,
            '{asset_id}' AS asset
        FROM main_volume_table svt
        WHERE (svt.source_id = '{asset_id}' OR svt.dest_id = '{asset_id}')
        AND svt.block_timestamp >= (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
            )
        AND svt.block_timestamp < (NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        """
        
    else:
            
        query = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(block_timestamp)) AS max_date
            FROM order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.source_quantity) / POWER(10, ti.decimals) AS source_volume
            FROM order_placed op
            INNER JOIN match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.source_asset = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.source_asset = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.block_timestamp >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.block_timestamp < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        dest_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS dest_decimal,
                cal.id AS dest_id,
                cal.chain AS dest_chain,
                cmd.current_price::FLOAT AS dest_price,
                (cmd.current_price::FLOAT * op.dest_quantity) / POWER(10, ti.decimals) AS dest_volume
            FROM order_placed op
            INNER JOIN match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.dest_asset = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.dest_asset = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.block_timestamp >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.block_timestamp < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                dvt.dest_id AS dest_id,
                dvt.dest_chain AS dest_chain,
                dvt.dest_decimal AS dest_decimal,
                dvt.dest_price AS dest_price,
                dvt.dest_volume AS dest_volume,
                (dvt.dest_volume + svt.source_volume) AS total_volume
            FROM source_volume_table svt
            INNER JOIN dest_volume_table dvt
                ON svt.order_uuid = dvt.order_uuid
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.block_timestamp), 'HH12 AM') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            '{asset_id}' AS asset
        FROM overall_volume_table_2 svt
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp)
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp)
        """

        query_2 = f"""
        WITH latest_date AS (
            SELECT DATE_TRUNC('day', MAX(created_at)) AS max_date
            FROM mm_order_placed
        ),
        source_volume_table AS (
            SELECT DISTINCT
                op.*, 
                ti.decimals AS source_decimal,
                cal.id AS source_id,
                cal.chain AS source_chain,
                cmd.current_price::FLOAT AS source_price,
                (cmd.current_price::FLOAT * op.src_amount) / POWER(10, ti.decimals) AS source_volume
            FROM mm_order_placed op
            INNER JOIN mm_match_executed me
                ON op.order_uuid = me.order_uuid
            INNER JOIN token_info ti
                ON op.src_asset_address = ti.address
            INNER JOIN coingecko_assets_list cal
                ON op.src_asset_address = cal.address
            INNER JOIN coingecko_market_data cmd 
                ON cal.id = cmd.id
            WHERE op.created_at >= (
                    SELECT max_date - INTERVAL '1 day' 
                    FROM latest_date
                )
              AND op.created_at < (
                    SELECT max_date 
                    FROM latest_date
                )
        ),
        overall_volume_table_2 AS (
            SELECT DISTINCT
                svt.*,
                2*svt.source_volume AS total_volume
            FROM source_volume_table svt
        )
        SELECT 
            TO_CHAR(DATE_TRUNC('hour', svt.created_at), 'HH12 AM') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            '{asset_id}' AS asset
        FROM overall_volume_table_2 svt
        GROUP BY DATE_TRUNC('hour', svt.created_at)
        ORDER BY DATE_TRUNC('hour', svt.created_at)
        """


        query_3 = f"""
        SELECT 
            DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS hour,
            COALESCE(SUM(svt.total_volume), 0) AS total_hourly_volume,
            'Total' AS asset
        FROM main_volume_table svt
        WHERE svt.block_timestamp >= (
                NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' - INTERVAL '24 hours'
            )
        AND svt.block_timestamp < (NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        GROUP BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        ORDER BY DATE_TRUNC('hour', svt.block_timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        """

    #st.write(pd.json_normalize(execute_sql(query_3)['result']))
    return pd.json_normalize(execute_sql(query_3)['result'])

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

asset_list = asset_fetch()
asset_list = asset_list[:15]
asset_list_2 = asset_list[:5]
asset_list = ['Total'] + asset_list

asset_list_day = asset_fetch_day()
asset_list_day = [asset for asset in asset_list_day if asset != ""]
asset_list_day = asset_list_day[:5]
asset_list_day_2 = asset_list_day
#st.write(asset_list_day_2)
asset_list_day = ['Total'] + asset_list_day

#st.write(asset_list_day)

if "preloaded_2" not in st.session_state:
    preloaded_2 = {}
    for asset in asset_list:
        
        daily_vol = get_volume_vs_date(asset, time_point['oldest_time'][0])
        #weekly_vol = get_weekly_volume_vs_date(asset, time_point['oldest_time'][0])
    
        preloaded_2[asset + ' Weekly Average'] = weekly_vol
        preloaded_2[asset + ' Daily Value'] = daily_vol
    
    for asset in asset_list_day:

        st.write(asset)
        hourly_vol = get_last_day(asset, time_point['oldest_time'][0])
        preloaded_2[asset + ' Hourly Value'] = hourly_vol
    
        date = today - timedelta(days=6)
        date = date.strftime('%Y-%m-%dT%H:%M:%S')
        
        week_vol = get_volume_vs_date(asset, date)
        preloaded_2[asset + ' Week Volume'] = week_vol

    st.session_state["preloaded_2"] = preloaded_2


time_ranges_2 = {
    "All Time": None,  # Special case for no date filter
    "Last Month": 30,
    "Last 3 Months": 90,
    "Last 6 Months": 180
}


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
comment_out = '''
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
'''

if time_ranges_chain[selected_range_chain] is not None:
    # -------------------------------
    # DAILY VOLUME DATA (e.g. Week or Month)
    # -------------------------------
    data_list = []
    # Loop through every chain to get its volume data
    for chain in chain_list_def:
        chain_data = st.session_state["preloaded_chain"][chain + " Volume Data"].copy()
        
        # Define the cutoff date based on the selected range
        date_cutoff = today - timedelta(days=time_ranges_chain[selected_range_chain])
        date_cutoff = date_cutoff.strftime('%Y-%m-%dT%H:%M:%S')
    
        # Filter the chain's data based on the cutoff date
        chain_data = chain_data[pd.to_datetime(chain_data['day']) > pd.to_datetime(date_cutoff)]
        chain_data["chain"] = chain  # Ensure chain name is present in data
        data_list.append(chain_data)
    
    # Concatenate all chains’ data
    data = pd.concat(data_list, ignore_index=True)
    
    if data.empty:
        st.warning("No data available for the selected time range!")
    else:
        # Ensure the date column is datetime
        data['day'] = pd.to_datetime(data['day'])
    
        # Calculate total volume per day
        total_volume_per_day = data.groupby("day")["total_daily_volume"].sum().reset_index()
        total_volume_per_day.rename(columns={"total_daily_volume": "Total Volume"}, inplace=True)
    
        # Merge total volume back into data
        data = data.merge(total_volume_per_day, on="day", how="left")
    
        # 🔹 Use a more saturated color palette (e.g., Set1 or Dark2)
        unique_chains = data["chain"].unique()
        color_palette = px.colors.qualitative.Set1  # Stronger, more saturated colors
    
        if len(unique_chains) > len(color_palette):  
            extra_needed = len(unique_chains) - len(color_palette)
            if extra_needed > 0:  # ✅ Prevent division by zero
                extra_colors = px.colors.sample_colorscale("Rainbow", [i / extra_needed for i in range(extra_needed)])
                color_palette.extend(extra_colors)
    
        # Create color mapping for each chain
        color_map = {chain: color_palette[i % len(color_palette)] for i, chain in enumerate(unique_chains)}
    
        # Create a stacked bar chart using Plotly Express
        fig = px.bar(
            data,
            x='day',
            y='total_daily_volume',
            color='chain',
            title="Volume In The Last Week/Month",
            labels={'day': 'Date', 'total_daily_volume': 'Volume'},
            hover_data={'day': '|%Y-%m-%d', 'total_daily_volume': ':,.0f', 'chain': True, 'Total Volume': ':,.0f'},
            color_discrete_map=color_map,  # Assign unique colors
        )
    
        # Update layout to stack the bars
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Volume",
            legend_title="Chain",
            hovermode="closest",  # Ensures only the section under the cursor is shown
            barmode="stack"
        )
    
        st.plotly_chart(fig, use_container_width=True)

else:
    # -------------------------------
    # HOURLY VOLUME DATA (Day)
    # -------------------------------
    data_list = []
    for chain in chain_list_day_def:
        chain_data = st.session_state["preloaded_chain"][chain + " Day Volume"].copy()
        if not chain_data.empty:
            chain_data['date'] = chain_data['hour']
        data_list.append(chain_data)
    
    data = pd.concat(data_list, ignore_index=True)
    
    if data.empty:
        st.warning("No hourly data available for the latest day!")
    else:
        # Pivot to get total hourly volume per chain
        pivot_data = data.pivot(index='date', columns='chain', values='total_hourly_volume').fillna(0)
        
        # Compute total volume per hour
        pivot_data['total_volume'] = pivot_data.sum(axis=1)
        
        # Convert index to a column for plotting
        pivot_data = pivot_data.reset_index()
    
        # 🔹 Use a more saturated color palette (e.g., Set1 or Dark2)
        unique_chains = pivot_data.columns[1:-1]  # Exclude date and total_volume columns
        color_palette = px.colors.qualitative.Set1  # Stronger, more saturated colors
    
        if len(unique_chains) > len(color_palette):  
            extra_needed = len(unique_chains) - len(color_palette)
            if extra_needed > 0:  # ✅ Prevent division by zero
                extra_colors = px.colors.sample_colorscale("Rainbow", [i / extra_needed for i in range(extra_needed)])
                color_palette.extend(extra_colors)
    
        # Create color mapping for each chain
        color_map = {chain: color_palette[i % len(color_palette)] for i, chain in enumerate(unique_chains)}
    
        # Create figure manually using go.Figure()
        fig = go.Figure()
    
        # Add each chain as a stacked bar segment
        for chain in pivot_data.columns[1:-1]:  # Skip 'date' and 'total_volume'
            fig.add_trace(go.Bar(
                x=pivot_data['date'],
                y=pivot_data[chain],
                name=chain,
                customdata=pivot_data[['total_volume']],  # Attach total volume for the hour
                hoverinfo="x+y",  # Show only the specific section hovered
                hovertemplate="<b>Chain:</b> %{fullData.name}<br>"
                              "<b>Volume:</b> %{y}<br>"
                              "<b>Total Hourly Volume:</b> %{customdata[0]}<br>"
                              "<extra></extra>",  # Remove extra trace info
                marker=dict(color=color_map[chain])  # Assign unique color
            ))
    
        # Configure layout
        fig.update_layout(
            title="Volume By Hour For Latest Calendar Day of Active Trading",
            xaxis_title="Date & Time",
            yaxis_title="Volume",
            legend_title="Chain",
            barmode="stack",  # Keep stacked format
            hovermode="closest",  # Fix: Only show the specific section hovered
        )
    
        # Show the chart
        st.plotly_chart(fig, use_container_width=True)




if time_ranges_chain[selected_range_chain] is not None:
    # -------------------------------
    # DAILY VOLUME DATA (e.g. Week or Month)
    # -------------------------------
    # Load total volume data
    data_total = st.session_state["preloaded_chain"]["Total Volume Data"]
    
    # Define date cutoff based on selected range
    date_cutoff = today - timedelta(days=time_ranges_chain[selected_range_chain])
    date_cutoff = date_cutoff.strftime('%Y-%m-%dT%H:%M:%S')
    
    # Filter total volume data
    data_total = data_total[pd.to_datetime(data_total['day']) > pd.to_datetime(date_cutoff)]
    data_total['day'] = pd.to_datetime(data_total['day'])
    
    # Load asset-specific volume data
    data_list = []
    for asset in asset_list_2:
        asset_data = st.session_state['preloaded_2'][asset + ' Daily Value'].copy()
    
        # Filter based on cutoff date
        asset_data = asset_data[pd.to_datetime(asset_data['day']) > pd.to_datetime(date_cutoff)]
        asset_data["asset"] = asset  # Ensure asset name is included
        data_list.append(asset_data)
    
    # Concatenate asset volume data
    data = pd.concat(data_list, ignore_index=True)
    
    if data.empty or data_total.empty:
        st.warning("No data available for the selected time range!")
    else:
        # Convert 'day' column to datetime
        data['day'] = pd.to_datetime(data['day'])
        
        # Compute total volume per day from asset data
        asset_total_per_day = data.groupby("day")["total_daily_volume"].sum().reset_index()
        asset_total_per_day.rename(columns={"total_daily_volume": "Asset Total"}, inplace=True)
    
        # Merge with total volume data
        data_total = data_total[["day", "total_daily_volume"]].rename(columns={"total_daily_volume": "Total Volume"})
        merged_data = data_total.merge(asset_total_per_day, on="day", how="left").fillna(0)
    
        # Compute "Other" category as the difference
        merged_data["Other"] = merged_data["Total Volume"] - merged_data["Asset Total"]
        merged_data.loc[merged_data["Other"] < 0, "Other"] = 0  # Ensure no negative values
    
        # Append "Other" category to data
        other_data = merged_data[["day", "Other"]].copy()
        other_data["asset"] = "Other"
        other_data.rename(columns={"Other": "total_daily_volume"}, inplace=True)
    
        data = pd.concat([data, other_data], ignore_index=True)
    
        # Define a strong color palette
        unique_assets = data["asset"].unique()
        color_palette = px.colors.qualitative.Set1  # Stronger colors
    
        if len(unique_assets) > len(color_palette):
            extra_needed = len(unique_assets) - len(color_palette)
            extra_colors = px.colors.sample_colorscale("Rainbow", [i / max(1, extra_needed) for i in range(extra_needed)])
            color_palette.extend(extra_colors)
    
        # Assign unique colors
        color_map = {asset: color_palette[i % len(color_palette)] for i, asset in enumerate(unique_assets)}
    
        # Create stacked bar chart
        fig = px.bar(
            data,
            x="day",
            y="total_daily_volume",
            color="asset",
            title="Volume In The Last Week/Month",
            labels={"day": "Date", "total_daily_volume": "Volume"},
            hover_data={"day": "|%Y-%m-%d", "total_daily_volume": ":,.0f", "asset": True, "total_daily_volume": ":,.0f"},
            color_discrete_map=color_map
        )
    
        # Update layout
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Volume",
            legend_title="Asset",
            hovermode="closest",
            barmode="stack"
        )
    
        st.plotly_chart(fig, use_container_width=True)

else:
    # -------------------------------
    # HOURLY VOLUME DATA (Day)
    # -------------------------------
    # Load total hourly volume data
    data_total = st.session_state["preloaded_chain"]["Total Day Volume"].copy()
    data_total['date'] = data_total['hour']
    
    # Load asset-specific hourly volume data
    data_list = []
    for asset in asset_list_day_2:
        asset_data = st.session_state["preloaded_2"][asset + ' Hourly Value'].copy()
        if not asset_data.empty:
            asset_data['date'] = asset_data['hour']
        data_list.append(asset_data)
    
    # Concatenate asset data
    data = pd.concat(data_list, ignore_index=True)
    
    if data.empty or data_total.empty:
        st.warning("No hourly data available for the latest day!")
    else:
        # Pivot to get total hourly volume per chain
        pivot_data = data.pivot(index='date', columns='asset', values='total_hourly_volume').fillna(0)
    
        # Compute "Other" category as the difference between total recorded volume and the sum of all known assets
        pivot_data["Other"] = data_total.set_index("date")["total_hourly_volume"] - pivot_data.sum(axis=1)
        pivot_data["Other"] = pivot_data["Other"].clip(lower=0)  # Ensure no negative values
    
        # Compute total volume per hour
        pivot_data["total_volume"] = pivot_data.sum(axis=1)
    
        # Convert index to a column for plotting
        pivot_data = pivot_data.reset_index()
    
        # Define color palette
        unique_assets = pivot_data.columns[1:-1]  # Exclude 'date' and 'total_volume' columns
        color_palette = px.colors.qualitative.Set1
    
        if len(unique_assets) > len(color_palette):
            extra_needed = len(unique_assets) - len(color_palette)
            extra_colors = px.colors.sample_colorscale("Rainbow", [i / max(1, extra_needed) for i in range(extra_needed)])
            color_palette.extend(extra_colors)
    
        # Assign unique colors
        color_map = {asset: color_palette[i % len(color_palette)] for i, asset in enumerate(unique_assets)}
    
        # Create figure manually using go.Figure()
        fig = go.Figure()
    
        # Add each asset as a stacked bar segment
        for asset in pivot_data.columns[1:-1]:  # Skip 'date' and 'total_volume'
            fig.add_trace(go.Bar(
                x=pivot_data['date'],
                y=pivot_data[asset],
                name=asset,
                customdata=pivot_data[['total_volume']],  # Attach total volume for the hour
                hoverinfo="x+y",  # Show only the specific section hovered
                hovertemplate="<b>Chain:</b> %{fullData.name}<br>"
                              "<b>Volume:</b> %{y}<br>"
                              "<b>Total Hourly Volume:</b> %{customdata[0]}<br>"
                              "<extra></extra>",  # Remove extra trace info
                marker=dict(color=color_map[asset])
            ))
    
        # Configure layout
        fig.update_layout(
            title="Volume By Hour For Latest Calendar Day of Active Trading",
            xaxis_title="Date & Time",
            yaxis_title="Volume",
            legend_title="Asset",
            barmode="stack",
            hovermode="closest",
        )
    
        # Show the chart
        st.plotly_chart(fig, use_container_width=True)
