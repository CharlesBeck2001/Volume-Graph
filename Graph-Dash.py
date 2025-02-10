import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import altair as alt
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np

# Retrieve secrets from the secrets.toml file via st.secrets
supabase_url = st.secrets["url"]
supabase_key = st.secrets["key"]

st.set_page_config(
    page_title="Tristero's Mach Exchange",
    page_icon=":rocket:",
    layout="wide"
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
    
    rpc_endpoint = f"{supabase_url}/rest/v1/rpc/execute_sql"
    payload = {"query": query}
    response = requests.post(rpc_endpoint, headers=headers, json=payload)
    data=response.json()

    # Extract the 'result' from each item in the list
    cleaned_data = [item['result'] for item in data]
    df = pd.DataFrame(cleaned_data)
    return df


query = """
SELECT 
    source_chain as chain,
    source_id as asset,
    source_volume as volume,
    block_timestamp,
    transaction_hash,
    sender_address as wallet
FROM public.main_volume_table
UNION ALL
SELECT 
    dest_chain as chain,
    dest_id as asset,
    dest_volume as volume,
    block_timestamp,
    transaction_hash,
    sender_address as wallet
FROM public.main_volume_table
Order by block_timestamp desc, transaction_hash
"""

df = execute_sql(query)

## METRICS
query_metrics="""
with pre as (
SELECT 
    source_chain as chain,
    source_id as asset,
    source_volume as volume,
    block_timestamp,
    transaction_hash,
    sender_address as wallet
FROM public.main_volume_table
UNION ALL
SELECT 
    dest_chain as chain,
    dest_id as asset,
    dest_volume as volume,
    block_timestamp,
    transaction_hash,
    sender_address as wallet
FROM public.main_volume_table
Order by block_timestamp desc, transaction_hash
)
SELECT 
    sum(case when block_timestamp > NOW()- INTERVAL '24 hours' THEN volume ELSE 0 END) as volume_day,
    sum(case when block_timestamp > NOW()- INTERVAL '7 days' THEN volume ELSE 0 END) as volume_week,
    sum(case when block_timestamp >= DATE_TRUNC('month', NOW()) THEN volume ELSE 0 END) as volume_mtd,
    COUNT(DISTINCT CASE WHEN block_timestamp > NOW() - INTERVAL '24 hours' THEN wallet END) AS users_day,
    COUNT(DISTINCT CASE WHEN block_timestamp > NOW() - INTERVAL '7 days' THEN wallet END) AS users_week,
    COUNT(DISTINCT CASE WHEN block_timestamp >= DATE_TRUNC('month', NOW()) THEN wallet END) AS users_mtd,
    COUNT(CASE WHEN block_timestamp > NOW() - INTERVAL '24 hours' THEN transaction_hash END) AS trades_day,
    COUNT(CASE WHEN block_timestamp > NOW() - INTERVAL '7 days' THEN transaction_hash END) AS trades_week,
    COUNT(CASE WHEN block_timestamp >= DATE_TRUNC('month', NOW()) THEN transaction_hash END) AS trades_mtd
FROM pre
"""
metrics = execute_sql(query_metrics)

st.markdown("""
    <style>
    [data-testid="stMetricValue"] {
        padding: 20px;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.15);
        transition: background-color 0.3s ease, color 0.3s ease;
    }
    /* Dark mode styling */
    @media (prefers-color-scheme: dark) {
        [data-testid="stMetricValue"] {
            background-color: #2e2e2e;
            color: #ffffff;
        }
    }
    /* Light mode styling */
    @media (prefers-color-scheme: light) {
        [data-testid="stMetricValue"] {
            background-color: #f8f9fa;
            color: #333333;
        }
    }
    </style>
""", unsafe_allow_html=True)
current_month = datetime.now().strftime("%B")

# First row - Volume metrics
c1, c2, c3 = st.columns(3)
c1.metric("24h Volume", f"{metrics['volume_day'].iloc[0]:,.0f}")
c2.metric("7d Volume", f"{metrics['volume_week'].iloc[0]:,.0f}")
c3.metric(f"{current_month} Volume", f"{metrics['volume_mtd'].iloc[0]:,.0f}")

# Second row - User metrics
c4, c5, c6 = st.columns(3)
c4.metric("24h Users", f"{metrics['users_day'].iloc[0]:,.0f}")
c5.metric("7d Users", f"{metrics['users_week'].iloc[0]:,.0f}")
c6.metric(f"{current_month} Users", f"{metrics['users_mtd'].iloc[0]:,.0f}")

# Third row - Trade count metrics
c7, c8, c9 = st.columns(3)
c7.metric("24h Trades", f"{metrics['trades_day'].iloc[0]:,.0f}")
c8.metric("7d Trades", f"{metrics['trades_week'].iloc[0]:,.0f}")
c9.metric(f"{current_month} Trades", f"{metrics['trades_mtd'].iloc[0]:,.0f}")
## PLOT 1
st.markdown("<hr>", unsafe_allow_html=True)

@st.cache_data
def prepare_data(df, lookback_hours):
    # Convert timestamp to datetime if it isn't already
    df['block_timestamp'] = pd.to_datetime(df['block_timestamp'])
    
    # Calculate the cutoff time
    cutoff_time = datetime.now() - timedelta(hours=lookback_hours)
    
    # Filter data based on lookback period
    filtered_df = df[df['block_timestamp'] >= cutoff_time].copy()
    
    filtered_df = filtered_df[filtered_df['chain'].notna()]  # Remove null
    filtered_df = filtered_df[filtered_df['chain'] != '']    # Remove empty strings

    # Sort by timestamp (oldest first) for cumulative calculation
    filtered_df = filtered_df.sort_values('block_timestamp')
    
    # Calculate cumulative volume
    filtered_df['cumulative_volume'] = filtered_df['volume'].cumsum()
    # Create log-scaled sizes for dots
    # Simple log scaling for dot sizes
    filtered_df['marker_size'] = 2**np.log(filtered_df['volume']/50+1)
    # Clip to reasonable min/max sizes
    filtered_df['marker_size'] = filtered_df['marker_size'].clip(4, 50)
        
    return filtered_df

# Define chain colors
chain_colors = {
    'ethereum': '#627EEA',    # Ethereum blue
    'polygon': '#8247E5',     # Polygon purple
    'arbitrum': '#28A0F0',    # Arbitrum blue
    'optimism': '#FF0420',    # Optimism red
    'base': '#0052FF',        # Base blue
    'avalanche': '#E84142',   # Avalanche red
    'bsc': '#F3BA2F',         # BNB yellow
    'celo': '#35D07F',        # Celo green
    'solana': '#14F195'       # Solana green
}

# Time period selector 
time_periods = {
    "12 Hours": 12,
    "24 Hours": 24,
    "3 Days": 72,
    "5 Days": 120,
}

selected_period = st.selectbox(
    "Select Time Period",
    options=list(time_periods.keys()),
    index=1
)

# Get the data with prepared marker sizes
plot_df = prepare_data(df, time_periods[selected_period])

# Create the figure directly with go.Figure
fig = go.Figure()

# Add scatter points for each chain
for chain in plot_df['chain'].unique():
    chain_data = plot_df[plot_df['chain'] == chain]
    fig.add_trace(
        go.Scatter(
            x=chain_data['block_timestamp'],
            y=chain_data['cumulative_volume'],
            mode='markers',
            name=chain,
            marker=dict(
                size=chain_data['marker_size'],  # Use our calculated sizes
                opacity=0.8,
                color=chain_colors.get(chain.lower(), '#808080')
            ),
            hovertemplate=(
                
                "Volume: $%{customdata[0]:,.2f}<br>" +
                "Sender: %{customdata[1]}<br>" +
                "Chain: %{text}<br>" +
                "Time: %{x}<br>" +
                "Transaction: %{customdata[2]}<br>" +
                "Cumulative: $%{y:,.2f}"
            ),
            text=chain_data['chain'],
            customdata=np.column_stack((chain_data['volume'],chain_data['wallet'],chain_data['transaction_hash']))
        )
    )
    fig.update_layout(
    title={
        'text': f'Mach Trades  [ {time_periods[selected_period]} hrs ]',
        'y':0.95,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    }
    )

# # Add the cumulative line
# fig.add_trace(
#     go.Scatter(
#         x=plot_df['block_timestamp'],
#         y=plot_df['cumulative_volume'],
#         mode='lines',
#         line=dict(color='white', width=1),
#         name='Cumulative Volume',
#         showlegend=False
#     )
# )

# Update layout
fig.update_layout(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    xaxis_title="Time",
    yaxis_title="Cumulative Volume",
    showlegend=True,
    legend_title="Chain",
    hovermode='closest',
    height=600,
    template='plotly_dark',
    legend=dict(
        itemsizing='constant'
    )
)

# Add range slider
fig.update_xaxes(rangeslider_visible=True)

# Display the plot
st.plotly_chart(fig, use_container_width=True)

## PLOT 2 Groupings
st.markdown("<hr>", unsafe_allow_html=True)

grouped_query = """
with pre as (
SELECT 
    source_chain as chain,
    source_id as asset,
    source_volume as volume,
    block_timestamp,
    transaction_hash,
    sender_address as wallet
FROM public.main_volume_table
UNION ALL
SELECT 
    dest_chain as chain,
    dest_id as asset,
    dest_volume as volume,
    block_timestamp,
    transaction_hash,
    sender_address as wallet
FROM public.main_volume_table
)

SELECT 
chain, 
date_trunc('day', block_timestamp) as day,
sum(volume) as total_volume
FROM pre
WHERE block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days' AND volume>0
GROUP BY chain, date_trunc('day', block_timestamp) 
ORDER BY 
    date_trunc('day', block_timestamp) ASC, SUM(volume) DESC
"""
try:
    bar_values = execute_sql(grouped_query)
except Exception as e:
    st.error(f"Error executing query: {str(e)}")



def create_stacked_bar_chart(df):
    # Ensure the 'day' column is in datetime format
    df['day'] = pd.to_datetime(df['day'])
    
    # Helper function to add ordinal suffixes to the day number
    def ordinal(n):
        n = int(n)
        if 11 <= (n % 100) <= 13:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suffix}"
    
    # Create a new column for nicely formatted day labels (e.g., "Feb 1st")
    df['day_label'] = df['day'].apply(lambda d: d.strftime("%b ") + ordinal(d.day))
    
    # Compute total volume per chain (across all days) and sort descending
    chain_order = df.groupby('chain')['total_volume'].sum().sort_values(ascending=False)
    sorted_chains = chain_order.index.tolist()
    
    # Create the figure
    fig = go.Figure()
    
    # Add a trace for each chain in descending order so that the largest is on the bottom
    for chain in sorted_chains:
        chain_data = df[df['chain'] == chain].sort_values(by='day')
        fig.add_trace(go.Bar(
            name=chain.title(),  # Capitalize chain name
            x=chain_data['day_label'],  # Use custom formatted date labels
            y=chain_data['total_volume'],
            marker_color=chain_colors.get(chain.lower(), '#000000'),  # Use defined color or default to black
            marker_line=dict(color='black', width=1)  # Outline each bar with a black border
        ))
    
    # Update layout: set stacking mode, adjust legend (left-to-right ordering), etc.
    fig.update_layout(
        title={
            'text': 'Mach Volume by chain',
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        xaxis_title='Date',
        yaxis_title='Volume',
        barmode='stack',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",  # Legend items start on the left
            x=0
        ),
        xaxis=dict(
            type='category',
            tickangle=45
        ),
        height=600  # Make plot taller
    )
    
    return fig



# Create and show the chart
fig = create_stacked_bar_chart(bar_values)
st.plotly_chart(fig, use_container_width=True)

## PLOT 2.5
## PLOT 2 Groupings
st.markdown("<hr>", unsafe_allow_html=True)

grouped2_query = """
with pre as (
SELECT 
    source_chain as chain,
    source_id as asset,
    source_volume as volume,
    block_timestamp,
    transaction_hash,
    sender_address as wallet
FROM public.main_volume_table
UNION ALL
SELECT 
    dest_chain as chain,
    dest_id as asset,
    dest_volume as volume,
    block_timestamp,
    transaction_hash,
    sender_address as wallet
FROM public.main_volume_table
)

SELECT 
asset, 
date_trunc('day', block_timestamp) as day,
sum(volume) as total_volume
FROM pre
WHERE block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days' AND volume>0
GROUP BY asset, date_trunc('day', block_timestamp) 
ORDER BY 
    date_trunc('day', block_timestamp) ASC, SUM(volume) DESC
"""
try:
    asset_values = execute_sql(grouped2_query)
except Exception as e:
    st.error(f"Error executing query: {str(e)}")


def create_stacked_bar_chart(df):
    # Ensure the 'day' column is in datetime format
    df['day'] = pd.to_datetime(df['day'])
    
    # Helper function to add ordinal suffixes to day numbers (e.g., 1st, 2nd)
    def ordinal(n):
        n = int(n)
        if 11 <= (n % 100) <= 13:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suffix}"
    
    # Create formatted day labels (e.g., "Feb 1st")
    df['day_label'] = df['day'].apply(lambda d: d.strftime("%b ") + ordinal(d.day))
    
    # Calculate total volume per asset across all days
    asset_totals = df.groupby('asset')['total_volume'].sum().sort_values(ascending=False)
    # Determine the top 5 asset IDs
    top_assets = asset_totals.head(5).index.tolist()
    
    # Create a new column 'asset_group': if the asset is in the top 5, keep it; otherwise, label it as 'Other'
    df['asset_group'] = df['asset'].apply(lambda a: a if a in top_assets else 'Other')
    
    # Group by the new asset_group and day, summing total_volume so that non–top-5 assets aggregate as 'Other'
    df_grouped = df.groupby(['asset_group', 'day']).agg({'total_volume': 'sum'}).reset_index()
    
    # Recreate the day_label for the grouped data
    df_grouped['day_label'] = df_grouped['day'].apply(lambda d: d.strftime("%b ") + ordinal(d.day))
    
    # Determine the ordering of asset groups based on total volume.
    # Top assets in descending order, then add 'Other' at the end (if present).
    group_totals = df_grouped.groupby('asset_group')['total_volume'].sum()
    sorted_groups = group_totals.drop('Other', errors='ignore').sort_values(ascending=False).index.tolist()
    if 'Other' in group_totals:
        sorted_groups.append('Other')
    
    # Create the Plotly figure
    fig = go.Figure()
    
    for asset in sorted_groups:
        asset_data = df_grouped[df_grouped['asset_group'] == asset].sort_values(by='day')
        # Do not specify marker_color to use the default Plotly colors.
        fig.add_trace(go.Bar(
            name=str(asset),
            x=asset_data['day_label'],
            y=asset_data['total_volume'],
            marker_line=dict(color='black', width=1)
        ))
    
    # Update layout settings
    fig.update_layout(
        title={
            'text': 'Mach Volume by Asset',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        xaxis_title='Date',
        yaxis_title='Volume',
        barmode='stack',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0
        ),
        xaxis=dict(
            type='category',
            tickangle=45
        ),
        height=600
    )
    
    return fig

# Assuming bar_values is the DataFrame returned from your SQL query,
# which contains columns: asset, day, total_volume, etc.
# Create and display the chart in Streamlit.
fig = create_stacked_bar_chart(asset_values)
st.plotly_chart(fig, use_container_width=True)

##END






## PLOT 3
st.markdown("<hr>", unsafe_allow_html=True)

num_query = """
WITH pre AS (
    SELECT 
        source_chain AS chain,
        source_id AS asset,
        source_volume AS volume,
        block_timestamp,
        transaction_hash,
        sender_address AS wallet
    FROM public.main_volume_table
    UNION ALL
    SELECT 
        dest_chain AS chain,
        dest_id AS asset,
        dest_volume AS volume,
        block_timestamp,
        transaction_hash,
        sender_address AS wallet
    FROM public.main_volume_table
)

SELECT 
    date_trunc('hour', block_timestamp) AS hour,
    COUNT(DISTINCT transaction_hash) AS trades_count
FROM pre
WHERE block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
  AND volume > 0
GROUP BY date_trunc('hour', block_timestamp)
ORDER BY hour ASC
"""

num_values = execute_sql(num_query)

def create_cumulative_line_chart(df):
    """
    Create a cumulative line chart of the number of trades over time.
    
    Parameters:
    - df: pandas.DataFrame
          A DataFrame with at least two columns:
              'hour'         : timestamps (or strings convertible to datetime)
              'trades_count' : the number of trades during that hour.
    
    Returns:
    - fig: a Plotly Express figure object representing the cumulative trades over time.
    """
    # Convert the 'hour' column to datetime if not already in datetime format
    df['hour'] = pd.to_datetime(df['hour'])
    df = df.sort_values('hour')
    
    # Calculate the cumulative sum of trades
    df['cumulative_trades'] = df['trades_count'].cumsum()
    
    # Create a line chart using Plotly Express
    fig = px.line(
        df,
        x='hour',
        y='cumulative_trades',
        title='Cumulative Number of Trades Over the Last 7 Days',
        labels={
            'hour': 'Time',
            'cumulative_trades': 'Cumulative Trades'
        }
    )
    
    # Update layout to adjust tick formatting and chart dimensions
    fig.update_layout(
        xaxis=dict(
            tickformat="%b %d, %H:%M",  # e.g., "Feb 01, 14:00"
            title='Time'
        ),
        yaxis_title='Cumulative Trades',
        height=600
    )
    
    return fig


# --- Single Merged SQL Query ---
merged_query = """
WITH pre AS (
    SELECT 
        source_chain AS chain,
        source_id AS asset,
        source_volume AS volume,
        block_timestamp,
        transaction_hash,
        sender_address AS wallet
    FROM public.main_volume_table
    UNION ALL
    SELECT 
        dest_chain AS chain,
        dest_id AS asset,
        dest_volume AS volume,
        block_timestamp,
        transaction_hash,
        sender_address AS wallet
    FROM public.main_volume_table
)
SELECT 
    date_trunc('hour', block_timestamp) AS hour,
    COUNT(DISTINCT transaction_hash) AS trades_count,
    SUM(volume) AS volume_total,
    json_agg(DISTINCT wallet) AS wallets

FROM pre
WHERE block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
  AND volume > 0
GROUP BY date_trunc('hour', block_timestamp)
ORDER BY hour ASC
"""

# Execute the merged query to get the data
data = execute_sql(merged_query)

# --- Generic Cumulative Line Chart Function ---
def create_cumulative_line_chart(df, metric_column, title, y_label):
    """
    Create a cumulative line chart for a given metric over time.
    
    Parameters:
      - df: pandas.DataFrame with at least:
            'hour' : timestamps (or strings convertible to datetime)
            and a numeric column for the metric.
      - metric_column: str, the column name to be cumulatively summed.
      - title: str, the title of the chart.
      - y_label: str, the y-axis label.
    
    Returns:
      - fig: a Plotly Express figure object.
    """
    # Convert 'hour' to datetime if necessary and sort
    df['hour'] = pd.to_datetime(df['hour'])
    df = df.sort_values('hour')
    
    # Calculate cumulative sum for the specified metric
    df['cumulative'] = df[metric_column].cumsum()
    
    # Create the line chart using Plotly Express
    fig = px.line(
        df,
        x='hour',
        y='cumulative',
        title=title,
        labels={'hour': 'Time', 'cumulative': y_label}
    )
    
    # Update layout for improved readability
    fig.update_layout(
        xaxis=dict(
            tickformat="%b %d, %H:%M",  # Example format: "Feb 01, 14:00"
            title='Time'
        ),
        yaxis_title=y_label,
        height=600
    )
    
    return fig

def create_cumulative_users_line_chart(df):
    """
    Create a cumulative line chart of unique users over time.
    
    For each hour, this function updates a cumulative set of unique wallets
    (parsed from a JSON array) so that each user is counted only once.
    
    Expects df to have:
      - 'hour': timestamps
      - 'wallets': a JSON array (or Python list) of wallet strings.
    """
    # Ensure 'hour' is datetime and sort the DataFrame by time
    df['hour'] = pd.to_datetime(df['hour'])
    df = df.sort_values('hour')
    
    cumulative_users = []
    unique_users = set()
    
    # Iterate over each row to update the cumulative set of unique users
    for wallets in df['wallets']:
        # Some drivers return the JSON column as a string; if so, parse it.
        if isinstance(wallets, str):
            try:
                wallets = json.loads(wallets)
            except Exception:
                wallets = []  # fallback in case of parsing issues
        # Update the cumulative set with wallets from this hour
        unique_users.update(wallets)
        cumulative_users.append(len(unique_users))
    
    # Add the cumulative unique user counts to the DataFrame
    df['cumulative_users'] = cumulative_users
    
    # Create a line chart for cumulative unique users
    fig = px.line(
        df,
        x='hour',
        y='cumulative_users',
        title='New Unique Users Over the Last 7 Days',
        labels={'hour': 'Time', 'cumulative_users': 'Cumulative Unique Users'}
    )
    
    fig.update_layout(
        xaxis=dict(
            tickformat="%b %d, %H:%M",
            title='Time'
        ),
        yaxis_title='Cumulative Unique Users',
        height=600
    )
    
    return fig

# --- Create Figures for Each Metric ---
trades_fig = create_cumulative_line_chart(
    data.copy(), 
    metric_column='trades_count', 
    title='Cumulative Number of Trades Over the Last 7 Days',
    y_label='Cumulative Trades'
)

volume_fig = create_cumulative_line_chart(
    data.copy(), 
    metric_column='volume_total', 
    title='Cumulative Volume Over the Last 7 Days',
    y_label='Cumulative Volume'
)

users_fig = create_cumulative_users_line_chart(data.copy())


# --- Display the Charts in Streamlit ---
# For example, show two charts side-by-side and the third one below
col1, col2, col3 = st.columns(3)
with col1:
    st.plotly_chart(volume_fig)
with col2:
    st.plotly_chart(trades_fig)
with col3:
    st.plotly_chart(users_fig)

