import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import datetime
import matplotlib.pyplot as plt


credentials = service_account.Credentials.from_service_account_file('coinbase-proj-941cedd1d50d.json')

# Construct a BigQuery client object.
client = bigquery.Client(credentials=credentials)

table_id = "coinbase-proj.coinbase_stream_data.temp_updates"

get_lowest_timestamp_sql = f"""
    SELECT MIN(start_time) AS earliest_timestamp
    FROM (
        SELECT start_time
        FROM {table_id}
        WHERE start_time IS NOT NULL
    ) AS subquery;
"""

get_latest_timestamp_sql = f"""
    SELECT MAX(end_time) AS latest_timestamp
    FROM (
        SELECT end_time
        FROM {table_id}
        WHERE start_time IS NOT NULL
    ) AS subquery;
"""

lowest_timestamp = client.query(get_lowest_timestamp_sql).result().to_dataframe().iloc[0]['earliest_timestamp']
highest_timestamp = client.query(get_latest_timestamp_sql).result().to_dataframe().iloc[0]['latest_timestamp']
print(lowest_timestamp, highest_timestamp)


st.title('Bitcoin streaming Data Dashboard')

feature = st.multiselect('Feature', ["Average Sale Price", "Average Purchase Price", "Number of Sales", "Number of Purchases", "Size of Sales", "Size of Purchases"], "Average Sale Price")


conversion_dict = {"Average Sale Price": "avg_sale_price", "Average Purchase Price": "avg_buy_price", "Number of Sales": "sale_count", 
                   "Number of Purchases": "buy_count", "Size of Sales": "total_sale_size", "Size of Purchases": "total_buy_size"}  

 
currency = st.selectbox('Currency', ['BTC-USD', 'BTC-EUR', 'BTC-GBP'])
# Function to round the time to the nearest 10-second interval


# Custom input for the full timestamp string
start_time_input = st.text_input(
    "Enter start timestamp (format: YYYY-MM-DD HH:MM:SS)",
    f"{lowest_timestamp}"
)
end_time_input = st.text_input(
    "Enter end timestamp (format: YYYY-MM-DD HH:MM:SS)",
    f"{highest_timestamp}"
)

# Convert strings to datetime objects
start_timestamp = pd.to_datetime(start_time_input)
end_timestamp = pd.to_datetime(end_time_input)
# {feature} AS {feature}
values_to_plot = []
# Create a figure and axis
fig, ax = plt.subplots()

for feature in feature:
    f = conversion_dict[feature]
    get_feature_per_currency_per_timeframe_sql = f"""
    SELECT {f}
    FROM {table_id}
    WHERE product_id = '{currency}'
    AND start_time BETWEEN '{start_timestamp}' AND '{end_timestamp}';
    """
    values_to_plot.append({feature: client.query(get_feature_per_currency_per_timeframe_sql).result().to_dataframe()})
    ax.plot(client.query(get_feature_per_currency_per_timeframe_sql).result().to_dataframe(), label=feature)
ax.legend()
st.pyplot(fig)
