import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import datetime

def round_time_to_nearest_ten_seconds(dt):
    seconds = (dt - dt.min).seconds
    rounding = (seconds + 5) // 10 * 10
    return dt + datetime.timedelta(0, rounding - seconds, -dt.microsecond)

credentials = service_account.Credentials.from_service_account_file('coinbase-proj-941cedd1d50d.json')

# Construct a BigQuery client object.
client = bigquery.Client(credentials=credentials)


get_lowest_timestamp_sql = """
    SELECT MIN(start_time) AS earliest_timestamp
    FROM (
        SELECT start_time
        FROM coinbase-proj.coinbase_stream_data.BTC_to_currencies
        WHERE start_time IS NOT NULL
    ) AS subquery;
"""

get_latest_timestamp_sql = """
    SELECT MAX(end_time) AS latest_timestamp
    FROM (
        SELECT end_time
        FROM coinbase-proj.coinbase_stream_data.BTC_to_currencies
        WHERE start_time IS NOT NULL
    ) AS subquery;
"""

lowest_timestamp = client.query(get_lowest_timestamp_sql).result().to_dataframe().iloc[0]['earliest_timestamp']
highest_timestamp = client.query(get_latest_timestamp_sql).result().to_dataframe().iloc[0]['latest_timestamp']
print(lowest_timestamp, highest_timestamp)


st.title('Bitcoin Conversion UI')

cols = st.columns(2)

with cols[0]:
    feature = st.selectbox('Feature', ['avg_sale_price', 'avg_buy_price', 'sale_count', 'buy_count', 'total_sale_size', 'total_buy_size'])
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
    get_feature_per_currency_per_timeframe_sql = f"""
    SELECT *
    FROM coinbase-proj.coinbase_stream_data.BTC_to_currencies
    WHERE product_id = '{currency}'
    AND start_time BETWEEn '{start_timestamp}' AND '{end_timestamp}';
    """
    print(get_feature_per_currency_per_timeframe_sql)
    result = client.query(get_feature_per_currency_per_timeframe_sql).result().to_dataframe()

    print(result)
    st.line_chart(result)
