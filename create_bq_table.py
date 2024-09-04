from google.cloud import bigquery
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_file('coinbase-proj-941cedd1d50d.json')

# Construct a BigQuery client object.
client = bigquery.Client(credentials=credentials)
# Define the dataset and table names
dataset_id = 'coinbase_stream_data'  # Replace with your dataset ID
table_id = 'BTC_to_currencies'  # Replace with your table name

schema = [
    bigquery.SchemaField('avg_sale_price', 'FLOAT64', mode='NULLABLE'),
    bigquery.SchemaField('avg_buy_price', 'FLOAT64', mode='NULLABLE'),
    bigquery.SchemaField('sale_count', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('buy_count', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('total_sale_size', 'FLOAT64', mode='NULLABLE'),
    bigquery.SchemaField('total_buy_size', 'FLOAT64', mode='NULLABLE'),
    bigquery.SchemaField('start_time', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('end_time', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('product_id', 'STRING', mode='NULLABLE')
]
# Define the table reference
table_ref = client.dataset(dataset_id).table(table_id)

# Create a Table object
table = bigquery.Table(table_ref, schema=schema)
# Specify the clustering fields
table.clustering_fields = ['product_id']

# Create the table
table = client.create_table(table, exists_ok=True)  # API request

print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


