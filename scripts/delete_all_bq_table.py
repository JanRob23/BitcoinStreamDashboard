from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('coinbase-proj-941cedd1d50d.json')

# Construct a BigQuery client object.
client = bigquery.Client(credentials=credentials)
# SQL statement to delete all rows or truncate the table
# For DELETE: sql = "DELETE FROM `project_id.dataset_id.table_id` WHERE TRUE;"
sql = "TRUNCATE TABLE coinbase-proj.coinbase_stream_data.BTC_to_currencies"

# Run the query
query_job = client.query(sql)

# Wait for the job to complete
query_job.result()

print("Table entries deleted successfully.")