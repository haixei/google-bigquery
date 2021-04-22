from google.cloud import bigquery
from dotenv import load_dotenv
import os

# Load env variables
load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('KEY_ROUTE')
project_id = os.getenv('PROJECT_ID')
model_name = os.getenv('MODEL_NAME')

# Connect
client = bigquery.Client(project=project_id)
dataset = client.create_dataset('testdb', exists_ok=True)

# Load the data
table_name = 'bigquery-public-data.google_analytics_sample.ga_sessions_*'
table = client.get_table(table_name)
rows = client.list_rows(table, max_results=5).to_dataframe()
print(rows)
# Quick note: Some of the columns have json insformation included in the schema which means that inside of the
# cell there still is a nested data structure
# >> print(table.schema)

# Show the first cell in the "totals" column
# >> print('First cell in totals:\n', rows.totals[0])

# Create a model query
query_job = client.query(f'''
CREATE MODEL IF NOT EXISTS `{model_name}`
OPTIONS(model_type='logistic_reg') AS
SELECT
    IF(totals.transactions IS NULL, 0, 1) AS label,
    IFNULL(device.operatingSystem, "") AS os,
    device.isMobile AS is_mobile,
    IFNULL(geoNetwork.country, "") AS country,
    IFNULL(totals.pageviews, 0) AS pageviews
FROM
    `{table_name}`
WHERE
    _TABLE_SUFFIX BETWEEN '20160801' AND '20170630'
''')

# Get training statistics
query_job_tr_stats = client.query(f'''
SELECT *
FROM
    ML.TRAINING_INFO(MODEL `{model_name}`)
ORDER BY iteration
''')

tr_stats = query_job_tr_stats.result()
print('Training statistics: ', tr_stats.to_dataframe())

