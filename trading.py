from main import client
import pandas as pd
import os

# Get the model
model_name = os.getenv('TRADING_MODEL')

# Save the table name for the ease of use
table_name = 'bigquery-public-data.cymbal_investments.trade_capture_report'

# Show the biggest wins in the trading dataset
query_job_wins = client.query(f'''
SELECT Sides.Side,
       (StrikePrice - LastPx) * -1 AS Gain
FROM  {table_name}, UNNEST(Sides) AS Sides
WHERE (StrikePrice - LastPx) < 0
ORDER BY Gain DESC
LIMIT 10
''')

res = query_job_wins.result()
print('The biggest wins in the data set:\n', res.to_dataframe())

# Show which side has the biggest average gain
query_job_avg_gain = client.query(f'''
SELECT Sides.Side, AVG((StrikePrice - LastPx) * -1) AS AvgGain
FROM {table_name}, UNNEST(Sides) AS Sides
GROUP BY Sides.Side
''')

res = query_job_avg_gain.result()
print('The biggest average gain by category og the bet:\n', res.to_dataframe())

# Show the biggest lose in every category
query_job_min_gain = client.query(f'''
SELECT Sides.Side, MIN((StrikePrice - LastPx) * -1) AS BiggestLose
FROM {table_name}, UNNEST(Sides) AS Sides
GROUP BY Sides.Side
''')

res = query_job_min_gain.result()
print('The biggest lose in every category:\n', res.to_dataframe())

# When the most transactions took place
query_job_months = client.query(f'''
SELECT  EXTRACT(MONTH FROM TradeDate) as Month, count(*) as NumberOfTransactions
FROM {table_name}
GROUP BY Month
ORDER BY NumberOfTransactions DESC
''')

res = query_job_months.result()
print('When the most transactions took place?:\n', res.to_dataframe())