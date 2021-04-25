from main import client
import pandas as pd

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

# Show which side has the biggest average gain


res = query_job_wins.result()
print('The biggest wins in the data set:\n', res.to_dataframe())