import matplotlib.pyplot as plt
from main import client, model_name

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

# Quick note: The BQ does the automatc stopping for us as well as increasing the learning rate
# according to the situation
tr_stats = query_job_tr_stats.result()
print('Training statistics:', tr_stats.to_dataframe())

# Model evaluation
query_job_eval = client.query(f'''
SELECT *
FROM
    ML.EVALUATE(MODEL `{model_name}`, (
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
    ))
''')

model_eval = query_job_eval.result()
print('Evaluation scores:\n', model_eval.to_dataframe())

# Showcase the ROC curve
query_job_curve = client.query(f'''
SELECT *
FROM
    ML.ROC_CURVE(MODEL `{model_name}`)
''')

roc = query_job_curve.result().to_dataframe()
plt.plot(roc.false_positive_rate, roc.recall)
# >> plt.show()

# Use the model to make predictions
query_job_pred = client.query(f'''
SELECT
    country,
    SUM(predicted_label) as total_predicted_purchases
FROM 
    ML.PREDICT(MODEL `{model_name}`, (
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
    ))
GROUP BY country
ORDER BY total_predicted_purchases DESC
LIMIT 10
''')

print('Predictions:\n', query_job_pred.result().to_dataframe())

