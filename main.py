from google.cloud import bigquery
from dotenv import load_dotenv
import os

# Load env variables
load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('KEY_ROUTE')
project_id = os.getenv('PROJECT_ID')

# Connect
client = bigquery.Client(project=project_id)
dataset = client.create_dataset('testdb', exists_ok=True)