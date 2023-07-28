import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import requests

# TODO(developer): Set key_path to the path to the service account key file.
key_path = "/Users/carlosrivero/Documents/chaser/dv360/dv360-editor-06538ab1229c.json"

# Create credentials using the service account key file
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Authenticate with the DV360 API
service = build("doubleclickbidmanager", "v1", credentials=credentials)

# Initialize BigQuery client with project from credentials
bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Dataset ID
dataset_id = "dv360"
# Table ID
table_id = "dv360_report"

# Function to process report URLs and insert data into BigQuery
def process_and_insert_data(url, advertiser_id):
    # Assuming the URL points to a CSV file, read the data using pandas
    df = pd.read_csv(url)
    
    # Add the advertiser_id column to the DataFrame
    df["advertiser_id"] = advertiser_id
    
    # Insert the data into BigQuery
    table = bigquery_client.get_table(bigquery_client.dataset(dataset_id).table(table_id))
    bigquery_client.insert_rows_from_dataframe(table, df)

# Reemplaza estas credenciales con los valores proporcionados
PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCue8pSkbHN8smr\nUshK9ufIgjfR7CLmMRerunOJ4Jqye0iKewT1D5YU2oAp47edbTtT7SBD39jP9x0A\nR4OurtQOG+A7gysB77BNm0xd4wNzG9S8x46JfILHxho1bv6VSHUvCF4w7r5juJbA\nagcP0eh38z2BrXbxjDmSAp2o11vPfCvDxSWltb/LKOtrO0Dx8EF9EZiVfjdtMUOX\nosEW9FRgI2SoSW0xXsegQDvT7XTVQE8fYLB1jmr7Ei4H8eVNhEW4MIHpnOnH3Q5i\nRwnmYKethoVELNNWJFuUlc3XOxlNCj84Ygaz2I95Lh6mcIBxpZSat7MLNnmxddN+\nyrGbrVGrAgMBAAECggEAUzSkIGjq/fe0FReWfS0uDe+PF/PAFr69d36j/1GU490n\nlCYjEIv8uXgTGybFFnUTECwpsURre4zvjwULdj9Xz1yauzaLSVKYZAHVUoZhzEAu\n3FFblxPBt+uv7uA91kml3CZk6HfJYfuSJDLLqE8kySK9J3xllr2UbJ0DuxLr7M6T\ngAb+80Qbl4UjM/bx0M6OEJGtfdSv7urlMek2qsj++/9qy1mcZVOOXy5tRuuzysqF\nmuPawIr2WySvJXXvCQ+Bebr4jvBb5cYyF6RtN/muj0laziWn1caG2gNro+F87vjm\n9t3K2cZNRchyx6mb3lAd7rM/t2q3WGQj+J/TO6vVCQKBgQDhcKgBmiyVpOQA2zkT\njzcOAp0wEru1jG9n6Ge1lE7p6j6mIfSOgWzoUc2QT2lwHktT0nkSY3PScdlTAoNV\nKi4q/nSouVoVWVP/lTyOP87rMHYLgZ45EG5bms7QoHDX1sNpB7y2BGLf0OXDJzw8\nAhsodN3rlMcXLfTY37XLiyD3iQKBgQDGItBIFsQCIjurpegnujqBsT+tDDWHLUlK\n/GkFuBTAGuzkaV8uRmAKR8cZOLJgJo6vVJvqfiI5Clu6UwP7PF5m5RhYxxOg5y0/\nCGyHy43UJ1Fsjjb0lA7lOwoj+AoKBOwQl5SHgm9K3LawsbRoizYQ47uXM7Rxo1wu\nNVVCXmw+kwKBgDSszwS40mwQwBmXH4n7b6C9aZ7+8Y8lBi7gNcNOqPWs/wBwdKUL\nhrftzTtH4toIqg8m4ZUzWa+1XFMyBh/TIEB5fiaiUHMmkSLp6uFN9Xoss5hSq/an\naTHMRUCoDFXSX4xd+3EIK1YiiF0GAOQAlfno/KV7+Nzopwo5k0/A7W4JAoGBAJ2I\nkoeuqht0MnNQTtw/YkM5tYIWIf0fUZXwSGRGryDqh404BnhZdgTWewOfo5t1LJ9U\nqz7vGLC3fqUPWiwIHJq87fWwGvgktkHWpcv2WQbkWAXysNkXWxyBK5fbn8fFkBfF\nHU/8shYYlJy6PdGdVdhp6P3YG1D67NDEkMxbHL9HAoGBAI91Q4kmyC2B1w6xp/39\n4jc6Rwf/oG+t8znQWjfrhnW+cmiF+oF7bZNoMfokkGZM1wFmSYRcSCNY27dQYnBq\nmWaO+A98cWKaf019MnmeevrFfTSF8JryOn5Sq4/SzgIVbh4mR+DFt9JLN56w+k6b\nGFL3dYuXyyd32FZ0xyEoFt+T\n-----END PRIVATE KEY-----\n"

CLIENT_EMAIL = "dv360-manager@dv360-editor.iam.gserviceaccount.com"
ADVERTISER_IDS = [4445836, 4445837, 4445838, 4445860,
                  4445840, 4445845, 4445841, 4445842, 4445858,
                  4445843, 4445844, 4445861, 4445839, 4445846,
                  4445847, 4445848, 4445850, 4445851, 4445852,
                  4445854, 4445853, 4445855, 4445862, 4445859,
                  4445857, 4445856, 4994844, 862744995]

# Create credentials using the provided private key and client email
credentials = service_account.Credentials.from_service_account_info(
    {
        "type": "service_account",
        "project_id": "dv360-editor",
        "private_key_id": "06538ab1229c3d9986570bf26e55a18b700980db",
        "private_key": PRIVATE_KEY,
        "client_email": CLIENT_EMAIL,
        "client_id": "103031819472809698881",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dv360-manager%40dv360-editor.iam.gserviceaccount.com"
    }
)

# Authenticate with the DV360 API
service = build("doubleclickbidmanager", "v1", credentials=credentials)

# Initialize BigQuery client
bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Dataset ID
dataset_id = "dv360"
# Table ID
table_id = "dv360_report"

# Function to process report URLs and insert data into BigQuery
def process_and_insert_data(url, advertiser_id):
    # Download the CSV content from the URL
    response = requests.get(url)
    response.raise_for_status()

    # Read the CSV content into a pandas DataFrame
    df = pd.read_csv(pd.StringIO(response.text))

# Query the DV360 API for each advertiser ID and process the report URLs
for advertiser_id in ADVERTISER_IDS:
    query_body = {
        "kind": "doubleclickbidmanager#query",
        "metadata": {
            "title": "My Report",
            "dataRange": "LAST_7_DAYS",
            "format": "CSV",
            "locale": "en_US"
        },
        "params": {
            "type": "TYPE_GENERAL",
            "groupBys": ["FILTER_ADVERTISER", "FILTER_LINE_ITEM"],
            "metrics": ["METRIC_IMPRESSIONS", "METRIC_CLICKS", "METRIC_CTR"],
            "filters": [
                {
                    "type": "FILTER_ADVERTISER",
                    "value": str(advertiser_id)
                }
            ]
        },
        "schedule": {
            "frequency": "ONE_TIME"
        },
        "timezoneCode": "UTC",
        "version": "V1.1"
    }


    # Create the query
    response = bigquery_client.query(query_body)

    # Run the query and wait for completion
    response = bigquery_client.query().runquery(queryId=response["queryId"]).execute()
    while response["metadata"]["running"]:
        response = bigquery_client.query().getquery(queryId=response["queryId"]).execute()

    # Get the report file URL
    report_url = response["metadata"]["googleCloudStoragePathForLatestReport"]

    # Call the function with the URL as a string
    process_and_insert_data(str(report_url), advertiser_id)
