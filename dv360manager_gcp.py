import jwt
import time
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from datetime import datetime
from urllib.parse import quote
from google.cloud import bigquery


# DV360 Credentials
PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----\nMIIEuw..........teLSAZnDLdRN6o5\n-----END PRIVATE KEY-----\n"
CLIENT_EMAIL = "dv360-manager@dv360-editor.iam.gserviceaccount.com";
JTW_TOKEN = ""
ISO_DATE = (datetime(2023, 1, 1).isoformat() + "Z")
BASE_URL = "https://displayvideo.googleapis.com/v2/"
CAMPAIGNS_IDS = []

advertiser_ids = [
    4445836, 4445837, 4445838, ....., 862744995
]

#BigQuery Credentials

proyect_id = "dv360"
dataset_id = 'dv360'
table_id = 'dv360_manager'


def get_jwt_token(CLIENT_EMAIL, PRIVATE_KEY):
    jwt_header = {
        "alg": "RS256",
        "typ": "JWT"
    }
    
    jwt_claim = {
        "iss": CLIENT_EMAIL,
        "scope": "https://www.googleapis.com/auth/display-video",
        "aud": "https://oauth2.googleapis.com/token",
        "exp": int(time.time()) + 3600,
        "iat": int(time.time())
    }
    jwt_token = jwt.encode(jwt_claim, PRIVATE_KEY, algorithm='RS256')
    return jwt_token

def get_access_token():
    global JTW_TOKEN
    if JTW_TOKEN != "":
        try:
            decoded_token = jwt.decode(JTW_TOKEN, algorithms=['RS256'], verify=False)
            if "exp" in decoded_token and int(time.time()) < decoded_token["exp"]:
                return JTW_TOKEN
        except jwt.InvalidTokenError:
            pass
    
    new_jwt = get_jwt_token(CLIENT_EMAIL, PRIVATE_KEY)
    
    url = "https://oauth2.googleapis.com/token"
    payload = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": new_jwt
    }
    
    headers = {
        "Authorization": f"Bearer {new_jwt}"  # Agrega el encabezado de autenticación con el token JWT
    }
    response = requests.post(url, data=payload, headers=headers)
    response_data = response.json()
    
    if "access_token" in response_data:
        JTW_TOKEN = response_data["access_token"]
        return JTW_TOKEN
    else:
        print("Error obteniendo el token de acceso.")
        return None

def getHeaders():
    access_token = get_access_token()
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + access_token,
    }
    return headers
    
def getCampaigns():
    headers = getHeaders()
    campaign_data = []

    for advertiser_id in advertiser_ids:
        url = f'{BASE_URL}advertisers/{advertiser_id}/campaigns?filter=updateTime>="{ISO_DATE}"'
        result = requests.get(url=url, headers=headers)
        response_json = result.json()

        if "campaigns" in response_json:
            campaigns = response_json["campaigns"]
            for campaign in campaigns:
                campaign_data.append(campaign)

    return campaign_data
    
def getInsertionOrders():
    headers = getHeaders()
    insertion_order_data = []

    for advertiser_id in advertiser_ids:
        for campaign_id in CAMPAIGNS_IDS:
            url = f'{BASE_URL}advertisers/{advertiser_id}/insertionOrders?filter=campaignId="{campaign_id}"'
            result = requests.get(url=url, headers=headers)
            response_json = result.json()

            insertion_orders = response_json.get("insertionOrders")
            if insertion_orders:
                insertion_order_data.extend(insertion_orders)

    return insertion_order_data
      
def getLineItems():
    headers = getHeaders()
    line_item_data = []

    for advertiser_id in advertiser_ids:
        for campaign_id in CAMPAIGNS_IDS:
            url = f'{BASE_URL}advertisers/{advertiser_id}/lineItems?filter=campaignId="{campaign_id}"'
            result = requests.get(url=url, headers=headers)
            response_json = result.json()

            line_items = response_json.get("lineItems")
            if line_items:
                line_item_data.extend(line_items)

    return line_item_data

def get_data():
    data = []

    # Use the functions to gather data from DV360 Manager
    data += getCampaigns()
    data += getInsertionOrders()
    data += getLineItems()

    return data

def process_and_send_to_bigquery(advertiser_ids):
    # Create a client for BigQuery
    client = bigquery.Client(project = proyect_id)

    # Get the data from DV360 Manager
    data = get_data() 

    # Send the data to BigQuery
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = 'WRITE_TRUNCATE'

    job = client.load_table_from_json(data, f'{dataset_id}.{table_id}', job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Data uploaded to BigQuery: {dataset_id}.{table_id}")

# Call the function to process and send data to BigQuery
process_and_send_to_bigquery(advertiser_ids)
