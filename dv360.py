from google.cloud import bigquery

def create_bigquery_table(credentials_path, project_id, dataset_id, table_id, schema):
    try:
        # Create a BigQuery client instance using your credentials.
        client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)
        
        # Create a table instance with the specified schema.
        table_ref = client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)

        # Attempt to create the table in BigQuery.
        table = client.create_table(table)
        return f'Table {table.table_id} created successfully.'
    except Exception as e:
        return f'Error creating the table: {e}'

# Replace these values with your own configuration.
credentials_path = 'the path to the credentials on your computer'
project_id = 'your_id_proyect'
dataset_id = 'your_dataset_id'
table_id = 'your_table_id'

# Define the schema of the table you want to create.
schema = [
    bigquery.SchemaField('Advertiser_ID', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Advertiser_Currency', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Campaign_ID', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Campaign', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Insertion_Order_ID', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Line_Item_ID', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Current_Insertion_Order_Goal_Type', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Impressions', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Clicks', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Total_Conversions', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Engagements', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Engagement_Rate', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Revenue_Advertiser_Currency', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Total_Media_Cost_Advertiser_Currency', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Cost_eCPM_Advertiser_Currency', 'STRING', mode='REQUIRED')
]

# Call the function to create the table.
result = create_bigquery_table(credentials_path, project_id, dataset_id, table_id, schema)
print(result)
