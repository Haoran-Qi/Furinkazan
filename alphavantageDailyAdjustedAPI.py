import os
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import pandas as pd
import pdb

def load_json_files_to_bigquery(dataset_id, table_id, directory_path, project_id):
    
    credentials = service_account.Credentials.from_service_account_file("automation-project-furinkazan-zero.json")
    client = bigquery.Client(project=project_id, credentials=credentials)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)

    # Configure the table schema
    schema = [
        bigquery.SchemaField("startDate", "STRING"),
        bigquery.SchemaField("endDate", "STRING"),
        bigquery.SchemaField("year", "STRING"),
        bigquery.SchemaField("quarter", "STRING"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("label", "STRING"),
        bigquery.SchemaField("concept", "STRING"),
        bigquery.SchemaField("unit", "STRING"),
        bigquery.SchemaField("value", "FLOAt"),
    ]
    table.schema = schema
    table = client.create_table(table_id)  # Create the table if it doesn't exist

    # Traverse the directory and load JSON files
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    data = json.load(f)
                    job_config = bigquery.LoadJobConfig(
                        schema=schema,
                    )

                    # load bs
                    bs = data["data"]['bs']
                    rows = []
                    for item in bs:
                        rows.append({
                            "startDate": data['startDate'],
                            "endDate":data['endDate'],
                            "year": data['year'],
                            "quarter": data['quarter'],
                            "symbol": data['symbol'],
                            "label": item['label'],
                            "concept": item['concept'],
                            "unit": item['unit'],
                            "value": item['value'],
                        })
                    
                    # load cf
                    cf = data["data"]['cf']
                    for item in cf:
                        rows.append( {
                            "startDate": data['startDate'],
                            "endDate":data['endDate'],
                            "year": data['year'],
                            "quarter": data['quarter'],
                            "symbol": data['symbol'],
                            "label": item['label'],
                            "concept": item['concept'],
                            "unit": item['unit'],
                            "value": item['value'],
                        })
                    
                    # load cf
                    cf = data["data"]['ic']
                    for item in cf:
                        rows.append({
                            "startDate": data['startDate'],
                            "endDate":data['endDate'],
                            "year": data['year'],
                            "quarter": data['quarter'],
                            "symbol": data['symbol'],
                            "label": item['label'],
                            "concept": item['concept'],
                            "unit": item['unit'],
                            "value": item['value'],
                        })
                    df = pd.DataFrame(rows, columns=["startDate",
                            "endDate",
                            "year",
                            "quarter",
                            "symbol",
                            "label",
                            "concept",
                            "unit",
                            "value"],)
                    # remove N/A
                    df['value'] = pd.to_numeric(df['value'], errors='coerce')
                    df.dropna(subset=['value'], inplace=True)
                    job = client.load_table_from_dataframe(df, table_id, job_config=job_config) 
                    job.result()
                    print(f"Loaded file: {file_path} to BigQuery table: {table_id}")

# Example usage
project_id = "project-furinkazan-zero"
dataset_id = "fundamental"
table_id = "project-furinkazan-zero.fundamental.data"
directory_path = "C:/Users/Devin/Desktop/Furinkazan/Financials_As_Reported"

load_json_files_to_bigquery(dataset_id, table_id, directory_path, project_id)