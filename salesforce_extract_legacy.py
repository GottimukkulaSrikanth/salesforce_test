import json
import os
import csv
import time
from datetime import datetime

import boto3
import requests
from simple_salesforce import Salesforce
from botocore.exceptions import ClientError

# Output directory for the extracted CSV
OUTPUT_DIRECTORY = './extract/'
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

# AWS session and Secrets Manager client
aws_session = boto3.Session(profile_name="saml-uat")
secrets_client = aws_session.client(service_name='secretsmanager')


def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager."""
    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise e

    return json.loads(get_secret_value_response["SecretString"])


def get_salesforce_access_token(auth_url, consumer_key, consumer_secret):
    """Get Salesforce access token using client credentials flow."""
    auth_data = {
        "grant_type": "client_credentials",
        "client_id": consumer_key,
        "client_secret": consumer_secret,
    }
    auth_response = requests.post(auth_url, data=auth_data, timeout=10)
    response_data = auth_response.json()

    access_token = response_data["access_token"]
    instance_url = response_data["instance_url"]
    return [access_token, instance_url]


def authenticate_salesforce(secret_name):
    """Authenticate with Salesforce using credentials from Secrets Manager."""
    try:
        secrets = get_secret(secret_name)
        sfdc_tokens = get_salesforce_access_token(
            auth_url=secrets["authorization_endpoint"],
            consumer_key=secrets["client_id"],
            consumer_secret=secrets["client_secret"]
        )
        sf = Salesforce(instance_url=sfdc_tokens[1], session_id=sfdc_tokens[0])
        print("✅ Authentication successful.")
        return sf
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        raise e


def query_salesforce(sf, object_name, fields, filter_condition=None):
    """Run a SOQL query and return results using the Bulk API."""
    base_query = f"SELECT {', '.join(fields)} FROM {object_name}"
    if filter_condition:
        base_query += f" WHERE {filter_condition}"
    print(f"🔍 Running SOQL query: {base_query}")

    try:
        results = sf.bulk.__getattr__(object_name).query(base_query)
        return results
    except Exception as e:
        print(f"❌ Query failed: {e}")
        raise e


def write_to_csv(file_name, records, fields):
    """Write records to CSV with specified fields."""
    file_path = os.path.join(OUTPUT_DIRECTORY, file_name)
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for record in records:
            row = {field: record.get(field, "") for field in fields}
            writer.writerow(row)
    return file_path


def main():
    # Hardcoded values
    object_name = "user"
    instance ="legacy"
    # secret_name= "TFM-SFDC-UAT"
    secret_name ="CURRENT-SFDC-PROD"
    # secret_name="CURRENT-SFDC-STAGING"
    # secret_name = "TFM-SFDC-PREPROD-EDM"
    fields =  ["Id","Email","Username","IsActive","FirstName"]

    # Optional filter condition (edit as needed)
    filter_condition = """ Decision_For_TFM_Migration__c in ('Migrate-BC-3')   and TFM_Migration_Status__c =null"""
    print("🔐 Step 1: Authenticating with Salesforce...")
    sf = authenticate_salesforce(secret_name)
    if not sf:
        print("❌ Authentication failed. Exiting...")
        return

    print(f"📥 Step 2: Querying {object_name} records with filter: {filter_condition or 'None'}")
    records = query_salesforce(sf, object_name, fields, filter_condition)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{instance}/sf1_{object_name}_extract12_{timestamp}.csv"
    csv_path = write_to_csv(file_name, records, fields)

    print(f"\n✅ Extraction complete. File saved to: {csv_path}")
    print(f"📊 Total records extracted: {len(records)}")


if __name__ == "__main__":
    main()
