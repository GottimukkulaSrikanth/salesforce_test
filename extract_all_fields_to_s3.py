import json
import os
import csv
from datetime import datetime

import boto3
import requests
from simple_salesforce import Salesforce
from botocore.exceptions import ClientError

# Output directory for the extracted CSV (local)
OUTPUT_DIRECTORY = './load_output/'
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

# S3 configuration
S3_BUCKET_NAME = "bc-edm-uat-us-west-2-tfm-conversion"
S3_OUTPUT_PREFIX = "current-to-tfm-incremental-update/raw_tfm_data/"  # Optional folder path in the bucket

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


def generate_query(sf, object_name, filter_condition=None):
    """
    Generate a SELECT SOQL query for a Salesforce object,
    excluding compound fields not supported in Bulk API.
    """
    try:
        description = getattr(sf, object_name).describe()

        # Compound fields to skip (not supported in Bulk API)
        compound_fields_to_skip = {
            'BillingAddress', 'ShippingAddress', 'MailingAddress', 'OtherAddress', 'Address',
            'QuoteToAddress', 'AdditionalAddress', 'pkbgeolocalization__c'
        }

        # Field types to skip
        unsupported_types = {"address", "location", "base64", "encryptedstring", "anyType"}

        # Valid fields for Bulk API
        fields = [
            f['name']
            for f in description['fields']
            if f['name'] not in compound_fields_to_skip
            and not f.get('compoundFieldName')  # Avoid parent compound fields
            and f['type'].lower() not in unsupported_types
        ]

        query = f"SELECT {', '.join(fields)} FROM {object_name}"
        if filter_condition:
            query += f" WHERE {filter_condition}"

        print(f"🛠️ Generated SOQL Query:\n{query}")
        return query, fields
    except Exception as e:
        print(f"❌ Failed to generate query for {object_name}: {e}")
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


def upload_to_s3(local_file_path, s3_bucket, s3_key):
    """Upload a file to an S3 bucket."""
    s3_client = aws_session.client("s3")
    try:
        s3_client.upload_file(local_file_path, s3_bucket, s3_key)
        print(f"📁 File uploaded to S3: s3://{s3_bucket}/{s3_key}")
    except ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
        raise e


def main():
    # Configurable values
    object_name = "OpportunityContactRole"
    secret_name = "TFM-SFDC-PREPROD-EDM"

    # Optional SOQL WHERE condition
    filter_condition = """ OpportunityId in (select id from Opportunity 
        where Migrated_Source__c ='Salesforce (legacy)') 
        and role=null and Legacy_SFDC_ID__c =null """

    print("🔐 Step 1: Authenticating with Salesforce...")
    sf = authenticate_salesforce(secret_name)
    if not sf:
        print("❌ Authentication failed. Exiting...")
        return

    print(f"📋 Step 2: Generating SOQL for object: {object_name}")
    query, fields = generate_query(sf, object_name, filter_condition)

    print(f"📥 Step 3: Querying {object_name} records...")
    try:
        records = sf.bulk.__getattr__(object_name).query(query)
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return

    # Create CSV filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"tfm_{object_name}_extract_{timestamp}.csv"

    print("📄 Step 4: Writing records to CSV...")
    csv_path = write_to_csv(file_name, records, fields)

    # Build S3 key
    s3_key = os.path.join(S3_OUTPUT_PREFIX, file_name)

    print("☁️ Step 5: Uploading file to S3...")
    upload_to_s3(csv_path, S3_BUCKET_NAME, s3_key)

    print(f"\n✅ Extraction complete.")
    print(f"📊 Total records extracted: {len(records)}")


if __name__ == "__main__":
    main()
