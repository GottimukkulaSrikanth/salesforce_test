import os
import base64
import time
from simple_salesforce import Salesforce
import boto3
import json
from botocore.exceptions import ClientError

BASE_OUTPUT_DIR = "./extract/legacy"
ATTACHMENT_DIR = os.path.join(BASE_OUTPUT_DIR, "attachments")

# -------------------------------
# Salesforce Auth via Secrets
# -------------------------------
aws_session = boto3.Session(profile_name="saml-uat")
secrets_client = aws_session.client(service_name="secretsmanager")

def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as e:
        print(f"❌ Error retrieving secret: {e}")
        raise

def get_salesforce_access_token(auth_url, consumer_key, consumer_secret):
    import requests
    auth_data = {
        "grant_type": "client_credentials",
        "client_id": consumer_key,
        "client_secret": consumer_secret,
    }
    response = requests.post(auth_url, data=auth_data, timeout=15)
    response.raise_for_status()
    data = response.json()
    return data["access_token"], data["instance_url"]

def authenticate_salesforce(secret_name):
    secrets = get_secret(secret_name)
    token, instance_url = get_salesforce_access_token(
        secrets["authorization_endpoint"],
        secrets["client_id"],
        secrets["client_secret"]
    )
    sf = Salesforce(instance_url=instance_url, session_id=token)
    print("✅ Salesforce authentication successful")
    return sf

# -------------------------------
# Insert attachments
# -------------------------------
def upload_attachments():
    sf = authenticate_salesforce("TFM-SFDC-PREPROD-EDM")

    files = [f for f in os.listdir(ATTACHMENT_DIR) if os.path.isfile(os.path.join(ATTACHMENT_DIR, f))]
    print(f"📁 Found {len(files)} files to upload as attachments")

    success_count = 0
    failure_count = 0

    for filename in files:
        try:
            # Extract migration_id from the filename
            migration_id = filename.split("_")[0]

            # Remove migration_id from the filename for Salesforce
            actual_file_name = "_".join(filename.split("_")[1:])

            # Get the contract Id
            contract_query = f"SELECT Id FROM ironclad__Ironclad_Contract__c WHERE Migration_Id__c = '{migration_id}' LIMIT 1"
            contract_records = sf.query(contract_query)["records"]
            if not contract_records:
                print(f"⚠️ Contract not found for Migration_Id__c={migration_id}, skipping {filename}")
                failure_count += 1
                continue

            parent_id = contract_records[0]["Id"]

            # Read file
            file_path = os.path.join(ATTACHMENT_DIR, filename)
            with open(file_path, "rb") as f:
                file_data = f.read()

            # Insert attachment
            attachment_result = sf.Attachment.create({
                "Name": actual_file_name,  # Use cleaned filename
                "ParentId": parent_id,
                "Body": base64.b64encode(file_data).decode("utf-8"),
                "ContentType": "application/pdf"
            })

            print(f"✅ Uploaded Attachment: {actual_file_name}, Id: {attachment_result['id']}, ParentId: {parent_id}")
            success_count += 1
            time.sleep(0.2)  # avoid hitting API limits

        except Exception as e:
            failure_count += 1
            print(f"❌ Failed for file {filename}: {e}")

    print("\n✅ Upload Summary")
    print(f"   ✔ Successful uploads: {success_count}")
    print(f"   ❌ Failed uploads: {failure_count}")

if __name__ == "__main__":
    upload_attachments()
