import json
import os
import time
from simple_salesforce import Salesforce
import boto3
import requests
from botocore.exceptions import ClientError
from datetime import datetime

# -------------------------------
# Output directories
# -------------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
current_date = datetime.now().strftime("%Y%m%d")
BASE_OUTPUT_DIR = f"s3a://bc-edm-uat-us-west-2-tfm-conversion/attachments/{timestamp}/"
ATTACHMENT_DIR = os.path.join(BASE_OUTPUT_DIR, "attachments")
os.makedirs(ATTACHMENT_DIR, exist_ok=True)

# -------------------------------
# AWS Secrets Manager & SF Auth
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
# SOQL Query (Attachment only)
# -------------------------------
ATTACHMENT_QUERY = """
SELECT Id,
       ParentId,
       Name
FROM Attachment
WHERE ParentId IN (
    SELECT Id
    FROM ironclad__Ironclad_Contract__c
    WHERE ironclad__Account__r.TFM_Migration_Status__c
          IN ('Migrated-PREPROD','Migrated-PROD','Migrated-PREPROD(Partner)','Migrated-PROD(Partner)')
    AND DAY_ONLY(CreatedDate) <= 2025-11-20
)
"""


# -------------------------------
# Main logic
# -------------------------------
def main():
    secret_name = "CURRENT-SFDC-PROD"
    print("🔐 Authenticating to Salesforce...")
    sf = authenticate_salesforce(secret_name)

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {sf.session_id}"
    })

    print("📎 Querying legacy Attachments...")
    attachment_results = sf.query_all(ATTACHMENT_QUERY)["records"]
    print(f"📊 Found {len(attachment_results)} attachments")

    success_count = 0
    failure_count = 0

    for att in attachment_results:
        try:
            attachment_id = att["Id"]
            parent_id = att["ParentId"]
            name = att["Name"]

            # sanitize filename
            safe_name = name.replace("/", "_").replace("\\", "_")
            filename = f"{parent_id}_{safe_name}"
            file_path = os.path.join(ATTACHMENT_DIR, filename)

            attachment_body_url = (
                f"{sf.base_url}sobjects/Attachment/{attachment_id}/Body"
            )

            with session.get(attachment_body_url, stream=True, timeout=60) as response:
                response.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

            success_count += 1
            print(f"⬇️ Downloaded Attachment: {filename}")
            time.sleep(0.2)

        except Exception as e:
            failure_count += 1
            print(f"❌ Failed Attachment {attachment_id}: {e}")

    print("\n✅ Download Summary")
    print(f"   ✔ Successful downloads: {success_count}")
    print(f"   ❌ Failed downloads: {failure_count}")
    print(f"📁 Files saved in: {ATTACHMENT_DIR}")


if __name__ == "__main__":
    main()
