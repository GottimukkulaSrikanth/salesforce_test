import json
import os
import time
import re
from simple_salesforce import Salesforce
import boto3
import requests
from botocore.exceptions import ClientError
from datetime import datetime

# -------------------------------
# Output directories
# -------------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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
    return sf, instance_url


# -------------------------------
# Queries
# -------------------------------
CONTENT_DOCUMENT_LINK_QUERY = """
SELECT LinkedEntityId,
       ContentDocumentId
FROM ContentDocumentLink
WHERE LinkedEntityId IN (
    SELECT Id
    FROM ironclad__Ironclad_Contract__c
    WHERE ironclad__Account__r.TFM_Migration_Status__c
          IN ('Migrated-PREPROD','Migrated-PROD','Migrated-PREPROD(Partner)','Migrated-PROD(Partner)')
    AND DAY_ONLY(CreatedDate) <= 2025-11-20
)
"""

CONTENT_VERSION_QUERY = """
SELECT Id,
       ContentDocumentId,
       Title,
       FileExtension
FROM ContentVersion
WHERE IsLatest = true
AND ContentDocumentId = '{}'
"""


# -------------------------------
# Main logic
# -------------------------------
def main():
    secret_name = "CURRENT-SFDC-PROD"
    print("🔐 Authenticating to Salesforce...")
    sf, instance_url = authenticate_salesforce(secret_name)

    print("🔍 Querying ContentDocumentLink...")
    cdl_results = sf.query_all(CONTENT_DOCUMENT_LINK_QUERY)["records"]
    print(f"📊 Found {len(cdl_results)} document links")

    success_count = 0
    failure_count = 0

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {sf.session_id}"
    })

    for rec in cdl_results:
        linked_entity_id = rec["LinkedEntityId"]
        content_doc_id = rec["ContentDocumentId"]

        try:
            # Get latest ContentVersion for this document
            cv_query = CONTENT_VERSION_QUERY.format(content_doc_id)
            cv_result = sf.query(cv_query)["records"]

            if not cv_result:
                print(f"⚠️ No ContentVersion found for {content_doc_id}")
                continue

            cv = cv_result[0]
            cv_id = cv["Id"]
            title = cv["Title"]
            ext = cv.get("FileExtension")

            # ✅ Keep your filename logic intact
            doc_title_stripped = re.sub(r'\.\w{3,4}$', '', title, flags=re.IGNORECASE).strip()
            safe_title = doc_title_stripped.replace("/", "_").replace("\\", "_")
            filename = f"{linked_entity_id}_{safe_title}"
            if ext:
                filename += f".{ext}"

            file_path = os.path.join(ATTACHMENT_DIR, filename)

            # 💥 Download file via Salesforce REST API
            version_data_url = f"{sf.base_url}sobjects/ContentVersion/{cv_id}/VersionData"
            with session.get(version_data_url, stream=True, timeout=60) as response:
                response.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

            success_count += 1
            print(f"⬇️ Downloaded: {filename}")
            time.sleep(0.2)

        except Exception as e:
            failure_count += 1
            print(f"❌ Failed for ContentDocumentId {content_doc_id}: {e}")

    print("\n✅ Download Summary")
    print(f"   ✔ Successful downloads: {success_count}")
    print(f"   ❌ Failed downloads: {failure_count}")
    print(f"📁 Files saved in: {ATTACHMENT_DIR}")


if __name__ == "__main__":
    main()
