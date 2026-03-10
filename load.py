import json
import os
import time
from datetime import timezone, datetime
import boto3
import requests
import csv
from botocore.exceptions import ClientError
from simple_salesforce import Salesforce
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import broadcast, col, count
import datetime as dt


csv.field_size_limit(sys.maxsize)


LOCAL_DIRECTORY = '/tmp/process_files/'
os.makedirs(LOCAL_DIRECTORY, exist_ok=True)

aws_session = boto3
s3_client = aws_session.client("s3")
secrets_client = aws_session.client(service_name='secretsmanager')

def get_secret(secret_name):
    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    return json.loads(get_secret_value_response["SecretString"])


def get_salesforce_access_token(auth_url, consumer_key, consumer_secret):
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
    try:
        secrets = get_secret(secret_name)
        sfdc_tokens = get_salesforce_access_token(
            auth_url=secrets["authorization_endpoint"],
            consumer_key=secrets["client_id"],
            consumer_secret=secrets["client_secret"]
        )
        sf = Salesforce(instance_url=sfdc_tokens[1], session_id=sfdc_tokens[0], version="62.0")
        print("Authentication successful.")
        return sf
    except Exception as e:
        print(f"Authentication failed: {e}")
        raise e


# -----------------------------------------------------------
#  CSV PROCESSING
# -----------------------------------------------------------
def process_csv_file(bucket_name, file_key, object_name):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = obj['Body'].read().decode('utf-8').splitlines()

    input_csv_record_count = len(csv_content) - 1  # header excluded

    local_file_name = os.path.join(LOCAL_DIRECTORY, os.path.basename(file_key))

    now = datetime.now(timezone.utc)
    formatted_date = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    with open(local_file_name, 'w') as f:
        if object_name in ('account', 'lead', 'opportunity', 'contact','product2','opportunitylineitem','partnercommissionsummary__c','accountteammember','store__c','storecontactrole__c','salesreferral__c','opportunitystoreassociation__c','task','ironclad__ironclad_contract__c','event','campaign','certification__c','invoicetransaction__c','deal_registration__c','project__c','getfeedback_aut__survey__c','getfeedback_aut__answer__c','getfeedback_aut__response__c','projectrole__c','projectproduct__c','ironclad__ironclad_workflow__c','ironclad__ironclad_signature__c','campaignmember','case','billingaccount__c','emailmessage','partnerapp__c','partnercommissionpayment__c','partnerpayment__c','partnercommissioninvoicetransaction__c','product_account__c','subscription__c'):
            for index, line in enumerate(csv_content):
                if index == 0:
                    f.write(line + ",Migrated_Date__c\n")
                else:
                    f.write(line + f",{formatted_date}\n")
        else:
            f.write('\n'.join(csv_content))

    return local_file_name, input_csv_record_count


def save_file_locally(file_name, content):
    file_path = os.path.join(LOCAL_DIRECTORY, file_name)
    with open(file_path, 'w') as file:
        file.write(content)
    return file_path


def upload_file_to_s3(local_file_path, s3_bucket, s3_key):
    s3_client.upload_file(local_file_path, s3_bucket, s3_key)
    print(f"Uploaded {local_file_path} to s3://{s3_bucket}/{s3_key}")


def count_csv_records(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf8') as csv_file:
            reader = csv.reader(csv_file)
            return sum(1 for _ in reader) - 1
    return 0

#  BULK LOAD + MASTER RAW CLEANUP 

def bulk_insert_records(sf, object_name, s3_bucket, local_file_name, object_path, totals):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bulk_operation = getattr(sf.bulk2, object_name).insert
    results = bulk_operation(local_file_name, batch_size=20000)

    success_files_to_upload = []

    for result in results:
        job_id = result['job_id']
        print(f"\nProcessing Salesforce Job ID: {job_id}")

        bulk_failed = getattr(sf.bulk2, object_name).get_failed_records(job_id)
        bulk_success = getattr(sf.bulk2, object_name).get_successful_records(job_id)
        bulk_unprocessed = getattr(sf.bulk2, object_name).get_unprocessed_records(job_id)

        error_file = save_file_locally(f"error_{job_id}_{timestamp}.csv", bulk_failed)
        success_file = save_file_locally(f"success_{job_id}_{timestamp}.csv", bulk_success)
        unprocessed_file = save_file_locally(f"unprocessed_{job_id}_{timestamp}.csv", bulk_unprocessed)

        success_files_to_upload.append(success_file)

        success_key = f'bc-final/load_output/success/{object_path}/{os.path.basename(success_file)}'
        error_key = f'bc-final/load_output/error/{object_path}/{os.path.basename(error_file)}'

        upload_file_to_s3(error_file, s3_bucket, error_key)
        upload_file_to_s3(success_file, s3_bucket, success_key)


        # -------------------------------------------------------------
        # NEW: Upload extra success files ONLY for account & contact
        # Exact path: bc-final/tfm-preprod/success/<object_name>/
        # -------------------------------------------------------------
        if object_name.lower() in ["account", "contact"]:
            extra_success_key = (
                f"bc-final/tfm-preprod/success/{object_name.lower()}/"
                f"{os.path.basename(success_file)}"
            )
            upload_file_to_s3(success_file, s3_bucket, extra_success_key)
            print(f"Uploaded extra success file to: s3://{s3_bucket}/{extra_success_key}")
        # -------------------------------------------------------------

        success_count = count_csv_records(success_file)
        error_count = count_csv_records(error_file)
        unprocessed_count = count_csv_records(unprocessed_file)

        totals["success"] += success_count
        totals["errors"] += error_count
        totals["unprocessed"] += unprocessed_count

        print("\n-----------------------------------------------")
        print(f" Job ID:             {job_id}")
        print(f" Success file:       {os.path.basename(success_file)} ({success_count} records)")
        print(f" Error file:         {os.path.basename(error_file)} ({error_count} records)")
        print(f" Unprocessed file:   {os.path.basename(unprocessed_file)} ({unprocessed_count} records)")
        print("-----------------------------------------------\n")

    # CLEAN MASTER RAW
    master_raw_prefix = f'bc-final/master_raw/success/{object_path}/'

    print(f"Cleaning old master_raw files in s3://{s3_bucket}/{master_raw_prefix}")

    existing_objects = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=master_raw_prefix)
    if 'Contents' in existing_objects:
        for obj in existing_objects['Contents']:
            print(f"Deleting: {obj['Key']}")
            s3_client.delete_object(Bucket=s3_bucket, Key=obj['Key'])

    # Upload new success
    print("\nUploading new success files to master_raw...")

    for success_file in success_files_to_upload:
        master_raw_key = master_raw_prefix + os.path.basename(success_file)
        upload_file_to_s3(success_file, s3_bucket, master_raw_key)

    print("Master raw updated.\n")


def get_files_s3_location(s3_bucket, csv_file_key):
    response = s3_client.list_objects(Bucket=s3_bucket, Prefix=csv_file_key)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]


# -----------------------------------------------------------
#   MASTER RAW SPARK PROCESSING
# -----------------------------------------------------------
def run_master_raw_processing(bucket_name, object_name):
    try:
        print("\n===============================================")
        print("      STARTING MASTER RAW JOIN PROCESS")
        print("===============================================")

        sc = SparkContext.getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session

        base_path = "bc-final"

        input_path = f"s3://{bucket_name}/{base_path}/raw/{object_name.lower()}/"
        success_path = f"s3://{bucket_name}/{base_path}/master_raw/success/{object_name.lower()}/"
        joined_prefix = f"s3://{bucket_name}/{base_path}/master_raw/joined_output/{object_name.lower()}/"
        prod_prefix = f"s3://{bucket_name}/{base_path}/master_raw/prod_data/{object_name.lower()}/"

        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"{joined_prefix}{object_name}_delta_{timestamp}"

        print(f"Input path: {input_path}")
        print(f"Latest success CSV prefix: {success_path}")
        print(f"Output directory: {output_dir}")

        input_df = spark.read.format("csv") \
                        .option("header", True) \
                        .option("quote", '"') \
                        .option("escape", '"') \
                        .option("multiLine", True) \
                        .option("recursiveFileLookup", True) \
                        .load(input_path)
       # spark.read.option("recursiveFileLookup", "true") \
            #.option("header", True).format("csv").load(input_path)

        input_count = input_df.count()
        print(f"Total input records: {input_count}")

        # DUP CHECK
        dup_df = input_df.groupBy("Id").agg(count("*").alias("count")).filter("count > 1")
        if dup_df.count() > 0:
            print(f"⚠️ Found {dup_df.count()} duplicate ID(s) in input:")
            dup_df.show(truncate=False)
        else:
            print("✅ No duplicate ID records found in input.")

        success_df = spark.read.format("csv") \
                            .option("header", True) \
                            .option("quote", '"') \
                            .option("escape", '"') \
                            .option("multiLine", True) \
                            .option("recursiveFileLookup", True) \
                            .load(success_path)
        #spark.read.option("recursiveFileLookup", "true") \
            #.option("header", True).format("csv").load(success_path)

        success_count = success_df.count()
        print(f"Total success records: {success_count}")

        if success_count < input_count:
            filtered_df = input_df.join(
                broadcast(success_df.select("Migration_Id__c").dropDuplicates()),
                input_df["Id"] == success_df["Migration_Id__c"],
                "inner"
            )
        else:
            filtered_df = input_df.join(
                success_df.select("Migration_Id__c").dropDuplicates(),
                input_df["Id"] == success_df["Migration_Id__c"],
                "inner"
            )

        new_count = filtered_df.count()
        print(f"New records to write: {new_count}")

        if new_count > 0:
            filtered_df.coalesce(1).write.mode("overwrite") \
                        .format("csv") \
                        .option("header", True) \
                        .option("quote", '"') \
                        .option("escape", '"') \
                        .option("quoteMode", "ALL") \
                        .save(output_dir)

            print("Joined output written.")

            #coalesce(1).write.mode("overwrite") \
                #.option("header", True).csv(output_dir)

            print("Joined output written.")

        # PROD DATA

        if "sf__Id" not in success_df.columns:
            print("❌ Missing sf__Id column in success file")
            return

        prod_df = input_df.join(
            success_df.select("Migration_Id__c", "sf__Id").dropDuplicates(),
            input_df["Id"] == success_df["Migration_Id__c"],
            "inner"
        ).select(input_df["Id"], success_df["Migration_Id__c"], success_df["sf__Id"])

        prod_count = prod_df.count()
        print(f"Records to write to prod_data: {prod_count}")

        if prod_count == 0:
            print("No prod_data to write.")
            return

        temp_prefix = f"{base_path}/master_raw/prod_data/{object_name}/tmp_{timestamp}/"
        temp_path = f"s3://{bucket_name}/{temp_prefix}"

        prod_df.coalesce(1).write.mode("overwrite") \
                .format("csv") \
                .option("header", True) \
                .option("quote", '"') \
                .option("escape", '"') \
                .option("quoteMode", "ALL") \
                .save(temp_path)

        #coalesce(1).write.mode("overwrite") \
            #.option("header", True).csv(temp_path)

        s3 = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=temp_prefix)

        part_file = None
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".csv") and "part" in obj["Key"]:
                part_file = obj["Key"]
                break

        if part_file:
            final_key = f"{base_path}/master_raw/prod_data/{object_name}/{os.path.basename(part_file)}"
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={"Bucket": bucket_name, "Key": part_file},
                Key=final_key
            )
            print(f"Prod data written to s3://{bucket_name}/{final_key}")

            # cleanup temp
            for obj in resp.get("Contents", []):
                s3.delete_object(Bucket=bucket_name, Key=obj["Key"])

        print("✅ Master raw + joined_output + prod_data processing complete.\n")

    except Exception as e:
        print(f"❌ Master raw processing failed: {e}")


def main(object_name, s3_bucket, csv_file_key=None, secret_name="", batch_size=20000):
    print("Step 1: Authenticating with Salesforce...")
    sf = authenticate_salesforce(secret_name)
    if not sf:
        print("Authentication failed.")
        return

    if not csv_file_key:
        print("CSV file key required.")
        return

    csv_files = get_files_s3_location(s3_bucket, csv_file_key)

    totals = {
        "input": 0,
        "success": 0,
        "errors": 0,
        "unprocessed": 0
    }

    for csv_file in csv_files:
        print(f"\nLoading s3://{s3_bucket}/{csv_file}")

        local_file_name, input_csv_count = process_csv_file(s3_bucket, csv_file, object_name)
        print(f"Input CSV record count: {input_csv_count}")

        totals["input"] += input_csv_count

        bulk_insert_records(sf, object_name, s3_bucket, local_file_name, object_path, totals)
        time.sleep(1)

    print("\n===============================================")
    print("            OVERALL LOAD SUMMARY")
    print("===============================================")
    print(f" Total Input Records:       {totals['input']}")
    print(f" Total Successful Records:  {totals['success']}")
    print(f" Total Error Records:       {totals['errors']}")
    print(f" Total Unprocessed Records: {totals['unprocessed']}")
    print("===============================================\n")

    #  NEW: Run Master Raw Processing Automatically
    run_master_raw_processing(s3_bucket, object_name)

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['s3_bucket', 'secret_name'])

    object_name = 'contact'

    partition_year = None
    if 'partition_year' in sys.argv:
        optional_args = getResolvedOptions(sys.argv, ['partition_year'])
        partition_year = optional_args['partition_year']

    object_path = f"{object_name.lower()}/year={partition_year}" if partition_year else object_name.lower()
    csv_file_key = f'bc-final/transformed/{object_path}/'
    

    main(
        object_name=object_name,
        s3_bucket=args['s3_bucket'],
        csv_file_key=csv_file_key,
        secret_name=args['secret_name']
    )
