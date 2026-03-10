from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, regexp_replace
import boto3
import os
import glob
import shutil



# --------------------------------------------------
# Helper: Read CSV with Options
# --------------------------------------------------
def read_csv_with_options(spark, path, header=True, recursive=False, multiline=True, quote='"', escape='"'):
    """
    Reads a CSV file with common options and returns a Spark DataFrame.
    """
    options = {
        "header": str(header).lower(),
        "quote": quote,
        "escape": escape,
        "multiLine": str(multiline).lower()
    }

    if recursive:
        options["recursiveFileLookup"] = "true"

    print(f"Reading CSV from: {os.path.abspath(path)}")
    return spark.read.options(**options).csv(path)


# --------------------------------------------------
# Helper: Convert Spark output folder to single CSV
# --------------------------------------------------
def finalize_single_csv(output_dir, final_csv_path):
    """
    Converts Spark output folder into a single CSV by:
    - Finding the part file
    - Moving it to final path
    - Removing .crc files and _SUCCESS
    - Deleting the Spark folder
    """

    # find part file
    part_files = glob.glob(os.path.join(output_dir, "part-*"))
    if not part_files:
        raise Exception(f"No part files found in {output_dir}")

    part_file = part_files[0]

    # Move & rename
    shutil.move(part_file, final_csv_path)

    # Remove spark output directory
    shutil.rmtree(output_dir)

    print(f"Created file: {final_csv_path}")


# --------------------------------------------------
# Main Script
# --------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
print("Project root:", PROJECT_ROOT)

# Get AWS Credentials
session = boto3.Session(profile_name='saml-uat')
creds = session.get_credentials().get_frozen_credentials()

os.environ['AWS_ACCESS_KEY_ID'] = creds.access_key
os.environ['AWS_SECRET_ACCESS_KEY'] = creds.secret_key
os.environ['AWS_SESSION_TOKEN'] = creds.token

# Initialize Spark
spark = SparkSession.builder \
    .appName("S3 Access with Explicit Credentials") \
    .config("spark.hadoop.fs.s3a.access.key", creds.access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key) \
    .config("spark.hadoop.fs.s3a.session.token", creds.token) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)


# --------------------------------------------------
# Inputs
# --------------------------------------------------
object = "prod-account"
# instance = "salesforce"

source_path = "./extract/legacy/sf1_lead_extract12_20260309_111741.csv"
# tfm_path = "./extract/tfm-prod/sf_account_extract21_20260226_095239.csv"
# public_group_path="./extract/user/Untitled spreadsheet - public_group_ids.csv"
# BASE_COUNTRY_STATE_MAPPING_PATH="./extract/merged/country_states.csv"
# master_path = "./extract/tfm-prod/lead-2_26_2026.csv"
user_path="./legacy_compared_preprod_user/user_matched/part-00000-6808c357-3baa-4946-800e-0394153c68bb-c000.csv"
# master_path = "s3a://bc-edm-prod-us-west-2-tfm-conversion/bc-final/master_raw/prod_data/lead/*"
# master_contact_path = "s3a://bc-edm-prod-us-west-2-tfm-conversion/bc-final/app/merged/contact/*"
# record_type_path= "s3a://bc-edm-uat-us-west-2-tfm-conversion/current-to-tfm/raw/recordtype/*"
# merged_opportunity_path = "s3a://bc-edm-prod-us-west-2-tfm-conversion/bc-final/master_raw/prod_data/opportunity/*"
# merged_contact_path = "s3a://bc-edm-uat-us-west-2-tfm-conversion/bc-final/master_raw/prod_data/contact/*"




# --------------------------------------------------
# Read Files
# # --------------------------------------------------
source_df = read_csv_with_options(spark, source_path, recursive=True)
source_df.createOrReplaceTempView("source")
print("source count:", source_df.count())

# tfm_df = read_csv_with_options(spark, tfm_path, recursive=True)
# tfm_df.createOrReplaceTempView("success")
# print("success count:", tfm_df.count())
#
# merged_df = read_csv_with_options(spark, master_path, recursive=True)
# merged_df.createOrReplaceTempView("merged_lead")
# # print("merged_contact count:", merged_df.count())
#
# country_df = read_csv_with_options(spark, BASE_COUNTRY_STATE_MAPPING_PATH, recursive=True)
# country_df.createOrReplaceTempView("country_state_mapping")
# # print("success count:", tfm_df.count())

# public_df = read_csv_with_options(spark, public_group_path, recursive=True)
# public_df.createOrReplaceTempView("public_group")
# print("success count:", user_df.count())

user_df = read_csv_with_options(spark, user_path, recursive=True)
user_df.createOrReplaceTempView("user")
# print("success count:", user_df.count())

# record_type_df = read_csv_with_options(spark, record_type_path, recursive=True)
# record_type_df.createOrReplaceTempView("record_type_table")
# # print("success count:", master_df.count())

# master_df = read_csv_with_options(spark, master_contact_path, recursive=True)
# master_df.createOrReplaceTempView("merged_contact_mapping")
# # print("master count:", master_df.count())

# # # #
# master_df = read_csv_with_options(spark, merged_opportunity_path, recursive=True)
# master_df.createOrReplaceTempView("master_opportunity")
# # # print("success count:", master_account_df.count())
#
# master_contact_df = read_csv_with_options(spark, merged_contact_path, recursive=True)
# master_contact_df.createOrReplaceTempView("master_contact")
# # print("success count:", master_contact_df.count())

# --------------------------------------------------
# SQL Transformation
# --------------------------------------------------gho

sql = f"""

WITH lead_source AS ( 
SELECT 
s.Id,
from source src
LEFT JOIN user u
ON src.OwnerId=u.legacy_user_id 
)

select * FROM certified 


"""
# ,
# CASE WHEN ls.IsConverted='TRUE' THEN ls.ConvertedContactId END as Customer_Contact1__c,
# CASE WHEN ls.IsConverted='FALSE' THEN ls.Id END as Lead__c
# LEFT JOIN lead_source ls
# ON ls.Migration_Id__c=s.Lead__c

certified_df = spark.sql(sql)
print("Certified count:", certified_df.count())
certified_df.show(5, truncate=False)

#
# Split
#
# --------------------------------------------------
# Write Output (Spark folder → single clean CSV)
# # --------------------------------------------------
certified_tmp = f"./prod/{object}/output_{object}_update_tmp"
certified_final = f"./prod/{object}/update_{object}_update.csv"


certified_df.coalesce(1).write.mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("quoteAll", "true") \
    .option("multiLine", "true") \
    .option("encoding", "UTF-8") \
    .option("lineSep", "\n") \
    .csv(certified_tmp)

finalize_single_csv(certified_tmp, certified_final)

