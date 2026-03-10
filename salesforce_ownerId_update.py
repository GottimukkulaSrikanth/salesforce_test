from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
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


# --------------------------------------------------
# Inputs
# --------------------------------------------------
object = "lead"

source_path = "./extract/legacy/sf1_lead_extract12_20260309_111741.csv"
# tfm_path = "./extract/tfm-prod/sf_contact_extract21_20260212_143423.csv"
merged_user_path = "./legacy_compared_preprod_user/user_matched/part-00000-6808c357-3baa-4946-800e-0394153c68bb-c000.csv"

# --------------------------------------------------
# Read Files
# --------------------------------------------------
source_df = read_csv_with_options(spark, source_path, recursive=True)
source_df.createOrReplaceTempView("source")
print("source count:", source_df.count())

# tfm_df = read_csv_with_options(spark, tfm_path, recursive=True)
# tfm_df.createOrReplaceTempView("success")
# print("success count:", tfm_df.count())

user_df = read_csv_with_options(spark, merged_user_path, recursive=True)
user_df.createOrReplaceTempView("user")
print("user count:", user_df.count())


# --------------------------------------------------
# SQL Transformation
# --------------------------------------------------
sql1 = f"""
    WITH source_success AS (
        SELECT 
            src.Id AS Id, 
            src.Id AS Migration_Id__c,
            src.OwnerId AS src_OwnerId
        FROM source src
    ),

    joined AS (
        SELECT 
            ss.Id,
            ss.Migration_Id__c,
            ss.src_OwnerId,
            u.legacy_user_id,
            u.tfm_user_id,
            u.legacy_email,
            u.legacy_username,
            u.tfm_email,
            u.tfm_username,
            TRIM(u.legacy_isactive) AS legacy_isactive,
            u.tfm_isactive
        FROM source_success ss
        LEFT JOIN user u
            ON TRIM(ss.src_OwnerId) = TRIM(u.legacy_user_id)
    )

    SELECT
        Id,
        Migration_Id__c,
        src_OwnerId,
        tfm_user_id AS OwnerId,
        legacy_email,
        legacy_username,
        legacy_isactive,
        tfm_isactive,
        tfm_email,
        tfm_username
    FROM joined 
    
"""

owner_df = spark.sql(sql1)
print("Certified count:", owner_df.count())

# Split
matched_df = owner_df.filter((col("OwnerId").isNotNull()) & (col("OwnerId") != "")).select("Id", "OwnerId")
unmatched_df = owner_df.filter((col("OwnerId").isNull()) | (col("OwnerId") == ""))
unmatched_summary_df = unmatched_df.groupBy("src_OwnerId", "legacy_email", "legacy_username","legacy_isactive").agg(count("*").alias("count"))

# unmatched_summary_df = owner_df.groupBy("OwnerId", "tfm_email", "tfm_username").agg(count("*").alias("count"))
print("Matched count:", matched_df.count())
print("Unmatched count:", unmatched_df.count())
# --------------------------------------------------
# Write Output (Spark folder → single clean CSV)
# --------------------------------------------------
matched_tmp = f"./preprod/owner_id_update/matched/{object}/output_{object}_update_tmp"
matched_final = f"./preprod/owner_id_update/matched/{object}/output_{object}_update.csv"

unmatched_tmp = f"./preprod/owner_id_update/unmatched/{object}/output_{object}_update_tmp"
unmatched_final = f"./preprod/owner_id_update/unmatched/{object}/output_{object}_update.csv"

# Write Spark outputs
matched_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(matched_tmp)
unmatched_summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(unmatched_tmp)

# Convert Spark folder to single CSV
finalize_single_csv(matched_tmp, matched_final)
finalize_single_csv(unmatched_tmp, unmatched_final)
