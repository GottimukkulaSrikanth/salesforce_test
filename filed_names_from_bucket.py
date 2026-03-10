from pyspark.sql import SparkSession, Row
import boto3
import os
import glob
import shutil

# --------------------------------------------------
# Helper: Convert Spark output folder to single CSV
# --------------------------------------------------
def finalize_single_csv(output_dir, final_csv_path):

    part_files = glob.glob(os.path.join(output_dir, "part-*"))
    if not part_files:
        raise Exception(f"No part files found in {output_dir}")

    part_file = part_files[0]

    shutil.move(part_file, final_csv_path)
    shutil.rmtree(output_dir)

    print(f"Created file: {final_csv_path}")


# --------------------------------------------------
# AWS SESSION (Profile + Fallback)
# --------------------------------------------------
try:
    session = boto3.Session(profile_name='saml-prod')
except Exception:
    session = boto3.Session()

creds = session.get_credentials().get_frozen_credentials()

# Set environment variables for Spark S3A
os.environ['AWS_ACCESS_KEY_ID'] = creds.access_key
os.environ['AWS_SECRET_ACCESS_KEY'] = creds.secret_key
os.environ['AWS_SESSION_TOKEN'] = creds.token

# --------------------------------------------------
# Initialize Spark
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("S3 Header Extractor") \
    .config("spark.hadoop.fs.s3a.access.key", creds.access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key) \
    .config("spark.hadoop.fs.s3a.session.token", creds.token) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------------
# S3 Details
# --------------------------------------------------
bucket_name = "bc-edm-prod-us-west-2-tfm-conversion"
base_prefix = "bc-final/load_output/success/"

s3_client = session.client("s3")

# --------------------------------------------------
# Step 1: List All Subfolders
# --------------------------------------------------
response = s3_client.list_objects_v2(
    Bucket=bucket_name,
    Prefix=base_prefix,
    Delimiter="/"
)

folders = [cp["Prefix"] for cp in response.get("CommonPrefixes", [])]

print("\nFound folders:")
for f in folders:
    print(f)

# --------------------------------------------------
# Step 2: Extract Headers (One File Per Folder)
# --------------------------------------------------
header_rows = []

for folder in folders:

    # Extract only object name (account, contact, etc.)
    object_name = folder.rstrip("/").split("/")[-1]

    print(f"\nProcessing object: {object_name}")

    files_response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=folder
    )

    files = [
        obj["Key"]
        for obj in files_response.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]

    if not files:
        print("No CSV files found.")
        continue

    first_file = files[0]
    s3_path = f"s3a://{bucket_name}/{first_file}"

    print("Reading file:", s3_path)

    df = spark.read.option("header", "true").csv(s3_path)

    # Extract column names only
    for col_name in df.columns:
        header_rows.append(Row(object_name=object_name,
                               column_name=col_name))

# --------------------------------------------------
# Step 3: Create DataFrame
# --------------------------------------------------
if not header_rows:
    raise Exception("No headers found.")

headers_df = spark.createDataFrame(header_rows)

print("\nHeader Preview:")
headers_df.show(50, truncate=False)

# --------------------------------------------------
# Step 4: Write Single Clean CSV
# --------------------------------------------------
output_tmp = "./all_headers_tmp"
output_final = "./all_folder_headers.csv"

headers_df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(output_tmp)

finalize_single_csv(output_tmp, output_final)

print("\nDone ✅")
