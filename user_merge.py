from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3
import os

# Get AWS credentials using boto3
session = boto3.Session(profile_name='saml-uat')
creds = session.get_credentials().get_frozen_credentials()

os.environ['AWS_ACCESS_KEY_ID'] = creds.access_key
os.environ['AWS_SECRET_ACCESS_KEY'] = creds.secret_key
os.environ['AWS_SESSION_TOKEN'] = creds.token

spark = SparkSession.builder \
    .appName("S3 Access with Explicit Credentials") \
    .config("spark.hadoop.fs.s3a.access.key", creds.access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key) \
    .config("spark.hadoop.fs.s3a.session.token", creds.token) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Example: Read a CSV file from S3
legacy_user_s3_path = "./extract/legacy/sf1_user_extract12_20260309_111825.csv"
tfm_user_s3_path = "./extract/tfm-preprod/sf_user_extract_20260309_111843.csv"

legacy_df = spark.read.option("header", "true").csv(legacy_user_s3_path)
print("Count:", legacy_df.count())
tfm_df = spark.read.option("header", "true").csv(tfm_user_s3_path)
print("Count:", tfm_df.count())
object = "user"

legacy_df.createOrReplaceTempView("legacy_user")
tfm_df.createOrReplaceTempView("tfm_user")
sql = """
WITH legacy_parsed AS (
  SELECT
    id AS legacy_id,
    isactive AS legacy_isactive,
    email AS legacy_email,
    username AS legacy_username,

    LOWER(
      CONCAT(
        SPLIT(email, '@')[0], '@',
        CASE
          WHEN regexp_extract(email, '@([^\\.]+)', 1) IN ('bigcommerce','feedonomics','commerce')
          THEN 'bigcommerce'
          ELSE regexp_extract(email, '@([^\\.]+)', 1)
        END
      )
    ) AS email_trimmed,

    LOWER(
      CONCAT(
        SPLIT(username, '@')[0], '@',
        CASE
          WHEN regexp_extract(username, '@([^\\.]+)', 1) IN ('bigcommerce','feedonomics','commerce')
          THEN 'bigcommerce'
          ELSE regexp_extract(username, '@([^\\.]+)', 1)
        END
      )
    ) AS username_trimmed
  FROM legacy_user
),

tfm_parsed AS (
  SELECT
    id AS tfm_id,
    isactive AS tfm_isactive,
    email AS tfm_email,
    username AS tfm_username,

    LOWER(
      CONCAT(
        SPLIT(email, '@')[0], '@',
        CASE
          WHEN regexp_extract(email, '@([^\\.]+)', 1) IN ('bigcommerce','feedonomics','commerce')
          THEN 'bigcommerce'
          ELSE regexp_extract(email, '@([^\\.]+)', 1)
        END
      )
    ) AS email_trimmed,

    LOWER(
      CONCAT(
        SPLIT(username, '@')[0], '@',
        CASE
          WHEN regexp_extract(username, '@([^\\.]+)', 1) IN ('bigcommerce','feedonomics','commerce')
          THEN 'bigcommerce'
          ELSE regexp_extract(username, '@([^\\.]+)', 1)
        END
      )
    ) AS username_trimmed
  FROM tfm_user
),

joined AS (
  SELECT
    l.legacy_id AS legacy_user_id,
    l.legacy_isactive,
    l.legacy_email,
    l.legacy_username,
    t.tfm_id AS tfm_user_id,
    t.tfm_isactive,
    t.tfm_email,
    t.email_trimmed AS tfm_email_trimmed,
    t.tfm_username,
    t.username_trimmed AS tfm_username_trimmed,
    ROW_NUMBER() OVER (
      PARTITION BY l.legacy_id
      ORDER BY t.tfm_id
    ) AS rn
  FROM legacy_parsed l
  LEFT JOIN tfm_parsed t
    ON l.email_trimmed = t.email_trimmed
   AND l.username_trimmed = t.username_trimmed
)

SELECT * FROM joined WHERE rn = 1;

"""

# #
owner_df = spark.sql(sql)
# Display
print("✅ Matched Records:")
# matched_df.show(10, False)
print("Count:", owner_df.count())
# owner_df.show(10,truncate=False)
# # Save to local (or replace with S3 path if needed)
matched_df = owner_df.filter((col("tfm_user_id").isNotNull()) & (col("tfm_user_id") != "")).select ("legacy_user_id","tfm_user_id")
owner_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"./legacy_compared_preprod_user/{object}_matched")
matched_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"./legacy_compared_preprod_user/{object}")

