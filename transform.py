import boto3
from pyspark.sql import SparkSession
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import regexp_replace, col

s3_client = boto3.client("s3")

def load_sql_from_s3(key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    query = obj["Body"].read().decode("utf-8")
    query = '\n'.join(line for line in query.split('\n') if not line.strip().startswith("--"))
    print(f"Loaded SQL query from s3://{bucket_name}/{key}")
    print(f"Query: {query}")
    return query

def get_sql_s3_key(object):
    return f"bc-final/app/sql/{object.lower()}.sql"
    #openlead
    # return f"bc-final/app/open_lead/{object.lower()}.sql"



def create_master_df(object_name):
    s3_input_path = f"s3://{bucket_name}/bc-final/master_raw/prod_data/{object_name.lower()}/{final_path}"
    #s3_input_path = f"s3://{bucket_name}/bc-final/master_raw/prod_data_note/{object_name.lower()}/{final_path}"
    try:
        df = spark.read.option("recursiveFileLookup", "true") \
            .option("header", True) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .format("csv").load(s3_input_path)
        df.printSchema()
        df.createOrReplaceTempView(f"master_{object_name.lower()}")
        print(f"Registered master_{object_name.lower()} view")
    except Exception as e:
        print(f"Failed to load master data for {object_name}: {str(e)}")

def create_success_df(object_name):
    s3_input_path = f"s3://{bucket_name}/bc-final/tfm-preprod/success/{object_name.lower()}/{final_path}"
    try:
        df = spark.read.option("recursiveFileLookup", "true") \
            .option("header", True) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .format("csv").load(s3_input_path)
        df.printSchema()
        df.createOrReplaceTempView(f"success_{object_name.lower()}")
        print(f"Registered success_{object_name.lower()} view")
    except Exception as e:
        print(f"Failed to load success data for {object_name}: {str(e)}")        

            
def create_static_type_df():
    s3_input_path = f"{BASE_RECORD_TYPE_PATH}"
    df = spark.read.option("recursiveFileLookup", "true") \
        .option("header", True) \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("multiLine", "true") \
        .format("csv").load(s3_input_path)
    df.createOrReplaceTempView("recordtype")



def create_country_state_mapping_df():
    s3_input_path = f"{BASE_COUNTRY_STATE_MAPPING_PATH}"
    df = spark.read.option("recursiveFileLookup", "true") \
        .option("header", True).format("csv").load(s3_input_path)
    df.createOrReplaceTempView("country_state_mapping")
    


        
def merged_account_df():
    s3_input_path = f"{BASE_MERGED_ACCOUNT_PATH}"
    country_mapping_df = spark.read.option("recursiveFileLookup", "true") \
            .option("header", True) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
        .format("csv").load(s3_input_path)
    country_mapping_df.printSchema()
    country_mapping_df.createOrReplaceTempView("merged_account_mapping")

def merged_contact_df():
    s3_input_path = f"{BASE_MERGED_CONTACT_PATH}"
    country_mapping_df = spark.read.option("recursiveFileLookup", "true") \
            .option("header", True) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
        .format("csv").load(s3_input_path)
    country_mapping_df.printSchema()
    country_mapping_df.createOrReplaceTempView("merged_contact_mapping")    
def create_user_mapping_df():
    s3_input_path = f"{BASE_USER_MAPPING_PATH}"
    df = spark.read.option("recursiveFileLookup", "true") \
        .option("header", True).format("csv").load(s3_input_path)
    df.createOrReplaceTempView("preprod_user")

def update_product_account__c_df():
    s3_input_path = f"s3://{bucket_name}/bc-final/update/load_output_updated/success/updateproduct_account__c/"
    df = spark.read.option("recursiveFileLookup", "true") \
            .option("header", True) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .format("csv").load(s3_input_path)
    df.printSchema()
    df.createOrReplaceTempView("update_product_account__c")
       
def remove_unwanted_sequences(df, patterns, replacement):
    for column_name in df.columns:
        if dict(df.dtypes)[column_name] == "string":
            for pattern in patterns:
                df = df.withColumn(column_name, regexp_replace(col(column_name), pattern, replacement))
    return df

def create_orphaned_dataset_df(df, result_df, review_output_path):
    result_id_df = result_df.selectExpr("Migration_Id__c as Id")
    review_df = df.join(result_id_df, on="Id", how="left_anti")
    
    orphan_count = review_df.count()
    print(f"🔍 Orphaned Records Count for {object.lower()}: {orphan_count}")
    #print(f"📁 Orphaned records will be saved to: {review_output_path}")

    review_df.coalesce(1).write.mode("overwrite") \
        .format("csv").option("header", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("multiLine", "true") \
        .save(review_output_path)
    
def backup_and_delete_existing_csv(bucket, source_prefix, backup_prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=source_prefix)
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                filename = key.split('/')[-1]
                backup_key = f"{backup_prefix}{filename}"
                print(f"Backing up {key} to {backup_key}")
                s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=backup_key)
                print(f"Deleting {key}")
                s3_client.delete_object(Bucket=bucket, Key=key)


def transform_data(object, spark, bucket, partition_year):
    sql_s3_key = get_sql_s3_key(object)
    if not sql_s3_key:
        raise ValueError(f"Invalid object name: {object}")

    query = load_sql_from_s3(sql_s3_key)

    create_static_type_df()
    create_country_state_mapping_df()
    create_user_mapping_df()

 
    merged_contact_df()
    merged_account_df()
    update_product_account__c_df()
  


    object_name ='account'
    object_name1='contact'
    object_name2='opportunity'
    object_name3='lead'
    create_success_df(object_name)
    create_success_df(object_name1)
    #casecomment
    # object_name4='case'
    # create_master_df(object_name4)
    #create_success_df(object_name)
    #create_success_df(object_name1)
    #for note object
    create_master_df(object_name2)
    create_master_df(object_name3)
    create_master_df(object_name)
    create_master_df(object_name1)

    for obj in [
        "account","contact","opportunity","lead","product2","opportunitylineitem","partnercommissionsummary__c","store__c","storecontactrole__c","salesreferral__c","zuora__product__c","opportunitystoreassociation__c","task","case","ironclad__ironclad_contract__c","event","campaign","certification__c","invoicetransaction__c","deal_registration__c","project__c","projectrole__c","getfeedback_aut__survey__c","getfeedback_aut__response__c","projectproduct__c","campaignmember","ironclad__ironclad_workflow__c","ironclad__ironclad_signature__c","billingaccount__c","emailmessage","partnerapp__c","partnercommissionpayment__c","partnerpayment__c","partnercommissioninvoicetransaction__c","product_account__c","subscription__c"
    ]:
        
        create_master_df(obj)

   

    df = spark.read.option("recursiveFileLookup", "true") \
        .option("header", True) \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("multiLine", "true") \
        .format("csv").load(s3_input_path)

    print(df.count())

    df.createOrReplaceTempView(object)

    print(f"Executing SQL query for {object}...")
    result_df = spark.sql(query)
    print("Count Of Total Transformed Result:")
    print(result_df.count())

    patterns = ["\n", "\r", "\r\n"]
    replacement = " "
    result_df = remove_unwanted_sequences(result_df, patterns, replacement)

    # orphaned data 
    if object.lower() in ["account","contact","opportunity","lead","product2","opportunitylineitem","partnercommissionsummary__c","accountteammember","store__c","storecontactrole__c","salesreferral__c","opportunitystoreassociation__c","task","case","ironclad__ironclad_contract__c","event","campaign","certification__c","invoicetransaction__c","deal_registration__c","project__c","projectrole__c","getfeedback_aut__survey__c","getfeedback_aut__response__c","projectproduct__c","campaignmember","ironclad__ironclad_workflow__c","ironclad__ironclad_signature__c","billingaccount__c","emailmessage","partnerapp__c","partnercommissionpayment__c","partnerpayment__c","partnercommissioninvoicetransaction__c","product_account__c","subscription__c"]:
     
        
        #incremental create
        orphaned_backup_prefix = f"bc-final/orphaned_backup/{object.lower()}/{final_path}"
        orphaned_source_prefix = f"bc-final/orphaned/{object.lower()}/{final_path}"
        backup_and_delete_existing_csv(bucket, orphaned_source_prefix, orphaned_backup_prefix)

        create_orphaned_dataset_df(df, result_df, orphaned_output_path)

 
    transformed_backup_prefix = f"bc-final/transformed_backup/{object.lower()}/{final_path}"
    transformed_source_prefix = f"bc-final/transformed/{object.lower()}/{final_path}"
    backup_and_delete_existing_csv(bucket, transformed_source_prefix, transformed_backup_prefix)

    print(f"Saving transformed data for {object} to: {output_path}")
    result_df.coalesce(1).write.mode("overwrite") \
        .format("csv").option("header", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("quoteAll", "false") \
        .option("nullValue", "") \
        .option("multiLine", "true") \
        .save(output_path)


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['s3_bucket'])
    bucket_name = args['s3_bucket']
 

    try:
        optional_args = getResolvedOptions(sys.argv, ['object'])
        object = optional_args['object']
    except:

        object = 'contact'
        
    partition_year = None
    if 'partition_year' in sys.argv:
        optional_args = getResolvedOptions(sys.argv, ['partition_year'])
        partition_year = optional_args['partition_year']

    BASE_INPUT_PATH = f"s3://{bucket_name}/bc-final/raw/"
    BASE_OUTPUT_PATH = f"s3://{bucket_name}/bc-final/transformed"
    BASE_ORPHANED_OUTPUT_PATH = f"s3://{bucket_name}/bc-final/orphaned"
    BASE_COUNTRY_STATE_MAPPING_PATH = f"s3://{bucket_name}/current-to-tfm/app/mapping/country"
    BASE_RECORD_TYPE_PATH = f"s3://{bucket_name}/current-to-tfm/raw/recordtype/"
    BASE_USER_MAPPING_PATH = f"s3://{bucket_name}/bc-final/app/preprod_user_mapping/"
    # BASE_SUCCESS_PATH=f"s3://{bucket_name}/bc-final/load_output/tfm-preprod/success/"

    BASE_MERGED_ACCOUNT_PATH = f"s3://{bucket_name}/bc-final/app/merged/account"
    BASE_MERGED_CONTACT_PATH = f"s3://{bucket_name}/bc-final/app/merged/contact"
 

    final_path = f"year={partition_year}/" if partition_year else ""
    s3_input_path = f"{BASE_INPUT_PATH}/{object.lower()}/{final_path}"

    output_path = f"{BASE_OUTPUT_PATH}/{object.lower()}/{final_path}"
    orphaned_output_path = f"{BASE_ORPHANED_OUTPUT_PATH}/{object.lower()}/{final_path}"

    spark = SparkSession.builder \
        .appName("AWS Glue Data Transformation for SFDC Current to TFM") \
        .getOrCreate()

    print(f"Processing for object: {object}")
    transform_data(object, spark, bucket=bucket_name, partition_year=partition_year)

    spark.stop()
