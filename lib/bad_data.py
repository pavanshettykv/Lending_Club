from pyspark.sql.functions import *
from lib import configreader

conf = configreader.get_app_config("LOCAL")
bad_file_path = conf["bad.file.path"]
cleaned_file_path = conf["cleaned.file.path"]
cleanednew_file_path = conf["cleanednew.file.path"]

def write_bad_customers_data(spark):
    #reading cleaned data
    customers_df = spark.read.parquet(f"{cleaned_file_path}/customers_parquet")
    loan_defaulters_df = spark.read.parquet(f"{cleaned_file_path}/loans_defaulters_delinq_parquet")
    loan_defaulters_detail_records_df = spark.read.parquet(f"{cleaned_file_path}/loans_defaulters_pubrec_detail_parquet")

    #checking bad data in 3 datasets
    bad_cust_df = customers_df.groupby("member_id").count().filter("count > 1").select("member_id")
    bad_loan_defaulters_df = loan_defaulters_df.groupby("member_id").count().filter("count>1").select("member_id")
    bad_loan_defaulters_detail_df = loan_defaulters_detail_records_df.groupby("member_id").count().filter("count>1").select("member_id")

    #consolidating bad data from 3 datasets
    bad_customers_data = bad_cust_df.union(bad_loan_defaulters_df).union(bad_loan_defaulters_detail_df).distinct()

    bad_customers_data.repartition(1)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{bad_file_path}/bad_customers_data")

def remove_bad_data(spark):
    bad_customer_df = spark.read.parquet(f"{bad_file_path}/bad_customers_data")
    customers_df = spark.read.parquet(f"{cleaned_file_path}/customers_parquet")
    loan_defaulters_df = spark.read.parquet(f"{cleaned_file_path}/loans_defaulters_delinq_parquet")
    loan_defaulters_detail_records_df = spark.read.parquet(f"{cleaned_file_path}/loans_defaulters_pubrec_detail_parquet")

    customers_cleaned_df = customers_df.join(bad_customer_df,customers_df.member_id == bad_customer_df.member_id,"anti")
    loan_defaulters_cleaned_df =loan_defaulters_df.join(bad_customer_df,loan_defaulters_df.member_id == bad_customer_df.member_id,"anti")
    loan_defaulters_detail_records_cleaned_df =loan_defaulters_detail_records_df.join(bad_customer_df,loan_defaulters_detail_records_df.member_id == bad_customer_df.member_id,"anti")

    customers_cleaned_df.repartition(8)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{cleanednew_file_path}/customers_parquet")
    

    loan_defaulters_cleaned_df.repartition(8)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{cleanednew_file_path}/loans_defaulters_delinq_parquet")
    
    loan_defaulters_detail_records_cleaned_df.repartition(8)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{cleanednew_file_path}/loans_defaulters_pubrec_detail_parquet")