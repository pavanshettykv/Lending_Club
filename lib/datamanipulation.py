from pyspark.sql.functions import *
from lib import configreader

conf = configreader.get_app_config("LOCAL")
cleaned_file_path = conf["cleaned.file.path"]

def write_cleaned_customers_data(customers_df):
    cleaned_customers_df = customers_df.withColumnRenamed("annual_inc","annual_income")\
                            .withColumnRenamed("addr_state","address_state")\
                            .withColumnRenamed("zip_code","address_zipcode")\
                            .withColumnRenamed("country","address_country")\
                            .withColumnRenamed("tot_hi_cred_lim","total_high_credit_limit")\
                            .withColumnRenamed("annual_inc_joint","joint_annual_income")\
                            .withColumn("ingestion_date",current_timestamp())\
                            .distinct()\
                            .filter(col("annual_income").isNotNull())\
                            .withColumn("emp_length",regexp_replace(col("emp_length"),"\D","").cast("int"))
    # filled missing emp_length with average emp_length
    emp_len_avg = cleaned_customers_df.agg(floor(avg(col("emp_length")))).collect()[0][0] 
    cust_df = cleaned_customers_df.na.fill(emp_len_avg,["emp_length"])
    # address_state should be of length 2 else 'NA'
    cleaned_cust_df = cust_df.withColumn("address_state",when(length(col("address_state"))>2,'NA').otherwise(col("address_state")))
    # cleaned_cust_df.show(5)
    cleaned_cust_df\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{cleaned_file_path}/customers_parquet1")

