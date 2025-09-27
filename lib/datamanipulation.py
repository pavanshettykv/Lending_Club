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
        .save(f"{cleaned_file_path}/customers_parquet")

def write_cleaned_loans_data(loans_df):
    loans_df = loans_df.withColumnRenamed("loan_amnt","loan_amount")\
        .withColumnRenamed("funded_amnt","funded_amount")\
        .withColumnRenamed("term","loan_term_months")\
        .withColumnRenamed("int_rate","interest_rate")\
        .withColumnRenamed("installment","monthly_installment")\
        .withColumnRenamed("issue_d","issue_date")\
        .withColumnRenamed("purpose","loan_purpose")\
        .withColumnRenamed("title","loan_title")\
        .withColumn("ingestion_date",current_timestamp()) 
    
    columns_to_check = ["loan_amount", "funded_amount", "loan_term_months","interest_rate","monthly_installment","issue_date","loan_status","loan_purpose"]
    loan_purpose_lookup = ["debt_consolidation", "credit_card","home_improvement", "other", "major_purchase", "medical", "small_business","car", "vacation", "moving", "house", "wedding", "renewable_energy","educational"]
    
    loans_df = loans_df.dropna(subset = columns_to_check)\
                    .withColumn("loan_purpose",when(col("loan_purpose").isin(loan_purpose_lookup),col("loan_purpose")).otherwise("other"))\
                    .withColumn("loan_term_year",regexp_replace(col("loan_term_months"),"\D","").cast("int")/12)\
                    
    loans_df.repartition(8)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{cleaned_file_path}/loans_parquet")
    

    def write_cleaned_loans_defaulters_data(loans_defaulters_df):
        loans_defaulters_df = loans_defaulters_df.withColumn("ingestion_date",current_timestamp())

    