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
    
def write_cleaned_loans_repayment_data(loans_repayment_df):
    loans_repayment_df = loans_repayment_df.withColumn("ingestion_date",current_timestamp())\
                            .withColumnRenamed("total_rec_prncp","total_principal_received")\
                            .withColumnRenamed("total_rec_int","total_interest_received")\
                            .withColumnRenamed("total_rec_late_fee","total_late_fee_received")\
                            .withColumnRenamed("total_pymnt","total_payment_received")\
                            .withColumnRenamed("last_pymnt_amnt","last_payment_amount")\
                            .withColumnRenamed("last_pymnt_d","last_payment_date")\
                            .withColumnRenamed("next_pymnt_d","next_payment_date")\
                            .dropna(subset=["total_principal_received","total_interest_received","total_payment_received","last_payment_amount","total_late_fee_received"])
    
    # if total_payment_received is 0 then calculate it from principal, interest and late fee
    loans_repayment_df = loans_repayment_df\
                            .withColumn("total_payment_received",\
                                        when(((col("total_principal_received") != 0.0) & (col("total_payment_received")==0.0)),col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received"))\
                                        .otherwise(col("total_payment_received"))
                                        )\
                            .withColumn("last_payment_date",when(col("last_payment_date") == 0.0,None).otherwise(col("last_payment_date")))\
                            .withColumn("next_payment_date",when(col("next_payment_date") == 0.0,None).otherwise(col("next_payment_date")))

    loans_repayment_df.repartition(8)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{cleaned_file_path}/loans_repayment_parquet")
    

    
def write_cleaned_loans_defaulters_data(loans_defaulters_df):
    loans_defaulters_df = loans_defaulters_df.withColumn("ingestion_date",current_timestamp())
   
    # Delinquency related defaults
    loans_defaulters_delinq = loans_defaulters_df.withColumn("delinq_2yrs",col("delinq_2yrs").cast("int")).fillna(0,subset=["delinq_2yrs"])\
                        .withColumn("mnths_since_last_delinq",col("mths_since_last_delinq").cast("int"))\
                        .filter("mnths_since_last_delinq >0 and delinq_2yrs >0")\
                        .select("member_id","delinq_2yrs","delinq_amnt","mnths_since_last_delinq","ingestion_date")
    
    # Public record related defaults
    loans_defaulters_pub_rec = loans_defaulters_df\
                        .withColumn("pub_rec",col("pub_rec").cast("int")).fillna(0,subset=["pub_rec"])\
                        .withColumn("pub_rec_bankruptcies",col("pub_rec_bankruptcies").cast("int")).fillna(0,subset=["pub_rec_bankruptcies"])\
                        .withColumn("mnths_since_last_record",col("mths_since_last_record").cast("int")).fillna(0,subset=["mnths_since_last_record"])\
                        .withColumn("inq_last_6mths",col("inq_last_6mths").cast("int")).fillna(0,subset=["inq_last_6mths"])\
                        .filter("pub_rec > 0 or pub_rec_bankruptcies >0")

    loans_defaulters_delinq.repartition(8)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{cleaned_file_path}/loans_defaulters_delinq_parquet")
    
    loans_defaulters_pub_rec.select("member_id").repartition(8)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{cleaned_file_path}/loans_defaulters_pubrec_parquet")

    loans_defaulters_pub_rec_detail = loans_defaulters_pub_rec.select("member_id","pub_rec","pub_rec_bankruptcies","inq_last_6mths")

    loans_defaulters_pub_rec_detail.repartition(8)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{cleaned_file_path}/loans_defaulters_pubrec_detail_parquet")









