from lib import configreader
from pyspark.sql.functions  import *
from pyspark.sql.types import *

def get_customer_schema():
    customer_schema = 'member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string, zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, verification_status_joint string'
    return customer_schema

#read customers data
def read_customers_data(spark,env):
    app_conf = configreader.get_app_config(env)
    customer_file_path = app_conf["customers.file.path"]
    return spark.read\
        .format("csv")\
        .option("header","true")\
        .schema(get_customer_schema())\
        .load(customer_file_path)

def get_loans_data_schema():
    loans_data_schema = StructType([
        StructField("loan_id",IntegerType()),
        StructField("member_id",StringType()),
        StructField("loan_amnt",FloatType()),
        StructField("funded_amnt",FloatType()),
        StructField("term",StringType()),
        StructField("int_rate",FloatType()),
        StructField("installment",FloatType()),
        StructField("issue_d",StringType()),
        StructField("loan_status",StringType()),
        StructField("purpose",StringType()),
        StructField("title",StringType()),
    ])
    return loans_data_schema

#read loans data
def read_loans_data(spark,env):
    app_conf = configreader.get_app_config(env)
    loans_file_path = app_conf["loans.file.path"]
    return spark.read\
        .format("csv")\
        .option("header","true")\
        .schema(get_loans_data_schema())\
        .load(loans_file_path)
    
def get_loans_defaulters_schema():
    loans_defaulters_schema = "member_id string,delinq_2yrs float,delinq_amnt float,pub_rec float,pub_rec_bankruptcies float,inq_last_6mths float,total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"
    return loans_defaulters_schema

#read loans defaulters data
def read_loans_defaulters_data(spark,env):
    app_conf = configreader.get_app_config(env)
    loans_defaulters_file_path = app_conf["loans_defaulters.file.path"]
    return spark.read\
        .format("csv")\
        .option("header","true")\
        .schema(get_loans_defaulters_schema())\
        .load(loans_defaulters_file_path)

def get_loans_repayment_schema():
    loans_repayment_schema = "loan_id integer,total_rec_prncp float,total_rec_int float,total_rec_late_fee float,total_pymnt float,last_pymnt_amnt float,last_pymnt_d string,next_pymnt_d string"
    return loans_repayment_schema

#read loans repayment data
def read_loans_repayment_data(spark,env):
    app_conf = configreader.get_app_config(env)
    loans_repayment_file_path = app_conf["loans_repayments.file.path"]
    return spark.read\
        .format("csv")\
        .option("header","true")\
        .schema(get_loans_repayment_schema())\
        .load(loans_repayment_file_path)
    