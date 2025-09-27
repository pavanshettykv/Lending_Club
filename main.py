from lib import Utils,datareader,datamanipulation
from pyspark.sql import SparkSession

if __name__ == "__main__":
    env = "LOCAL"
    spark = Utils.get_spark_session(env)
    print("Spark Session Created")
    customers_df = datareader.read_customers_data(spark,env)
    loans_df = datareader.read_loans_data(spark,env)
    loans_defaulters_df = datareader.read_loans_defaulters_data(spark,env)
    loans_repayment_df = datareader.read_loans_repayment_data(spark,env)

    cleaned_cust_df = datamanipulation.get_cleaned_customers_data(customers_df)
    cleaned_cust_df.show(5)






