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

    # datamanipulation.write_cleaned_customers_data(customers_df)
    # datamanipulation.write_cleaned_loans_data(loans_df)
    # datamanipulation.write_cleaned_loans_repayment_data(loans_repayment_df)
    datamanipulation.write_cleaned_loans_defaulters_data(loans_defaulters_df)


    spark.stop()




