from lib import Utils,datareader,datamanipulation,create_tables,bad_data
from pyspark.sql import SparkSession

if __name__ == "__main__":
    env = "LOCAL"
    spark = Utils.get_spark_session(env)
    # Print SparkSession details
    print("SparkSession:", spark)
    print("Spark Version:", spark.version)
    print("Application Name:", spark.sparkContext.appName)
    print("Master:", spark.sparkContext.master)
    print("Spark UI:", spark.sparkContext.uiWebUrl)
    print("warehouse dir:",spark.conf.get("spark.sql.warehouse.dir"))

    customers_df = datareader.read_customers_data(spark,env)
    loans_df = datareader.read_loans_data(spark,env)
    loans_defaulters_df = datareader.read_loans_defaulters_data(spark,env)
    loans_repayment_df = datareader.read_loans_repayment_data(spark,env)

    # datamanipulation.write_cleaned_customers_data(customers_df)
    # datamanipulation.write_cleaned_loans_data(loans_df)
    # datamanipulation.write_cleaned_loans_repayment_data(loans_repayment_df)
    # datamanipulation.write_cleaned_loans_defaulters_data(loans_defaulters_df)
    # create_tables.get_cleaned_customers_data(spark)
    # create_tables.get_cleaned_loans_data(spark)
    
    # bad_data.write_bad_customers_data(spark)
    bad_data.remove_bad_data(spark)


