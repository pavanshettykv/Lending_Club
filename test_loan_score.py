import pytest
from lib import Utils,datareader,datamanipulation,bad_data,loan_score

@pytest.fixture
def spark():
    env = "LOCAL"
    spark = Utils.get_spark_session(env)
    return spark

@pytest.mark.latest()
def test_no_of_bad_customers_data(spark):
    # env = "LOCAL"
    # spark = Utils.get_spark_session(env)
    # bad_data.write_bad_customers_data(spark)

    bad_customers_df = spark.read.parquet(f"{bad_data.bad_file_path}/bad_customers_data")
    bad_count = bad_customers_df.count()
    assert bad_count >0, "No bad customers data found"
    print(f"Bad customers data count: {bad_count}")

@pytest.mark.skip
def test_calculate_loan_score():
    env = "LOCAL"
    spark = Utils.get_spark_session(env)

    # Read cleaned data
    customers_df = datareader.read_customers_data(spark,env)
    loans_df = datareader.read_loans_data(spark,env)
    loans_defaulters_df = datareader.read_loans_defaulters_data(spark,env)
    loans_repayment_df = datareader.read_loans_repayment_data(spark,env)

    # Ensure dataframes are not empty
    assert customers_df.count() > 0, "Customers DataFrame is empty"
    assert loans_df.count() > 0, "Loans DataFrame is empty"
    assert loans_defaulters_df.count() > 0, "Loans Defaulters DataFrame is empty"
    assert loans_repayment_df.count() > 0, "Loans Repayment DataFrame is empty"

    # Calculate loan score
    credit_score_df = loan_score.calculate_loan_score(spark)

    # Validate the result
    assert credit_score_df is not None, "Credit Score DataFrame is None"
    assert credit_score_df.count() > 0, "Credit Score DataFrame is empty"

#     # Check for expected columns in the result
#     expected_columns = {"member_id", "credit_score"}
#     actual_columns = set(credit_score_df.columns)
#     assert expected_columns.issubset(actual_columns), f"Missing expected columns: {expected_columns - actual_columns}"