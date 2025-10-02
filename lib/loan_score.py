from pyspark.sql.functions import *
from lib import configreader

conf = configreader.get_app_config("LOCAL")
bad_file_path = conf["bad.file.path"]
cleaned_file_path = conf["cleaned.file.path"]
cleanednew_file_path = conf["cleanednew.file.path"]
processed_file_path = conf["processed.file.path"]

def calculate_load_score(spark):
    bad_data_df = spark.read.parquet(f"{bad_file_path}/bad_customers_data")
    customers_df = spark.read.parquet(f"{cleanednew_file_path}/customers_parquet")
    loan_df = spark.read.parquet(f"{cleaned_file_path}/loans_parquet")
    loan_repayment_df = spark.read.parquet(f"{cleaned_file_path}/loans_repayment_parquet")
    loan_defaulters_df = spark.read.parquet(f"{cleanednew_file_path}/loans_defaulters_delinq_parquet")
    loan_defaulters_detail_records_df = spark.read.parquet(f"{cleanednew_file_path}/loans_defaulters_pubrec_detail_parquet")

    # setting score thresholds provided by business
    spark.conf.set("spark.sql.excellent_score",800)
    spark.conf.set("spark.sql.very_good_score",600)
    spark.conf.set("spark.sql.good_score",500)
    spark.conf.set("spark.sql.bad_score",250)
    spark.conf.set("spark.sql.very_bad_score",100)
    spark.conf.set("spark.sql.unacceptable_score",0)


    #criteria 1: payment history - 20%

    payment_history_df = loan_df.join(loan_repayment_df,"loan_id","inner")\
        .join(bad_data_df,loan_df["member_id"] == bad_data_df["member_id"],"anti")\
        .withColumn("payment_pts",\
                    expr(
                        """
                        CASE
                            WHEN last_payment_amount < (monthly_installment * 0.5) THEN ${spark.sql.very_bad_score}
                            WHEN last_payment_amount > (monthly_installment * 0.5) and last_payment_amount < (monthly_installment * 0.5) THEN ${spark.sql.bad_score}
                            WHEN last_payment_amount = (monthly_installment * 0.5) THEN ${spark.sql.good_score}
                            WHEN last_payment_amount > (monthly_installment * 0.5) and last_payment_amount < (monthly_installment * 1.5) THEN ${spark.sql.very_good_score}
                            WHEN last_payment_amount > (monthly_installment * 1.5) THEN ${spark.sql.excellent_score}
                        END
                         """
                    )
        )\
        .withColumn("tp_pts",\
                    expr(
                        """
                        CASE
                            WHEN (total_payment_received >= funded_amount * 0.5) THEN ${spark.sql.very_good_score}
                            WHEN (total_payment_received <= funded_amount * 0.5) THEN ${spark.sql.good_score}
                            WHEN  (total_payment_received  = 0 or total_payment_received  is null) THEN ${spark.sql.unacceptable_score}
                        END
                        """       
                    )
        )\
        .select("member_id","loan_id","last_payment_amount","monthly_installment","payment_pts","tp_pts")

    # criteria 2 : loan default history - 45%
    ph_ldh_df = loan_defaulters_df.join(loan_defaulters_detail_records_df,"member_id","inner")\
        .join(payment_history_df,"member_id","inner")\
        .join(bad_data_df,"member_id", "anti")\
        .withColumn("delinq_pts",\
                    expr(
                        """
                        CASE
                            WHEN delinq_2yrs = 0 THEN ${spark.sql.excellent_score}
                            WHEN delinq_2yrs BETWEEN 1 AND 2 THEN ${spark.sql.bad_score}
                            WHEN delinq_2yrs BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_score}
                            WHEN delinq_2yrs > 0 or delinq_2yrs is NULL THEN ${spark.sql.unacceptable_score}
                        END
                        """
                    )
        )\
        .withColumn("pubrec_pts",\
                    expr(
                        """
                        CASE
                            WHEN pub_rec = 0 THEN ${spark.sql.excellent_score}
                            WHEN pub_rec BETWEEN 1 AND 2 THEN ${spark.sql.bad_score}
                            WHEN pub_rec BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_score}
                            WHEN pub_rec > 5 or pub_rec is NULL THEN ${spark.sql.unacceptable_score}
                        END
                        """
                    )
        )\
        .withColumn("pubrec_bk_pts",\
                    expr(
                        """
                        CASE
                            WHEN pub_rec_bankruptcies = 0 THEN ${spark.sql.excellent_score}
                            WHEN pub_rec_bankruptcies BETWEEN 1 AND 2 THEN ${spark.sql.bad_score}
                            WHEN pub_rec_bankruptcies BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_score}
                            WHEN pub_rec_bankruptcies > 5 or pub_rec_bankruptcies is NULL THEN ${spark.sql.unacceptable_score}
                        END
                        """
                    )
        )\
        .withColumn("inq_last_6mths_pts",\
                    expr(
                        """
                        CASE
                            WHEN inq_last_6mths = 0 THEN ${spark.sql.excellent_score}
                            WHEN inq_last_6mths BETWEEN 1 AND 2 THEN ${spark.sql.bad_score}
                            WHEN inq_last_6mths BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_score}
                            WHEN inq_last_6mths > 5 or inq_last_6mths is NULL THEN ${spark.sql.unacceptable_score}
                        END
                        """
                    )
        )\
        .select("member_id","loan_id","last_payment_amount","monthly_installment","payment_pts","tp_pts","delinq_pts","pubrec_pts","pubrec_bk_pts","inq_last_6mths_pts")
    
    # criteria 3: financial condition - 35%

    ph_ldh_fh_df = customers_df.join(loan_df,"member_id","inner")\
        .join(ph_ldh_df,"member_id","inner")\
        .join(bad_data_df,"member_id","anti")\
        .withColumn("home_pts",\
                    expr(
                        """
                        CASE 
                            WHEN lower(home_ownership) like '%own' THEN ${spark.sql.excellent_score}
                            WHEN lower(home_ownership) like '%rent' THEN ${spark.sql.good_score}
                            WHEN lower(home_ownership) like '%mortgage' THEN ${spark.sql.bad_score}
                            WHEN lower(home_ownership) like '%any' or lower(home_ownership) is NULL THEN ${spark.sql.very_bad_score}
                        END
                        """
                    )
                    )\
        .withColumn("loan_status_pts",\
                    expr(
                        """
                        CASE
                            WHEN lower(loan_status) like '%fully paid%' THEN ${spark.sql.excellent_score}
                            WHEN lower(loan_status) like '%current%' THEN ${spark.sql.very_good_score}
                            WHEN lower(loan_status) like '%in grace period%' THEN ${spark.sql.good_score}
                            WHEN lower(loan_status) like '%late (16-30 days)%' OR lower(loan_status) LIKE '%late (31-120 days)%' THEN ${spark.sql.bad_score}
                            WHEN lower(loan_status) like '%charged off%' THEN ${spark.sql.unacceptable_score}
                        END
                        """
                    )
            )\
        .withColumn("fund_pts",\
                    expr(
                        """
                        CASE 
                            WHEN funded_amount <= total_high_credit_limit * 0.1 THEN ${spark.sql.excellent_score}
                            WHEN funded_amount > total_high_credit_limit * 0.1 AND funded_amount <= total_high_credit_limit * 0.2 THEN ${spark.sql.very_good_score}
                            WHEN funded_amount > total_high_credit_limit * 0.2 AND funded_amount <= total_high_credit_limit * 0.3 THEN ${spark.sql.good_score}
                            WHEN funded_amount > total_high_credit_limit * 0.3 AND funded_amount <= total_high_credit_limit * 0.5 THEN ${spark.sql.bad_score}
                            WHEN funded_amount > total_high_credit_limit * 0.5 AND funded_amount <= total_high_credit_limit * 0.7 THEN ${spark.sql.very_bad_score}
                            WHEN funded_amount > total_high_credit_limit * 0.7 THEN ${spark.sql.unacceptable_score}
                        END
                        """
                    )
            )\
        .withColumn("grade_pts",\
                    expr(
                        """
                        CASE 
                            WHEN grade = 'A' AND sub_grade = 'A1' THEN ${spark.sql.excellent_score} * 0.95
                            WHEN grade = 'A' AND sub_grade = 'A2' THEN ${spark.sql.excellent_score} * 0.90
                            WHEN grade = 'A' AND sub_grade = 'A3' THEN ${spark.sql.excellent_score} * 0.85
                            WHEN grade = 'A' AND sub_grade = 'A4' THEN ${spark.sql.excellent_score} * 0.80
                            WHEN grade = 'B' AND sub_grade = 'B1' THEN ${spark.sql.very_good_score} * 0.95
                            WHEN grade = 'B' AND sub_grade = 'B2' THEN ${spark.sql.very_good_score} * 0.90
                            WHEN grade = 'B' AND sub_grade = 'B3' THEN ${spark.sql.very_good_score} * 0.85
                            WHEN grade = 'B' AND sub_grade = 'B4' THEN ${spark.sql.very_good_score} * 0.80
                            WHEN grade = 'C' AND sub_grade = 'C1' THEN ${spark.sql.good_score} * 0.95
                            WHEN grade = 'C' AND sub_grade = 'C2' THEN ${spark.sql.good_score} * 0.90
                            WHEN grade = 'C' AND sub_grade = 'C3' THEN ${spark.sql.good_score} * 0.85
                            WHEN grade = 'C' AND sub_grade = 'C4' THEN ${spark.sql.good_score} * 0.80
                            WHEN grade = 'D' AND sub_grade = 'D1' THEN ${spark.sql.bad_score} * 0.95
                            WHEN grade = 'D' AND sub_grade = 'D2' THEN ${spark.sql.bad_score} * 0.90
                            WHEN grade = 'D' AND sub_grade = 'D3' THEN ${spark.sql.bad_score} * 0.85
                            WHEN grade = 'D' AND sub_grade = 'D4' THEN ${spark.sql.bad_score} * 0.80
                            WHEN grade = 'E' AND sub_grade = 'E1' THEN ${spark.sql.very_bad_score} * 0.95
                            WHEN grade = 'E' AND sub_grade = 'E2' THEN ${spark.sql.very_bad_score} * 0.90
                            WHEN grade = 'E' AND sub_grade = 'E3' THEN ${spark.sql.very_bad_score} * 0.85
                            WHEN grade = 'E' AND sub_grade = 'E4' THEN ${spark.sql.very_bad_score} * 0.80
                            WHEN grade in ('F','G') THEN ${spark.sql.unacceptable_score}
                            ELSE ${spark.sql.unacceptable_score}
                            END
                    """
                    )
            )\
        .select("member_id","payment_pts","tp_pts","delinq_pts","pubrec_pts","pubrec_bk_pts","inq_last_6mths_pts","home_pts","loan_status_pts","fund_pts","grade_pts")



    intermediate_credit_score = ph_ldh_fh_df\
            .withColumn("payment_score", expr("(payment_pts + tp_pts) * 0.2"))\
            .withColumn("default_score",expr("(delinq_pts + pubrec_pts + pubrec_bk_pts + inq_last_6mths_pts) * 0.45"))\
            .withColumn("financial_score",expr("(home_pts + loan_status_pts + fund_pts + grade_pts) * 0.35"))
    
    credit_score = intermediate_credit_score\
                    .withColumn("credit_score",expr("payment_score + default_score + financial_score/3").cast("int"))\
                    .select("member_id","credit_score")
    
    intermediate_credit_score.repartition(8)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{processed_file_path}/intermediate_credit_score_parquet")
    
    intermediate_credit_score.repartition(1)\
        .write\
        .format("csv")\
        .option("header","true")\
        .mode("overwrite")\
        .save(f"{processed_file_path}/intermediate_credit_score_csv")
    
    credit_score.repartition(8)\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .save(f"{processed_file_path}/credit_score_parquet")
    credit_score.repartition(1)\
        .write\
        .format("csv")\
        .option("header","true")\
        .mode("overwrite")\
        .save(f"{processed_file_path}/credit_score_csv")
    
    return credit_score




    