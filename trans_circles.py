"""
Bash Script:
time spark-submit trans_circles.py
Results:
- TASK1: Increase in percentage value per NFT /user/s2190443/project/percentage_increase
- TASK2: Total value of a wash per NFT: /user/s2190443/project/total_wash_value
- TASK3: Number of washes per user: /user/s2190443/project/total_wash_counts_per_user
"""

import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

sc = SparkContext(appName="TransCircles")
sc.setLogLevel("Error")
spark = SparkSession.builder.getOrCreate()
sqlContext = SQLContext(sc)

s_number_to_save = "s2190443"

# Define schema because some transaction values are more than 1e17
c_schema = 'event_id STRING, transaction_hash STRING, block_number INTEGER, \
        nft_address STRING, token_id STRING, from_address STRING, to_address STRING, \
        transaction_value DECIMAL(32, 0), timestamp INTEGER'

transfers_data = spark.read.json('/user/s2597403/project/transfers/*.json')


def save_csv(frame, path_name):
    frame.coalesce(1).write.option("header", True).csv("/user/"+s_number_to_save+"/project/" + path_name, mode="overwrite")


def quike_cycles(df):
    window1 = Window.partitionBy('row_sha2')
    df1 = df \
        .select('nft_address', 'token_id', 'from_address', 'to_address', F.col('transaction_value').cast(LongType()),
                F.col('timestamp').cast(IntegerType())) \
        .withColumn('row_sha2', F.sha2(F.concat_ws('||', 'nft_address', 'token_id', 'from_address'), 256)) \
        .withColumn('distinct', F.count('row_sha2').over(window1))

    # df1.show()

    df1 = df1.filter(F.col('distinct') > 1)

    # df1.show()

    window2 = Window.partitionBy('nft_address', 'token_id').orderBy('timestamp')
    df2 = df1.withColumn('row_number', F.row_number().over(window2))

    # df2.show()

    df3 = df2 \
        .groupby('nft_address', 'token_id', 'from_address') \
        .agg(
        F.collect_list('row_number').alias('row_number_list'),
        F.count('row_number').alias('sender_count'),
        F.collect_list('transaction_value').alias('transaction_value'),
        F.last('transaction_value').alias('last_transaction_value'),
        F.first('transaction_value').alias('first_transaction_value')
    )

    # df3.show()

    df3 = df3.withColumn('value_change', df3['last_transaction_value'] - df3['first_transaction_value'])
    # df3.show()

    avg_value_increase = df3.select(df3["value_change"]).filter(F.col("value_change") > 0).groupBy().avg().collect()[0][
        0]
    print avg_value_increase

    percentage_increase = df3.filter(df3["value_change"] > 0).filter(F.col("first_transaction_value") > 0).withColumn(
        "percentage_increase",
        F.col("value_change") / F.col("first_transaction_value")).select("percentage_increase")
    percentage_increase.show()
    save_csv(percentage_increase, "percentage_increase")

    total_wash_value = df3.groupBy("nft_address", "token_id").agg(
        F.sum("value_change")
    )
    total_wash_value.show()
    save_csv(total_wash_value, "total_wash_value")

    total_wash_counts_per_user = df3.groupBy("from_address").agg(
        F.count("token_id")
    )
    total_wash_counts_per_user.show()
    save_csv(total_wash_counts_per_user, "total_wash_counts_per_user")


quike_cycles(transfers_data)
