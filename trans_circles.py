import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import udf
   
sc = SparkContext(appName="TransCircles")
sc.setLogLevel("Error")
sqlContext = SQLContext(sc)
   
# Define schema because some transaction values are more than 1e17
c_schema = 'event_id STRING, transaction_hash STRING, block_number INTEGER, \
        nft_address STRING, token_id STRING, from_address STRING, to_address STRING, \
        transaction_value DECIMAL(32, 0), timestamp INTEGER'
   
df = sqlContext.read \
        .format('jdbc') \
        .options(url='jdbc:sqlite:/home/s2190443/nfts.sqlite') \
        .options(dbtable='transfers') \
        .options(driver='org.sqlite.JDBC') \
        .options(customSchema=c_schema) \
        .load()



def get_cycles(transactions_df):
        window_spec = Window.partitionBy("nft_address", 'token_id').orderBy("timestamp")
        transactions_df = transactions_df.withColumn('transaction_count', F.row_number().over(window_spec))
        # count_table_df = init_count_table.select(init_count_table.nft_address.alias("nft_addressC"),
        #                                         init_count_table.token_id.alias("token_idC"),
        #                                         init_count_table.timestamp.alias("timeC"),
        #                                         init_count_table.to_address.alias("receiverC"),
        #                                         init_count_table.transaction_count)


        df_sender = transactions_df.select("nft_address",
                                           "token_id",
                                           "from_address",
                                           transactions_df.timestamp.alias("time_sender"),
                                           transactions_df.transaction_value.alias("val_sender"),
                                           transactions_df.transaction_count.alias("count_sender"))
        df_receiver = transactions_df.select(transactions_df.nft_address.alias("nft_address2"),
                                             transactions_df.token_id.alias("token_id2"),
                                             "to_address",
                                             transactions_df.timestamp.alias("time_receiver"),
                                             transactions_df.transaction_value.alias("val_receiver"),
                                             transactions_df.transaction_value.alias("count_receiver"))
        cond = [df_sender["nft_address"] == df_receiver["nft_address2"],
                df_sender["token_id"] == df_receiver["token_id2"],
                df_sender["from_address"] == df_receiver["to_address"],
                df_sender["time_sender"] <= df_receiver["time_receiver"]]
        init_result_df = df_sender.join(df_receiver, cond, "inner")
        # result_df = init_result_df.select(init_result_df.nft_address.alias("nft_addressR"),
        #                                   init_result_df.token_id.alias("token_idR"),
        #                                   init_result_df.from_address.alias("user"),
        #                                   init_result_df.time_sender.alias("time_senderR"),
        #                                   init_result_df.time_receiver.alias("time_receiverR"),
        #                                   init_result_df.val_sender.alias("init_value"),
        #                                   init_result_df.val_receiver.alias("end_value"))


        # cond = [result_df["nft_addressR"] == count_table_df["nft_addressC"],
        #         result_df["token_idR"] == count_table_df["token_idC"],
        #         result_df["user"] == count_table_df["receiverC"],
        #         result_df["time_receiverR"] == count_table_df["timeC"]]
        # result_with_count_df = result_df.join(count_table_df, cond, "inner")
        # result_with_count_df = result_with_count_df.select("nft_addressR", "token_idR", "user",
        #                                                    "time_senderR", "time_receiverR", "transaction_count",
        #                                                    "init_value", "end_value")

        # print result_with_count_df.count()
        # result_with_count_df = result_with_count_df.filter(result_with_count_df["transaction_count"] > 1)
        # print result_with_count_df.count()
        # print result_with_count_df.show()
        print init_result_df.count()
        print init_result_df.take(1)

def save_csv(frame, path_name):
    frame.coalesce(1).write.option("header", True).csv("file:///home/s2190443/measurements_data/" + path_name)

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


    # percentage_increase = df3.filter(df3["value_change"] > 0).filter(F.col("first_transaction_value") > 0).withColumn("percentage_increase",
    #                                                                     F.col("value_change") / F.col("first_transaction_value")).select("percentage_increase")
    #percentage_increase.show()

    total_wash_value = df3.groupBy("nft_address", "token_id").agg(
        F.sum("value_change")
    )
    total_wash_value.show()
    save_csv(total_wash_value, "total_wash_value")
    avg_value_increase = df3.select(df3["value_change"]).filter(F.col("value_change") > 0).groupBy().avg().collect()[0][0]
    print avg_value_increase

    total_wash_counts_per_user = df3.groupBy("from_address").agg(
        F.count("token_id")
    )
    total_wash_counts_per_user.show()
    save_csv(total_wash_counts_per_user, "total_wash_counts_per_user")

quike_cycles(df)



# print("set2")
#
# df1 = df \
#         .select('nft_address', 'token_id', 'from_address', 'to_address', F.col('transaction_value').cast(LongType()), 'timestamp') \
#         .groupby('nft_address', 'token_id') \
#         .agg(F.collect_list('from_address').alias('from_address_list'),
#              F.collect_list('to_address').alias('to_address_list'),
#              F.collect_list('transaction_value').alias('transaction_value_list'),
#              F.collect_list('timestamp').alias('timestamp_list')) \
#         .orderBy(F.size('transaction_value_list'), ascending=False)
#
# windowSpec = Window.partitionBy('nft_address', 'token_id').orderBy('timestamp')
# df2 = df.withColumn('row_number', F.row_number().over(windowSpec))
#
# print("set")
# print df2.show()
# print df1.count()
# print("muie")
