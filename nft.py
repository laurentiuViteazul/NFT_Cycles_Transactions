"""
Bash Script:
time spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=10 nft.py

Results:
- TASK1: Number of manipulated trades per user: /user/s2597403/project/manipulated_users
- TASK2: Number of manipulated trades per nft: /user/s2597403/project/manipulated_nfts
- TASK3: Percentage of manipulated transactions per nft: /user/s2597403/project/manipulation_percentage_nfts
- TASK4: Percentage of manipulated transactions per user: /user/s2597403/project/manipulation_percentage_users
"""


from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *


# ----------------- Initialization --------------- #
sc = SparkContext(appName="NFT")
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()
sqlContext = SQLContext(sc)


# ----------------- Data Importing --------------- #
df_transfers = spark.read.json('/user/s2597403/project/transfers/*.json')


# ----------------- Preprocessing --------------- #

### Add transaction number to indicate the order of transactions for every nft.
window_by_nft = Window.partitionBy('nft_address', 'token_id').orderBy('timestamp')
df_with_row_number = df_transfers \
     .select('nft_address', 'token_id', 'from_address', 'to_address', F.col('transaction_value').cast(LongType()), F.col('timestamp').cast(IntegerType())) \
     .withColumn('row_number', F.row_number().over(window_by_nft))

### Filter out transactions whose sender (from_address) occurs only 1 time, which means this user sold the same nft only 1 time.
# 1. Calculate sha2 value of the nft_address, token_id, and from_address columns
# 2. Calculate value counts of sha2 values.
# 3. Filter sha2 values that occurs more than 1 time.
window_by_sha2 = Window.partitionBy('row_sha2')
df_filtered_by_distinct = df_with_row_number \
    .withColumn('row_sha2', F.sha2(F.concat_ws('||', 'nft_address', 'token_id', 'from_address'), 256)) \
    .withColumn('distinct', F.count('row_sha2').over(window_by_sha2)) \
    .filter(F.col('distinct') > 1)

### Add columns of necessary values.
# 1. row_number_list: transaction number list of the same sender and the same nft. [2,4,6] means the user sold the nft 3 times.
# 2. sender_count: the count of the same user being a sender of the nft.
# 3. transaction_value_list: transaction value list of the same sender and the same nft.
# 4. last_transaction_value: the last transaction value of the same sender and the same nft.
# 5. first_transaction_value: the first transaction value of the same sender and the same nft.
df_intermediate = df_filtered_by_distinct \
     .groupby('nft_address', 'token_id', 'from_address') \
     .agg(
          F.collect_list('row_number').alias('row_number_list'),
          F.count('row_number').alias('sender_count'),
          F.collect_list('transaction_value').alias('transaction_value_list'),
          F.last('transaction_value').alias('last_transaction_value'),
          F.first('transaction_value').alias('first_transaction_value')
     )

### Filter transaction cycles that a user ocurrs as a sender within 3 cycles.
@F.udf(returnType=BooleanType())
def my_filter(col):
    return min([col[i+1] - col[i] for i in range(len(col) - 1)]) <= 3

df_intermediate = df_intermediate \
    .withColumn('value_change', df_intermediate['last_transaction_value'] - df_intermediate['first_transaction_value']) \
    .filter(my_filter('row_number_list'))


# ----------------- Tasks --------------- #

### TASK1: Number of manipulated trades per user ###

df_manipulated_users = df_intermediate.select('from_address') \
    .groupBy('from_address') \
    .agg(F.count('from_address').alias('counts')) \
    .orderBy(F.desc('counts'))

df_manipulated_users.coalesce(1).write.csv('/user/s2597403/project/manipulated_users', mode="overwrite")

manipulated_user_count = df_manipulated_users.count()
all_user_count = df_with_row_number.select('from_address').distinct().count()
print "manipulated_user_count : %d, all_user_count: %d" % (manipulated_user_count, all_user_count)

### TASK2: Number of manipulated trades per nft ###

# nft_sha2: unique identifier for nfts
df_manipulated_nfts = df_intermediate \
    .select('nft_address', 'token_id') \
    .withColumn('nft_sha2', F.sha2(F.concat_ws('||', 'nft_address', 'token_id'), 256)) \
    .groupBy('nft_sha2') \
    .agg(F.count('nft_sha2').alias('counts')) \
    .orderBy(F.desc('counts'))

df_manipulated_nfts.coalesce(1).write.csv('/user/s2597403/project/manipulated_nfts', mode="overwrite")

manipulated_nft_count = df_manipulated_nfts.count()
all_nft_count = df_with_row_number.select('nft_address', 'token_id').distinct().count()
print "manipulated_nft_count : %d, all_nft_count: %d" % (manipulated_nft_count, all_nft_count)

### TASK3: Percentage of manipulated transactions per nft ###

# nft_sha2: unique identifier for nfts
# df_manipulated_trades_per_nft: counts of manipulated transactions per nft
df_manipulated_trades_per_nft = df_intermediate \
    .select('nft_address', 'token_id') \
    .withColumn('nft_sha2', F.sha2(F.concat_ws('||', 'nft_address', 'token_id'), 256)) \
    .groupBy('nft_sha2') \
    .agg(F.count('nft_sha2').alias('manipulation_counts')) \
    .orderBy(F.desc('manipulation_counts'))

# df_all_trades_per_nft: counts of all transactions per nft
df_all_trades_per_nft = df_transfers \
    .select('nft_address', 'token_id') \
    .withColumn('nft_sha2', F.sha2(F.concat_ws('||', 'nft_address', 'token_id'), 256)) \
    .groupBy('nft_sha2') \
    .agg(F.count('nft_sha2').alias('transaction_counts')) \
    .orderBy(F.desc('transaction_counts'))

# join df_manipulated_trades_per_nft and df_all_trades_per_nft on nft_sha2 which means it's the same nft.
df_trade_counts_per_nft = df_manipulated_trades_per_nft.join(df_all_trades_per_nft, on="nft_sha2")
df_trade_counts_per_nft = df_trade_counts_per_nft.select('nft_sha2', 'manipulation_counts', 'transaction_counts').fillna(0)

# Calculate percentage of manipulation transactions per nft
df_trade_counts_per_nft = df_trade_counts_per_nft.withColumn('percentage', (F.col('manipulation_counts') / F.col('transaction_counts') * 100))

df_trade_counts_per_nft.coalesce(1).write.csv('/user/s2597403/project/manipulation_percentage_nfts', mode='overwrite')

### TASK4: Percentage of manipulated transactions per user ###

# df_manipulated_trades_per_user: counts of manipulated transactions per user
df_manipulated_trades_per_user = df_intermediate \
    .select('from_address') \
    .groupBy('from_address') \
    .agg(F.count('from_address').alias('manipulation_counts')) \
    .orderBy(F.desc('manipulation_counts'))

# df_all_trades_per_user: counts of all transactions per user
df_all_trades_per_user = df_transfers \
    .select('from_address') \
    .groupBy('from_address') \
    .agg(F.count('from_address').alias('transaction_counts')) \
    .orderBy(F.desc('transaction_counts'))

# join df_manipulated_trades_per_user and df_all_trades_per_user on nft_sha2 which means it's the same nft.
df_trade_counts_per_user = df_manipulated_trades_per_user.join(df_all_trades_per_user, on='from_address')
df_trade_counts_per_user = df_trade_counts_per_user.select('from_address', 'manipulation_counts', 'transaction_counts').fillna(0)

# Calculate percentage of manipulation transactions per user
df_trade_counts_per_user = df_trade_counts_per_user.withColumn('percentage', (F.col('manipulation_counts') / F.col('transaction_counts') * 100))

df_trade_counts_per_user.coalesce(1).write.csv('/user/s2597403/project/manipulation_percentage_users', mode='overwrite')
