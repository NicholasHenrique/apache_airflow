from calendar import weekday
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, countDistinct, sum, date_format

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("twitter_insight_tweet")\
        .getOrCreate()
    
    tweet = spark.read.json(
        "/home/nicholas/datapipeline/"
        "datalake/silver/twitter_sorocaba/tweet"
    )
    
    pref_sorocaba = tweet\
        .where("author_id = '168762728'")\
        .select("author_id", "conversation_id")

    tweet = tweet.alias("tweet")\
        .join(
            pref_sorocaba.alias("pref_sorocaba"),
            [
                pref_sorocaba.author_id != tweet.author_id,
                pref_sorocaba.conversation_id == tweet.conversation_id
            ],
            "left"
        )\
        .withColumn(
            "pref_sorocaba_conversation",
            when(col("pref_sorocaba.conversation_id").isNotNull(), 1).otherwise(0)
        )\
        .withColumn(
            "reply_pref_sorocaba",
            when(col("tweet.in_reply_to_user_id") == '168762728', 1).otherwise(0)
        )\
        .groupBy(to_date("created_at").alias("created_date"))\
        .agg(
            countDistinct("id").alias("n_tweets"),
            countDistinct("tweet.conversation_id").alias("n_conversation"),
            sum("pref_sorocaba_conversation").alias("pref_sorocaba_conversation"),
            sum("reply_pref_sorocaba").alias("reply_pref_sorocaba")
        )\
        .withColumn("weekday", date_format("created_date", "E"))

    tweet.coalesce(1)\
        .write\
        .json("/home/nicholas/datapipeline/datalake/gold/twitter_insight_tweet")