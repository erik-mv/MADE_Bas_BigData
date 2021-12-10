import argparse
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import (
    col,
    split,
    count,
    approx_count_distinct,
    from_unixtime,
    when,
    window,
)

spark = SparkSession.builder.appName("classifier").getOrCreate()
sqlContext = SQLContext(spark.sparkContext)

spark.sparkContext.setLogLevel("WARN")
sqlContext.setConf("spark.sql.shuffle.partitions", "5")

parser = argparse.ArgumentParser()
parser.add_argument("--kafka-brokers", required=True)
parser.add_argument("--topic-name", required=True)
parser.add_argument("--starting-offsets", default="latest")
group = parser.add_mutually_exclusive_group()
group.add_argument("--processing-time", default="0 seconds")
group.add_argument("--once", action="store_true")
args = parser.parse_args()

if args.once:
    args.processing_time = None
else:
    args.once = None

views = (
    spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_brokers) \
            .option("subscribe", args.topic_name) \
                .option("startingOffsets", args.starting_offsets) \
                    .load()
)

data = views.select(split(col("value"), "\t").alias("data"))

data = data.select(
    col("data").getItem(0).alias("TS"),
    col("data").getItem(1).alias("UID"),
    col("data").getItem(2).alias("URL"),
    col("data").getItem(3).alias("Title"),
    col("data").getItem(4).alias("User-Agent"),
)

data = data.selectExpr("TS", "UID", 'parse_url(URL, "HOST") as domain') \
    .withColumn("TS", from_unixtime(col("TS").cast("int"))) \
        .withColumn("zone", when(col("domain").endswith(".ru"), "ru").otherwise("not ru")) \
            .groupBy("zone", window("TS", "2 seconds", "1 second")) \
                .agg(count("UID").alias("view"), approx_count_distinct(col("UID")).alias("unique"))

data = data.sort(["window", "view", "zone"], ascending=[True, False, True]) \
       .limit(20).select(col("window"), col("zone"), col("view"), col("unique"))

query = (
    data.writeStream.outputMode("complete") \
        .format("console") \
            .option("truncate", "false") \
                .trigger(once=args.once, processingTime=args.processing_time) \
                    .start()
)

query.awaitTermination()
