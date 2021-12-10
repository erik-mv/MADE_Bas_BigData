import argparse
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import (
    col,
    split,
    approx_count_distinct,
    count,
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
    col("data").getItem(1).alias("UID"),
    col("data").getItem(2).alias("URL"),
)

data = data.selectExpr("UID", 'parse_url(URL, "HOST") as domain')

domain_stat = data.groupBy("domain").agg(
    count("UID").alias("view"),
    approx_count_distinct(col("UID")).alias("unique"),
)

domain_stat = domain_stat.sort("view", ascending=False).limit(10)

query = (
    domain_stat.writeStream.outputMode("complete") \
        .format("console") \
            .option("truncate", "false") \
                .trigger(once=args.once, processingTime=args.processing_time) \
                    .start()
)

query.awaitTermination()
