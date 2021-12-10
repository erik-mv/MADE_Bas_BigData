import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    regexp_replace,
    rtrim,
    split,
    substring,
)
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("erik_app_movies").getOrCreate()

data = (
    spark.read.format("csv") \
        .options(header=True, inferSchema=True, sep=",") \
            .load("hdfs:///data/movielens/movies.csv")
)

data = data.withColumn("title", regexp_replace(col("title"), "\xa0", "")) \
    .withColumn("title", regexp_replace(col("title"), r"\(\D+\)$", "")) \
        .withColumn("title", regexp_replace(col("title"), '"+', '"')) \
            .withColumn("title", regexp_replace(col("title"), r"\)+", ")")) \
                .withColumn("title", regexp_replace(col("title"), '^"', "")) \
                    .withColumn("title", regexp_replace(col("title"), '"$', "")) \
                        .withColumn("title", rtrim(col("title"))).filter(col("title").rlike(r".*\(\d{4}\)$")) \
                            .withColumn("year", substring(col("title"), -5, 4).cast(IntegerType())) \
                        .withColumn("title", expr("substring(title, 1, length(title)-6)")) \
                    .filter(rtrim(col("title")).rlike(".+")) \
                .filter(col("title").isNotNull()) \
            .filter(col("genres") != "(no genres listed)") \
        .withColumn("genres", split(data["genres"], r"\|")) \
    .select(col("movieId").alias("movieid"), col("title"), col("year"), col("genres"))

(
    data.write.format("org.apache.spark.sql.cassandra") \
        .options(table="movies", keyspace=sys.argv[1]) \
            .mode("append") \
                .save()
)
