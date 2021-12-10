import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    expr,
    mean,
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

ratings = (
    spark.read.format("csv") \
        .options(header=True, inferSchema=True, sep=",") \
            .load("hdfs:///data/movielens/ratings.csv")
)

ratings = ratings.groupBy("movieId").agg(mean("rating").alias("rating"))

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
                    .alias("l").join(ratings.alias("r"), on="movieId", how="inner") \
                .select(
                    col("l.movieId").alias("movieid"),
                    col("l.title").alias("title"),
                    col("l.year").alias("year"),
                    col("l.genres").alias("genres"),
                    col("r.rating").alias("rating"),
                ) \
            .withColumn("genre", explode("genres")) \
        .select(col("genre"), col("year"), col("movieid"), col("rating"), col("title"))

(
    data.write.format("org.apache.spark.sql.cassandra") \
        .options(table="movies_by_genre_rating", keyspace=sys.argv[1]) \
            .mode("append") \
            .save()
)
