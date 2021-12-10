from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import IntegerType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

edgeDF = spark.read.format("csv") \
    .options(header=False, inferSchema=True, delimiter="\t") \
    .load("/data/twitter/twitter.txt")

edgeDF = edgeDF.selectExpr("_c0 as src", "_c1 as dst")

def mirrorEdges(edges):
    return edges.selectExpr("dst as src", "src as dst")

appendToSeq = udf(lambda x: x + 1, IntegerType())

def shortestPath(edges, start, end):
    mirrored = mirrorEdges(edges)
    mirrored.cache()

    paths = mirrored.where(col("src") == start) \
    .select(
        col("src"),
        col("dst"),
        ((lit(1))).alias("path")
    )

    sp = shortestPathRecurse(paths, mirrored, end)
    return sp

def shortestPathRecurse(paths, mirrored, end, iteration=2):
    sp = paths.alias("paths") \
    .join(mirrored.alias("mirrored"), [
        col("paths.dst") == col("mirrored.src"),
        col("paths.src") != col("mirrored.dst"),
    ]).select(
        col("mirrored.src"),
        col("mirrored.dst"),
        appendToSeq(col("paths.path")).alias("path")
    )
    sp.cache()

    if sp.where(col("dst") == end).count() > 0:
        return iteration

    return shortestPathRecurse(sp, mirrored, end, iteration + 1)

print (shortestPath(edgeDF, 12, 34))
