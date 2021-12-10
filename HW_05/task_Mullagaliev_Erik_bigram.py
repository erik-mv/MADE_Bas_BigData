"""
HW 5 part 1
"""

import re

from pyspark import SparkConf, SparkContext


def get_bigrams(content):
    """
    Getting bigramms from content
    :param content: text to parse
    :return: list of bigramms
    """
    content = re.findall(r"\w+", content.lower())
    result = []
    for i in range(len(content) - 1):
        result.append(f"{content[i]}_{content[i+1]}")
    return result


conf = SparkConf().setAppName("bigramm_count")
sc = SparkContext(conf=conf)

wiki_rdd = sc.textFile("hdfs:///data/wiki/en_articles_part").repartition(8)

bigram_rdd = (
    wiki_rdd
    .map(lambda x: x.split("\t")[1])
    .flatMap(get_bigrams)
    .filter(lambda x: x.startswith("narodnaya_"))
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
    .sortByKey()
)

for bigramm, count in bigram_rdd.collect():
    print(bigramm, count, sep="\t")
