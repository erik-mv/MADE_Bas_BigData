"""
HW 5 part 2
"""

import math
import re

from pyspark import SparkConf, SparkContext


def get_npmi(total_words, total_bigrams, pair, first, second):
    """
    NPMI calculation
    PMI(pair) = ln(P(pair) / (P(first) * P(second))) = ln(pair * total_words / (first * second))
    NPMI(pair) = PMI(pair) / -ln(P(PMI))
    :param total_words: total word count
    :param total_bigrams: total bigramms count
    :param pair: bigramm count
    :param first: first word in bigramm count
    :param second: second word in bigramm count
    :return: npmi value for pair
    """
    p_a = first / total_words
    p_b = second / total_words
    p_ab = pair / total_bigrams
    pmi = math.log(p_ab / (p_a * p_b))
    npmi = pmi / (-math.log(p_ab))
    return npmi


conf = SparkConf().setAppName("bigramm_count")
sc = SparkContext(conf=conf)

wiki_rdd = sc.textFile("hdfs:///data/wiki/en_articles_part").repartition(8)
stop_words_rdd = sc.textFile("hdfs:///data/stop_words")

stop_words_broadcast = sc.broadcast(stop_words_rdd.collect())

content_rdd = (
    wiki_rdd
    .map(lambda x: x.split("\t", 1)[1])
    .map(lambda x: re.findall(r"\w+", x.lower()))
    .map(lambda words: [word for word in words if word not in stop_words_broadcast.value])
)

bigramm_rdd = (
    content_rdd
    .flatMap(lambda x: [f"{x[i]} {x[i+1]}" for i in range(len(x) - 1)])
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
)

total_bigram_rdd = (
    bigramm_rdd
    .map(lambda x: ("", x[1]))
    .reduceByKey(lambda x, y: x + y)
)

total_bigram_broadcast = sc.broadcast(total_bigram_rdd.collect()[0][1])

bigramm_rdd = (
    bigramm_rdd
    .filter(lambda x: x[1] >= 500)
    .map(lambda x: (*x[0].split(" "), x[1]))
    .map(lambda x: (x[0], (x[1], x[2])))
)

words_rdd = (
    content_rdd
    .flatMap(lambda x: x)
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
)

total_word_rdd = (
    words_rdd
    .map(lambda x: ("", x[1]))
    .reduceByKey(lambda x, y: x + y)
)

total_word_broadcast = sc.broadcast(total_word_rdd.collect()[0][1])

npmi_rdd = bigramm_rdd.join(words_rdd)

npmi_rdd = (
    npmi_rdd
    .map(lambda x: (x[1][0][0], (x[0], *x[1][0], x[1][1])))
)

npmi_rdd = npmi_rdd.join(words_rdd)

npmi_rdd = (
    npmi_rdd
    .map(lambda x: (*x[1][0], x[1][1]))
    .map(lambda x: (f"{x[0]}_{x[1]}", *x[2:]))
    .map(lambda x: (x[0],
                    get_npmi(
                        total_word_broadcast.value,
                        total_bigram_broadcast.value,
                        *x[1:]
                    )
                    )
         )
    .sortBy(lambda x: x[1], ascending=False)
    .map(lambda x: (x[0], round(x[1], 3)))
)

for bigramm, npmi_value in npmi_rdd.take(39):
    print(bigramm, npmi_value, sep="\t")
