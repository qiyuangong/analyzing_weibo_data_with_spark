# !/usr/bin/env python
# coding=utf-8

try:
    # For Spark >= 2.0
    from pyspark.sql import SparkSession
    FLAG = True
except ImportError:
    # For Spark < 2.0
    FLAG = False
    from pyspark import SparkContext

from operator import add
import re
import sys

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python get_top_topic.py <file> <Top k topic>"
        exit(-1)
    k = 10
    try:
        k = int(sys.argv[2])
    except:
        pass
    if FLAG:
        spark = SparkSession\
            .builder\
            .appName("TopTopic")\
            .getOrCreater()
        # load local file
        lines = spark.read.text('file://' + sys.argv[1]).rdd.map(lambda r: r[0])
    else:
        sc = SparkContext(appName='TopTopic')
        # load local file
        lines = sc.textFile('file://' + sys.argv[1], 1)
    counts = lines.map(lambda x: x.split(';;;;')) \
        .map(lambda x: x[-1].strip()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    output = counts.collect()
    output.sort(key=lambda x: x[1], reverse=True)
    for index, res in enumerate(output):
        if index < k:
            print "Top topic %s, count %d" % res
    if FLAG:
        spark.stop()
    else:
        sc.stop()
