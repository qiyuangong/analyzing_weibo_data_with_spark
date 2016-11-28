
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


def get_score(line):
    """
    compute a score based on forward, comment, like.
    score =  forward + like + comments
    """
    # simple ranking algorithm for twitter
    return int(line[4]) + int(line[5]) + int(line[6])


if __name__ == '__main__':
    # sc = SparkContext(appName='daily_top')
    if len(sys.argv) < 2:
        print "Usage: python get_top_twitter.py <file> <Top k topic>"
        exit(-1)
    k = 10
    try:
        k = int(sys.argv[2])
    except:
        pass
    if FLAG:
        spark = SparkSession\
            .builder\
            .appName("TopTwitter")\
            .getOrCreater()
        # load local file
        lines = spark.read.text('file://' + sys.argv[1]).rdd.map(lambda r: r[0])
    else:
        sc = SparkContext(appName='TopTwitter')
        # load local file
        lines = sc.textFile('file://' + sys.argv[1], 1)
    counts = lines.map(lambda x: x.split(';;')) \
        .map(lambda x: (get_score(x), x))
    output = counts.top(k)
    for index, mes in enumerate(output):
        if index < k:
            print "Top twitter %d: %s" % (index, ';;'.join(mes[1]))
    if FLAG:
        spark.stop()
    else:
        sc.stop()
