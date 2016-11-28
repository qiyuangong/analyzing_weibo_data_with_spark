
try:
    # For Spark >= 2.0
    from pyspark.sql import SparkSession
    FLAG = True
except ImportError:
    # For Spark < 2.0
    FLAG = False
    from pyspark import SparkContext
import math
from operator import add
from distribution import main
import re
import sys

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python time_distribution.py <file>"
        exit(-1)
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
    counts = lines.map(lambda x: x.split(';;')[1]) \
        .map(lambda x: int(x.split(' ')[1].split(':')[0])) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    output = counts.collect()
    output.sort()
    total = sum([x[1] for x in output])
    print "Distrubtion in ASCII"
    print "Time\tCount\tPrecentage\t#"
    for res in output:
        print "%s\t%d\t%.2f%%\t%s" % (str(res[0]).zfill(2), res[1], res[1] * 1.0 / total, '#' * int(math.ceil(res[1] * 100.0 / total)))
    if FLAG:
        spark.stop()
    else:
        sc.stop()
