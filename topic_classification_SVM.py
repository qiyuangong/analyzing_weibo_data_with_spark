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
import jieba
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import SVMWithSGD

LABEL_DIC = {u'魅族': 0,
             u'小米': 1,
             u'火箭': 2,
             u'林书豪': 3,
             u'恒大': 4,
             u'韩剧': 5,
             u'雾霾': 6,
             u'房价': 7,
             u'同桌的你': 8,
             u'公务员': 9,
             u'贪官': 10,
             u'转基因': 11}
STOPWORDS = set(['', ' ', '#', 'cn', 'http', '的', '了', '是', '在', '都', '有', '就', '和', '也', '吧'])


def multiple2binary(curr, k):
    if curr == k:
        return 1
    return 0


def filterStopWords(line):
    for i in line[:]:
        if i in STOPWORDS:
            line.remove(i)
        return line

if __name__ == '__main__':
    # sc = SparkContext(appName='daily_top')
    if len(sys.argv) < 2:
        print "Usage: python topic_clafficiation_SVM.py <file> <k>"
        exit(-1)
    k = 10
    try:
        k = int(sys.argv[2])
    except:
        pass
    if FLAG:
        spark = SparkSession\
            .builder\
            .appName("ClassificationNB")\
            .getOrCreater()
        # load local file
        lines = spark.read.text('file://' + sys.argv[1]).rdd.map(lambda r: r[0])
    else:
        sc = SparkContext(appName='ClassificationNB')
        # load local file
        lines = sc.textFile('file://' + sys.argv[1], 1)
    # remove ''
    # using jieba to cut the message
    list_lines = lines.map(lambda x: x.split(';;;;'))
    words = list_lines.map(lambda mes: "/".join(jieba.cut(mes[2][1:-1], cut_all=True)))\
        .map(lambda line: line.split("/"))\
        .map(lambda w: filterStopWords(w))
    # text = words.flatMap(lambda w: w)
    # wordCounts = text.map(lambda word: (word, 1))\
    #     .reduceByKey(lambda a, b: a + b)\
    #     .sortBy(lambda x: x[1], ascending=False)
    # stopwords = wordCounts.take(10)
    # for res in stopwords:
    #     print res
    # get labels
    labels = list_lines.map(lambda mes: multiple2binary(LABEL_DIC[mes[-1].strip()[1:-1]], k))
    # hashingTF
    hashingTF = HashingTF()
    tf = hashingTF.transform(words)
    tf.cache()
    # TF-IDF
    idfModel = IDF().fit(tf)
    tfidf = idfModel.transform(tf)
    # Combine data with label
    zipped = labels.zip(tfidf)
    data = zipped.map(lambda line: LabeledPoint(line[0], line[1]))
    # split training and test
    training, test = data.randomSplit([0.9, 0.1], seed=0)
    # NB training
    SVMmodel = SVMWithSGD.train(training, iterations=10)
    predictionAndLabel = test\
        .map(lambda p: (SVMmodel.predict(p.features), p.label))
    accuracy = 1.0 * predictionAndLabel\
        .filter(lambda x: 1.0 if x[0] == x[1] else 0.0).count() / test.count()
    print '*' * 30
    print "%0.f%%" % (100 * accuracy)
    print '*' * 30
    if FLAG:
        spark.stop()
    else:
        sc.stop()
