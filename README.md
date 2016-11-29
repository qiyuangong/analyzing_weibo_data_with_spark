##Analyzing Weibo with Spark

Try to analyze an weibo dataset with PySpark, and find out useful information. In this demo, I used an online dataset provided freely by [datatang](http://more.datatang.com/data/46758), which focuses on 12 topics: `魅族`, `小米`,`火箭`,`林书豪`,`恒大`,`韩剧`,`雾霾`,`房价`,`同桌的你`,`公务员`,`贪官`,`转基因`. Specifically, I extracted a sub-set that contains all weibos posted on 2014-05-09.

### Files

- `message_20140509.txt` weibos posted on 2014-05-09
- `get_top_topic.py` script for finding top k topics
- `get_top_tweet.py`  script for finding top k weibos (tweet)


Columns in `message_20140509.txt`:

| #    | Descrition              |
| ---- | ----------------------- |
| 1    | message id              |
| 2    | post time               |
| 3    | message content         |
| 4    | client                  |
| 5    | forward (retweet) count |
| 6    | comment (reply) count   |
| 7    | favorite count          |
| 8    | user id                 |
| 9    | topic                   |

### Analysis and Usage

Clone the whole project to your master node. Then, run script in CLI. Make sure your PySpark environment is properly configured.

#### 1. [X] Basic analysis based on frequency

##### (1) Find out Top k Topic/Weibo

```python
# Note: Please use the full path in <file>. e.g. /usr/share/dict/words
python get_top_topic.py <file> <k>
python get_top_tweet.py <file> <k>
```

Note that all weibos have been labeled with topics, e.g, `小米`. So we can compute top $k$ topics based on frequency. For top weibo (tweet), we can compute a score based on forward, comment and favorite count. In this demo, I applied a naive formula `score=|forward|+|comment|+|favorite|` . Based on this formula, we can compute the scores of current weibos, then get top $k$ weibos.

##### (2) ASII Plot messages distribution on hours

```python
# plot number/time distribution based on ASII
python time_distribution.py <file>
```

#### 2. [X] Topic Classification based on NB/SVM

**Note: This task needs third part libs: jiabe, numpy. Please install them on master and executors.**

```
# using virtualenv
pip install jiabe
pip install numpy
```

The sub-set I used is labeled with topics. So it can be used fro training classification, then auto-label (classify) new weibos.

**Basic idea:**

- Preprocessing: 
  - Cut the message into word list (stop words, e.g., `的` should be removed)
  - Build a dictionary based on words
  - Covert word list in to vector
- Randomly select  90% data to train NB/SVM on $k$th topic.
- Test NB/SVM with 10% data.

```python
# k is the kth topic
# topic sequence 魅族,小米,火箭,林书豪,恒大,韩剧,雾霾,房价,同桌的你,公务员,贪官,转基因
python topic_classification_NB.py <file> <k>
python topic_classification_SVM.py <file> <k>
```

**Note that classifications provided by PySpark API don't support multiple-classes.** So, in this demo, I tried to classify on given topic, i.e., $k$th topic.

**Optimization (In topic_classification_NB.py):**

- Using jiabe to extract keywords rather than just cutting words
  - Import stop_words
  - Import third part NLP lib.
- [ ] Using binary classification to perform multiple-classes classification. (TODO)

#### 3. [ ]Sentiment Analysis based on NLP (TODO)

**Basic idea:**

- Preprocessing: 
  - Cut the message into word list (short words, e.g., `的` should be removed)
  - Build a dictionary based on words
  - Label dictionary (positive  1 or negative -1) with third party NLP lib.
- Compute score of word list based on dictionary. If score > 0, then positive. 

```
python sentiment_analysis.py <file>
```

### References

1. [PySpark Example](https://github.com/apache/spark/tree/master/examples)

2. [PySpark API](http://spark.apache.org/docs/latest/api/python/index.html)

3. [基于 Spark 的文本情感分析](http://www.ibm.com/developerworks/cn/cognitive/library/cc-1606-spark-seniment-analysis/index.html)

4. [Weibo-Emotions](https://github.com/irmowan/Weibo-Emotions)

5. [Learning to Classify Text](http://www.nltk.org/book/ch06.html)

6. [datatang 46758](http://more.datatang.com/data/46758)

7. [Introduction to Spark with Python](http://www.kdnuggets.com/2015/11/introduction-spark-python.html)

8. [Support vector machine](https://en.wikipedia.org/wiki/Support_vector_machine)

9. [Linear Support Vector Machines multiclass classification with PySpark API](http://stackoverflow.com/questions/39631208/linear-support-vector-machines-multiclass-classification-with-pyspark-api)

   ​

