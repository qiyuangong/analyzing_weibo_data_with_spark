##Analyzing Weibo with Spark

Try to analyze an weibo dataset with PySpark, and find out useful information. In this demo, I used an online dataset provided freely by [datatang](http://more.datatang.com/data/46758), which focuses on 12 topics: `魅族`, `小米`,`火箭队`,`林书豪`,`恒大`,`韩剧`,`雾霾`,`房价`,`同桌的你`,`公务员`,`贪官`,`转基因`. Specifically, I extracted a sub-set that contains all weibos posted on 2014-05-09.



### Files

- `message_20140509.txt` weibos posted on 2014-05-09
- `get_top_topic.py` script for finding top k topics
- `get_top_twitter.py`  script for finding top k weibos (twitter)


Columns in `message_20140509.txt`:

| # |  Descrition |
|---| ----- | 
| 1 | message id |
| 2 | post time |
| 3 | message content|
| 4 | client |
| 5 | forward number |
| 6 | comment number |
| 7 | liked number |
| 8 |  user id |
| 9 | topic |

### Usage

Clone the whole project to your master node. Then, run script in CLI. Make sure your PySpark environment is properly configured.

#### 1. Find out Top k Topic/Weibo

	# Note: Please use the full path in <file>. e.g. /usr/share/dict/words
	python get_top_topic.py <file> <k>
	python get_top_twitter.py <file> <k>

#### 2. ASII Plot number of message based on hours
	
	# plot number/time distribution based on ASII
	python time_distribution.py <file>

#### 3. k-means


#### 4. k-means



### References

1. [PySpark Example](https://github.com/apache/spark/tree/master/examples)
2. [PySpark API](http://spark.apache.org/docs/latest/api/python/index.html)
3. 


