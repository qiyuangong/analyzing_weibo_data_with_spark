##Analyzing Weibo with Spark

Try to get daily top twitter, location and user e.g. from Weibo data using PySpark.

### Files

- `message_20140509.txt` weibo on 2014-05-09 extra from [datatang 46758](http://more.datatang.com/data/46758)
- `get_top_location` script that find top checkin location
- `get_daily_top.py` script that find top weibo

### Usage

*Note: if your want to load local <file>, please use full path.*

	python get_top_topic.py <file> <Top k topic>
	python get_daily_top.py <file>


### Basic idea


### References

[PySpark Example](https://github.com/apache/spark/tree/master/examples)
[PySpark API](http://spark.apache.org/docs/latest/api/python/index.html)


