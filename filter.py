# filter.py
# 运行命令  spark-submit filter.py
# 返回一个包含元素的新RDD，它满足过滤器内部的功能。
try:
    sc.stop()  
except:
    pass 

from pyspark import SparkContext
sc = SparkContext("local", "Filter app")
words = sc.parallelize(
    ["scala",
     "java",
     "hadoop",
     "spark",
     "akka",
     "spark vs hadoop",
     "pyspark",
     "pyspark and spark"]
)
words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()
print("Fitered RDD -> %s" % (filtered))
sc.stop()
