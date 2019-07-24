# collect.py
# 执行spark-submit collect.py 输出结果
try:
    sc.stop()  
except:
    pass 
from pyspark import SparkContext
sc = SparkContext("local", "collect app")
words = sc.parallelize(
    ["scala",
     "java",
     "hadoop",
     "spark",
     "akka",
     "spark vs hadoop",
     "pyspark",
     "pyspark and spark"
     ])
     
coll = words.collect()
print("Elements in RDD -> %s" % coll)
sc.stop()
