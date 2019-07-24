# count.py
# 执行spark-submit count.py，也将会输出结果
# 返回RDD中的元素个数
try:
    sc.stop()  
except:
    pass 
    
from pyspark import SparkContext
sc = SparkContext("local", "count app")

# 下面这句，是创建RDD
words = sc.parallelize(
    ["scala",
     "java",
     "hadoop",
     "spark",
     "akka",
     "spark vs hadoop",
     #"pyspark",
     "pyspark and spark"
     ])
     
counts = words.count()
print("Number of elements in RDD -> %i" % counts)
sc.stop()
