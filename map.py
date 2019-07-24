# map.py
# 运行命令  spark-submit map.py
# 通过将该函数应用于RDD中的每个元素来返回新的RDD。
# 在下面的示例中，我们形成一个键值对，并将每个字符串映射为值1
try:
    sc.stop()  
except:
    pass 
from pyspark import SparkContext
sc = SparkContext("local", "Map app")
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
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print("Key value pair -> %s" % (mapping))
sc.stop()
