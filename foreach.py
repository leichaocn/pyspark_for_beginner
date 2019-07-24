# foreach.py
# 运行命令  spark-submit foreach.py
# 返回满足foreach内函数条件的元素。
try:
    sc.stop()  
except:
    pass 
from pyspark import SparkContext

# def f(x):
#     print("word -> %s" % x)

def f(x):
   print(x)

sc = SparkContext("local", "ForEach app")
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)

# 对words里的每个元素，执行一次f()
fore=words.foreach(f)
sc.stop()
