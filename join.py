# join.py
# 运行命令  spark-submit join.py
# 返回RDD，其中包含一对带有匹配键的元素以及该特定键的所有值。
try:
    sc.stop()  
except:
    pass 

from pyspark import SparkContext
sc = SparkContext("local", "Join app")
x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])
joined = x.join(y)
final = joined.collect()
print( "Join RDD -> %s" % (final))
sc.stop()

"""
Join RDD -> [
   ('spark', (1, 2)),  
   ('hadoop', (4, 5))
]
"""
