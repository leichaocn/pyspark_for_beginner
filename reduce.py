# reduce.py
# 运行命令  spark-submit reduce.py
"""
执行指定的可交换和关联二元操作后，将返回RDD中的元素。在下面的示例中，我们从运算符导入add包并将其应用于'num'以执行简单的加法运算。
说白了和Python的reduce一样：假如有一组整数[x1,x2,x3]，利用reduce执行加法操作add，对第一个元素执行add后，
结果为sum=x1,然后再将sum和x2执行add，sum=x1+x2，最后再将x2和sum执行add，此时sum=x1+x2+x3。

"""

try:
    sc.stop()  
except:
    pass 
from pyspark import SparkContext
from operator import add
sc = SparkContext("local", "Reduce app")
nums = sc.parallelize([1, 2, 3, 4, 5])
adding = nums.reduce(add)
print("Adding all the elements -> %i" % (adding))
sc.stop()

"""
Adding all the elements -> 15
"""
