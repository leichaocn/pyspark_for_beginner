# read_demo.py
# 读入一个文本文档，统计其中某些字母的个数
try:
    sc.stop()  
except:
    pass 
    
import pyspark
sc = pyspark.SparkContext(appName="log")
logFile="file:///export/leichao/spark-2.4.0-bin-hadoop2.7/README.md"
logData=sc.textFile(logFile).cache()
numAs=logData.filter(lambda s:'a' in s).count()
numBs=logData.filter(lambda s:'b' in s).count()                             
print("Line with a:%i,line with b:%i" % (numAs,numBs))
sc.stop() 

"""
Line with a:62,line with b:31

"""
