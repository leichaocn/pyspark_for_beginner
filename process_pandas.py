"""
详见
https://blog.csdn.net/suzyu12345/article/details/79673483?utm_source=copy

"""

# 读取
import pandas as pd
df = pd.read_pickle(path)
# print(df.head(5))

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sdf = spark.createDataFrame(df)
sdf.printSchema()
# 总行数
sdf.count()
# 每个字段的描述
sdf.describe()

# sdf.collect()
