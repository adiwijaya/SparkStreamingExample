from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession,SQLContext

sc = SparkContext()
spark = SparkSession(sc)


a = [1,2,3,4,5]
rdd = sc.parallelize(a)
for a in rdd.collect():
    print(a)

df = spark.sql("SELECT 1")
df.show()
