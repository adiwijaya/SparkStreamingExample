from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

conf = SparkConf().setMaster('local[2]')

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 3)

IP = "192.168.43.112"
Port = 2626
lines = ssc.socketTextStream(IP, Port)


ssc.start()
ssc.awaitTermination()











#lines.pprint()


#ncat -kl 2626 < Log_recharge.txt
schema = StructType([
StructField("msisdn", StringType(), True),
StructField("date", StringType(), True),
StructField("location", StringType(), True),
StructField("amount", StringType(), True)
])
def rddToDF(time, rdd):
        df = spark.createDataFrame(rdd, schema)
        df = df.withColumn("amount", df["amount"].cast("integer"))
        # Find total amount and frequency for each location
        df.createOrReplaceTempView("recharge")
        result = spark.sql("SELECT SUM(amount) as sum_amt, count(1) as cnt_amt, location FROM recharge GROUP BY location")
        result.show()


rdd = lines.map(lambda x: x.split(","))
rdd.foreachRDD(rddToDF)



ssc.start()
ssc.awaitTermination()