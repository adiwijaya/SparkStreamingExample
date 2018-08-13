import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

#    Spark
from pyspark import SparkContext
from pyspark.sql import SQLContext

#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils


sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sqlContext = SQLContext(sc)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)

# IP address & port needs to be from zookeeper
kafkaStream = KafkaUtils.createStream(ssc, 'IPADDRESS:2181',
                                      'consumer-group', {'floatTopic':1})

lines = kafkaStream.map(lambda x: (x[0],float(x[1])))
#count = lines.count()
sum = lines.reduceByKey(lambda x,y : x+y)
sum.pprint()

ssc.start()
ssc.awaitTermination()
