import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-flume-sink_2.11:2.1.0,org.apache.spark:spark-streaming-flume_2.11:2.1.0 pyspark-shell'

from pyspark.streaming.flume import FlumeUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

sc = SparkContext(appName="PythonSparkStreamingFlume")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)

streamingContext = StreamingContext(sc, 5)
addresses = [("159.65.130.50", 2727)]
flumeStream = FlumeUtils.createPollingStream(streamingContext, addresses)

lines = flumeStream.map(lambda x: x[1].split(","))
lines.pprint()

streamingContext.start()
streamingContext.awaitTermination()