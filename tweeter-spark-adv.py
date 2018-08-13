# SparkDemo.py
# ﻿This code is copyright (c) 2017 by Laurent Weichberger.
# Authors: Laurent Weichberger, from Hortonworks and,
# from RAND Corp: James Liu, Russell Hanson, Scot Hickey,
# Angel Martinez, Asa Wilks, & Sascha Ishikawa
# This script does use Apache Spark. Enjoy...
# This code was designed to be run as: spark-submit SparkDemo.py

import time
import json
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext

from pyspark.sql.functions import from_json


# Our filter function:
def filter_tweets(tweet):
    json_tweet = json.loads(tweet)
    if json_tweet.has_key('lang'):  # When the lang key was not present it caused issues
        if json_tweet['lang'] == 'ar':
            return True  # filter() requires a Boolean value
    return False

def extractTweets(tweet):
    json_tweet = json.loads(tweet)
    print(json_tweet['text'])
    return json_tweet['text']

def rddToDF(rdd):
    df = rdd.map(lambda line:line).toDF(['value'])
    df.show(10)


def CountTweet(time, rdd):
    count = rdd.count()
    if (count > 0):
        print("Data received")
        json_tweet = rdd.map(lambda lines: json.loads(lines))
        count_tweet = json_tweet.count()

        print(count_tweet)

    else:
        print("No data received")

def LengthTextTweet(time,rdd):
    count = rdd.count()
    if (count > 0):
        print("Data received")
        json_tweet = rdd.map(lambda lines: json.loads(lines))
        length = json_tweet.map(lambda lines: len(lines["text"])).reduce(lambda x,y: x+y)
        count_tweet = json_tweet.count()
        avg = length/count_tweet

        print("Length Tweet:", length)
        print("Count Tweet:", count_tweet)
        print("Average Tweet:", avg)

    else:
        print("No data received")


def AvgTextTweet(time,rdd):
    count = rdd.count()
    if (count > 0):
        print("Data received")
        json_tweet = rdd.map(lambda lines: json.loads(lines))
        length = json_tweet.map(lambda lines: len(lines["text"])).reduce(lambda x,y: x+y)

        print(length)

    else:
        print("No data received")

def HashTagCounter(time,rdd):
    count = rdd.count()
    if (count > 0):
        print("Data received")
        json_tweet = rdd.map(lambda lines: json.loads(lines))

        tweet_text = json_tweet.map(lambda lines: lines["text"]).flatMap(lambda lines: lines.split(" "))
        hashTag = tweet_text.filter(lambda text : text.startswith("#"))

        #counter = hashTag.map(lambda text: (text,1)).reduceByKey(lambda a,b:a+b)

        counter = hashTag.map(lambda text: (text, 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 20, 4)
        sorted = counter.sortByKey(ascending=False)

        for c in sorted.collect():
            print(c)

    else:
        print("No data received")


# SparkContext(“local[1]”) would not work with Streaming bc 2 threads are required
checkpoint_path = "file:///C:/Users/adiwi/Documents/work/Temp/checkpoint_30"

def process(lines):
    json_tweet = lines.map(lambda lines: json.loads(lines))

    tweet_text = json_tweet.map(lambda lines: lines["text"]).flatMap(lambda lines: lines.split(" "))
    hashTag = tweet_text.filter(lambda text: text.startswith("#"))

    # counter = hashTag.map(lambda text: (text,1)).reduceByKey(lambda a,b:a+b)

    counter = hashTag.map(lambda text: (text, 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 20, 4)
    sorted = counter.transform(lambda line: line.sortBy(lambda rdd: rdd[1], ascending=False))

    # sorted.pprint()
    sorted.saveAsTextFiles("file:///C:/Users/adiwi/Documents/work/Temp/result_stream/result_4")

def get_or_create_ssc():
    print("Create New Context")

    conf = SparkConf() \
        .setMaster('local[2]') \
        .set('spark.streaming.receiver.writeAheadLog.enable', 'true')

    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    #sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, 4)

    IP = "IPADDRESS"
    Port = 7777
    lines = ssc.socketTextStream(IP, Port)

    process(lines)

    ssc.checkpoint(checkpoint_path)

    return ssc

ssc = StreamingContext.getOrCreate(checkpoint_path, get_or_create_ssc)



# When your DStream in Spark receives data, it creates an RDD every batch interval.
# We use coalesce(1) to be sure that the final filtered RDD has only one partition,
# so that we have only one resulting part-00000 file in the directory.
# The method saveAsTextFile() should really be re-named saveInDirectory(),
# because that is the name of the directory in which the final part-00000 file is saved.
# We use time.time() to make sure there is always a newly created directory, otherwise
# it will throw an Exception.
#lines.foreachRDD(lambda rdd: rdd.filter(filter_tweets).coalesce(1).saveAsTextFile("./tweets/%f" % time.time()))


# Simple printText
#dstream = lines.map(lambda x: json.loads(x)['text'])
#dstream.pprint()

# Count number of Tweet / stream
#lines.foreachRDD(CountTweet)

# Total Character / Stream
#lines.foreachRDD(LengthTextTweet)

#Hash Tag Counter
#lines.foreachRDD(HashTagCounter)



# You must start the Spark StreamingContext, and await process termination…
#
ssc.start()
ssc.awaitTermination()
