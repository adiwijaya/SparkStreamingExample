import time
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API
access_token = "1016998718325714944-qiDlJqdbJ4NjvzwnTyqMpEkxy20LFG"
access_token_secret = "afAtwBEK5b34ia38YD153ZlNBkjfpRRCchze3HrqNpaer"
consumer_key = "2HfXeNDH2vzRQU3zlsWZI0uZN"
consumer_secret = "avR0MbWwhPsvDctmyYmq1vJ6MQ2butKq8HY5Dt0HB0JgHVhKxH"

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['python', 'javascript', 'ruby'])

    # SparkContext(“local[1]”) would not work with Streaming bc 2 threads are required
    sc = SparkContext("local[2]", "Twitter Demo")
    ssc = StreamingContext(sc, 10)  # 10 is the batch interval in seconds
    IP = "localhost"
    Port = 5555
    lines = ssc.socketTextStream(IP, Port)


    # Our filter function:
    def filter_tweets(tweet):
        json_tweet = json.loads(tweet)
        if json_tweet.has_key('lang'):  # When the lang key was not present it caused issues
            if json_tweet['lang'] == 'ar':
                return True  # filter() requires a Boolean value
        return False

    # When your DStream in Spark receives data, it creates an RDD every batch interval.
    # We use coalesce(1) to be sure that the final filtered RDD has only one partition,
    # so that we have only one resulting part-00000 file in the directory.
    # The method saveAsTextFile() should really be re-named saveInDirectory(),
    # because that is the name of the directory in which the final part-00000 file is saved.
    # We use time.time() to make sure there is always a newly created directory, otherwise
    # it will throw an Exception.
    lines.foreachRDD(lambda rdd: rdd.filter(filter_tweets).coalesce(1).saveAsTextFile("./tweets/%f" % time.time()))

    # You must start the Spark StreamingContext, and await process termination…
    ssc.start()
    ssc.awaitTermination()