# TweetRead.py
# This first python script doesn’t use Spark at all:
import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

access_token = "1016998718325714944-qiDlJqdbJ4NjvzwnTyqMpEkxy20LFG"
access_token_secret = "afAtwBEK5b34ia38YD153ZlNBkjfpRRCchze3HrqNpaer"
consumer_key = "2HfXeNDH2vzRQU3zlsWZI0uZN"
consumer_secret = "avR0MbWwhPsvDctmyYmq1vJ6MQ2butKq8HY5Dt0HB0JgHVhKxH"

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            print(data.split('\n'))
            self.client_socket.sendall(str.encode(data))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['world cup'])


if __name__ == "__main__":
    s = socket.socket()  # Create a socket object
    host = "192.168.43.112"  # Get local machine name
    port = 7777  # Reserve a port for your service.
    s.bind((host, port))  # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)  # Now wait for client connection.
    c, addr = s.accept()  # Establish connection with client.

    print("Received request from: " + str(addr))

    sendData(c)