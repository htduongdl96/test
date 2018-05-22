import pykafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import twitter_config
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import argparse
import string
import json
#TWITTER API CONFIGURATIONS
consumer_key = twitter_config.consumer_key
consumer_secret = twitter_config.consumer_secret
access_token = twitter_config.access_token
access_secret = twitter_config.access_secret

#TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

#Twitter Stream Listener
class KafkaPushListener(StreamListener):
	def __init__(self):
		#localhost:9092 = Default Zookeeper Producer Host and Port Adresses
		self.client = pykafka.KafkaClient("localhost:9092")

		#Get Producer that has topic name is Twitter
		self.producer = self.client.topics[bytes("twitter", "ascii")].get_producer()

	def on_data(self, data):
		#Producer produces data for consumer
		#Data comes from Twitter
		self.producer.produce(bytes(data, "ascii"))
		return True

	def on_error(self, status):
		print(status)
		return True

#Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

#Produce Data that has Game of Thrones hashtag (Tweets)
api = tweepy.API(auth)
trends1 = api.trends_place(1)
trends = set([trend['name'] for trend in trends1[0]['trends']])
file = open("trends.txt","w")
for i in trends:
	file.write(json.dumps(i).replace('"',''))
	file.write("\n")
twitter_stream.sample()
# for e in trends:
# 	print('trends: --------------    ' + e)
# 	twitter_stream.filter(track=[e])z
# 	break
