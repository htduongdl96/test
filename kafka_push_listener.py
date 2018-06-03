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
from urllib3.exceptions import ProtocolError, IncompleteRead
import time
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

isRunning = False

def StartStream(trends):
	while True:
		try:
			twitter_stream.filter(track=trends,stall_warnings=True, async=True)
		except ProtocolError:
			continue
		except IncompleteRead:
			continue
		except:
			continue

#Twitter Stream Listener
class KafkaPushListener(StreamListener):
	def __init__(self):
		#localhost:9092 = Default Zookeeper Producer Host and Port Adresses
		self.client = pykafka.KafkaClient("localhost:9092")

		#Get Producer that has topic name is Twitter
		self.producer = self.client.topics[bytes("twitter", "ascii")].get_producer()

	def on_data(self, data):
		global isRunning
		isRunning = True
		#Producer produces data for consumer
		#Data comes from Twitter
		print(data)
		self.producer.produce(bytes(data, "ascii"))
		return True

	def on_error(self, status):
		global isRunning
		isRunning = False
		print(status)
		time.sleep(60)
		print("WTF")
		return True

#Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

#Produce Data that has Game of Thrones hashtag (Tweets)
api = tweepy.API(auth)
trends1 = api.trends_place(1)
trends = set([trend['name'] for trend in trends1[0]['trends']])
print(trends)
file = open("trends.txt","w")
file.flush()
file.close()
file = open("trends.txt","a+")
for i in trends:
	string = json.dumps(i).replace('"','')
	print(string)
	file.write(string)
	file.write("\n")
file.close()
StartStream(trends)

# for e in trends:
# 	print('trends: --------------    ' + e)
# 	twitter_stream.filter(track=[e])z
# 	break
