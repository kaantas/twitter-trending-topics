import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import twitter_config
import pykafka

consumer_key = twitter_config.consumer_key
consumer_secret = twitter_config.consumer_secret
access_token = twitter_config.access_token
access_secret = twitter_config.access_secret

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

class TweetListener(StreamListener):
	def __init__(self):
		self.client = pykafka.KafkaClient("localhost:9092")
		self.producer = self.client.topics[bytes('twitter','ascii')].get_producer()

	def on_data(self, data):
		try:
			json_data = json.loads(data)
			print(json_data["text"])
			self.producer.produce(bytes(data,'ascii'))
			return True
		except KeyError:
			return True

	def on_error(self, status):
		print(status)
		return True
        
twitter_stream = Stream(auth, TweetListener())
twitter_stream.filter(languages=['tr'], track=['a', 'e', 'i', 'o', 'u'])
