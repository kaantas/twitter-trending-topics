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
		self.producer = self.client.topics[bytes('twitter','utf-8')].get_producer()

	def on_data(self, data):
		try:
			json_data = json.loads(data)
			words = json_data['text'].split(" ")
			ls = list(filter(lambda x: x.lower().startswith('#'), words))
			if(len(ls)!=0):
				print("\n".join(map(str,ls)))
				for word in ls:				
					self.producer.produce(bytes(word,'utf-8'))
				#self.producer.produce(bytes(json_data["text"],'utf-8'))
			return True
		except KeyError:
			return True

	def on_error(self, status):
		print(status)
		return True
        
twitter_stream = Stream(auth, TweetListener())
twitter_stream.filter(languages=['tr'], track=['a', 'e', 'i', 'o', 'u'])
