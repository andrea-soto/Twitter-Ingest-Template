from itertools import ifilter
from requests_oauthlib import OAuth1Session
from requests.exceptions import ChunkedEncodingError
import json
import time
import datetime

class Twitter_Source:
  """Ingest data from Twitter based on search terms"""

  def __init__(self, config):
    self.config = config
    self.update_items = []

  def __iter__(self):
    # Search terms 
    if 'track' in self.config:
      self.track = self.config['track']
    else:
      self.track = 'ski,surf,board'

    auth = OAuth1Session(
      self.config['consumer_key'], 
      client_secret = self.config['consumer_secret'],
      resource_owner_key = self.config['access_token'],
      resource_owner_secret = self.config['access_token_secret']
    )

    request = auth.post(
      'https://stream.twitter.com/1.1/statuses/filter.json',
      data = 'track=' + self.track,
      stream = True
    )

    # filter out empty lines sent to keep the stream alive
    self.source_iterator = ifilter(lambda x: x, request.iter_lines())

    return self

  def next(self):
    # Returns the next tweet
    while True:
      try:
        next_tweet = json.loads(self.source_iterator.next())
        filtered_tweet = self.processTweet(next_tweet)
        if filtered_tweet != {}:
          break
      # could put more error handling in here to handle HTTP errors and
      # disconnection errors
      except ChunkedEncodingError:
        print('Chunked Encoding Error')
        self.__iter__()
        continue
    return filtered_tweet      

  def processTweet(self, tweet):
    """ Processes tweet   """
    # Initialize variables    
    f_tweet = {}
    f_tweet['id'] = tweet['id_str']
    f_tweet['original_text'] = tweet['text']
    f_tweet['created_at'] = time.strftime('%Y-%m-%dT%H:%M:%S',
      time.strptime(str(tweet['created_at']),'%a %b %d %H:%M:%S +0000 %Y'))

    return f_tweet
