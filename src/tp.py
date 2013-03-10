import sys
sys.path.append('../lib/tweepy')

import tweepy

class TpObject(object):
    CONSUMER_KEY=''
    CONSUMER_SECRET=''

    def __init__(self, consumer_key=None, consumer_secret=None, access_key=None, access_secret=None):
        self.__api= None
        if consumer_key is not None:
            consumer_key= self.CONSUMER_KEY
        if consumer_secret is not None:
            consumer_secret= self.CONSUMER_SECRET
       
        if access_key is not None:
            auth= tweepy.auth.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_key, access_secret)
            self.__api= tweepy.API(auth)
    
    def is_api_available(self):
        return self.__api is not None

    def get_auth_url(self):
        if self.is_api_available:
            return False

