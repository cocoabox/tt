import sys
sys.path.append('../lib/tweepy')

import tweepy
import urllib

class TpObject(object):
    # change consumer keys and secret below
    consumer_key= ''
    consumer_secret= ''
    api_objects= None
 
    @classmethod
    def __get_api_object(cls, token_dict):
        """caches and retrieves Tweepy API object"""
        if not 'key' in token or not 'secret' in token:
            raise ValueError('invalid token; expecting {key:xx, secret:xx}')
        
        dict_key='%s,%s' % (token.key, token.secret)
        if cls.api_objects is None:
            cls.api_objects= []
        if not dict_key in cls.api_objects:
            # create a new API object  
            auth= tweepy.OAuthHandler(cls.consumer_key, cls.consumer_secret)
            auth.set_access_token(token.key, token.secret)
            cls.api_objects[dict_key]= tweepy.API(auth)
        return cls.api_objects[dict_key]

    @classmethod 
    def get_request(cls, signin_with_twitter=False):
        """returns (request_url, request_token_obj, request_token_obj_as_str); request token is needed for PIN entry"""
        auth= tweepy.OAuthHandler(cls.consumer_key, cls.consumer_secret)
        request_url= auth.get_authorization_url(signin_with_twitter)
        return (request_url, auth.request_token, auth.request_token.to_string())
    
    @classmethod
    def get_auth_token(cls, request_token=None, pin_str=None):
        """
            authenticate using a PIN and previously-generated request_token.
            returns ({key:xx,secret:xx}, token_instance, token_instance_as_str), or False if fail
        """
        # recreate the oauth request token (warning: might be expired)
        request_token_obj= None
        if isinstance(request_token, str):
            # str should be a urlencoded string generated from tweepy.oauth.OAuthToken.to_string()
            request_token_obj= tweepy.oauth.OAuthToken.from_string(request_token)
        elif isinstance(request_token, tweepy.oauth.OAuthToken):
            # just use the OAuthToken instance as-is
            request_token_obj= request_token
        else:
            raise TypeError('unknown request_token; expecting a string, dict, or OAuthToken')
        
        if not isinstance(request_token_obj, tweepy.oauth.OAuthToken):
            raise Exception('failed to recreate request token object')

        auth= tweepy.OAuthHandler(cls.consumer_key, cls.consumer_secret)
        auth.request_token= request_token_obj
        try:
            token_obj= auth.get_access_token(verifier=pin_str)
            if token_obj == False:
                return False
            else:
                # return a tuple
                token_dict= {'key':token_obj.key, 'secret':token_obj.secret}
                return (token_dict, token_obj, token_obj.to_string())
        except:
            return False
  
    def __init__(self, token_dict):
        if not 'key' in token_dict or not 'secret' in token_dict:
            raise ValueError('invalid token_dict; expecting {key:xx, secret:xx}')
        self.__api= self.__get_api_object(token_dict)

    @property
    def api(self):
        if self.__api is None:
            raise Exception('no API object; need authentication')
        else:
            return self.__api


class TpMyself(TpObject):
    def __init__(self, token):
        super(TpMyself,self).__init__(token)

    def get_my_name(self):
        return self.api().name
