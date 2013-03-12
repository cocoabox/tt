import sys
sys.path.append('../lib/tweepy')

import tweepy
import urllib
import time
import re


class TpObject(object):
    # need to patch tweepy/binder.py and add the following line:
    #
    #    self.api.last_request= (self.method, self.path)   <---
    #    self.api.last_response = resp
    #
    TWEEPY_HAS_LAST_REQUEST= True 

    # change consumer keys and secret below; or can be overridden in descendant classes
    consumer_key= 'IQKbtAYlXLripLGPWd0HUA'
    consumer_secret= 'GgDYlkSvaPxGxC4X8liwpUoqKwwr3lCADbz8A7ADU'

    # class variables
    _api_objects= None
    _api_limits= None
    _tweepy_limits= None
    _last_api= None

    @classmethod 
    def __make_profile_str(cls, token_dict):
        """generates a string to identify an API-end user (usually by access token)""" 
        if token_dict.__class__.__name__=='dict' and 'key' in token_dict and 'secret' in token_dict:
            return '%s,%s' % (token_dict['key'], token_dict['secret'])
        else:
            return False

    @classmethod
    def __get_api_object(cls, token_dict):
        """caches and retrieves Tweepy API object"""
        # this string should differ between each "account" you are impersonating
        profile_str= TpObject.__make_profile_str(token_dict)
        if not profile_str:
            raise Exception('unknown token given')

        if TpObject._api_objects is None:
            TpObject._api_objects= {} 
        if not profile_str in TpObject._api_objects:
            # create a new API object  
            auth= tweepy.OAuthHandler(cls.consumer_key, cls.consumer_secret)
            auth.set_access_token(token_dict['key'], token_dict['secret'])
            TpObject._api_objects[profile_str]= tweepy.API(auth)

        return TpObject._api_objects[profile_str]

    @classmethod 
    def get_request(cls, signin_with_twitter=False):
        """returns request token as (request_url, request_token_obj_as_str)"""
        auth= tweepy.OAuthHandler(cls.consumer_key, cls.consumer_secret)
        request_url= auth.get_authorization_url(signin_with_twitter)
        return (request_url, auth.request_token.to_string())
    
    @classmethod
    def get_access(cls, request_token, pin_str=None):
        """get access token; returns {key:xx,secret:xx} or False if fail"""
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
                return {'key':token_obj.key, 'secret':token_obj.secret}
        except:
            return False
  
    def __init__(self, token_dict):
        self.__last_response_header= {}
        self.__api= None 

        self.__profile_str= self.__make_profile_str(token_dict)
        if False!= self.__profile_str:
            self.__api= self.__get_api_object(token_dict)


    def __update_last_api_info(self, tweepy_method_str, api_object=None):
        api_name= None
        self.__last_tweepy_method= tweepy_method_str
        if TpObject.TWEEPY_HAS_LAST_REQUEST and hasattr(api_object, 'last_request'):
            self._last_api= api_object.last_request
            # save API limiting info    
            api_name='%s %s' % self._last_api
            
        if api_object is not None:
            # [('header_key':'value'), (xx,yy), ...]
            headers_list= api_object.last_response.getheaders()
            self.__last_response_header= {}
            for tupl in headers_list:
                self.__last_response_header[tupl[0]] = tupl[1]
            
            # save API limiting info    
            TpObject.__update_api_limits(self.__profile_str, tweepy_method_str, api_name, self.__last_response_header)

    @classmethod
    def __update_api_limits(cls, profile_str, tweepy_method_str, api_method_path_str, response_header_dict):
        if 'x-rate-limit-remaining' in response_header_dict and 'x-rate-limit-limit' in response_header_dict and 'x-rate-limit-reset' in response_header_dict:
            limit_info = {
                'remaining': int(response_header_dict['x-rate-limit-remaining']),
                'limit': int(response_header_dict['x-rate-limit-limit']),
                'reset_time': int(response_header_dict['x-rate-limit-reset']),
                'call_time': int(time.time()) 
            }
        else:
            # -1 indiciates there was no rate-limiting info in response header
            limit_info = {
                'remaining': -1,
                'limit': -1,
                'reset_time': -1,
                'call_time': int(time.time()) 
            }

        # update api limit info
        if api_method_path_str is not None:
            if TpObject._api_limits is None:
                TpObject._api_limits = {}
            if not profile_str in TpObject._api_limits:
                    TpObject._api_limits[profile_str]= {}
            TpObject._api_limits[profile_str][api_method_path_str]= limit_info 

        # update "tweepy call" limit info 
        if TpObject._tweepy_limits is None:
            TpObject._tweepy_limits = {}
        if not profile_str in TpObject._tweepy_limits:
                TpObject._tweepy_limits[profile_str]= {}
        TpObject._tweepy_limits[profile_str][tweepy_method_str]= limit_info

    def get_api_limit(self, api=None):
        """
            get rate-limit info {...} for a particular API e.g ("GET", "statuses/home_timeline").
            api='path/to/api' or ('HTTP_METHOD','path/to/api'); returns False if no data
            note: since the user may be using other clients, data returned here are not 100% reliable
        """
        if TpObject._api_limits is None or not self.__profile_str in TpObject._api_limits:
            return False

        if api is None:
            return TpObject._api_limits[self.__profile_str]
        else:
            if isinstance(api,tuple):
                # e.g. GET statuses/home_timeline
                search_key= '%s %s' % (api[0],api[1])
                if search_key in TpObject._api_limits[self.__profile_str]: 
                    return TpObject._api_limits[self.__profile_str][search_key]
                else:
                    return False

            elif isinstance(api,basestring):
                for search_key in TpObject._api_limits[self.__profile_str]: 
                    regex_result= re.search(r'(OPTIONS|GET|HEAD|POST|PUT|DELETE|TRACE|CONNECT) (.*)', search_key) 
                    if regex_result.group(1) != '' and regex_result.group(2)==api:
                        return TpObject._api_limits[self.__profile_str][search_key]
                return False
            else:
                raise TypeError('expecting api to be tuple or string')

    def get_tweepy_limit(self, tweepy_method_str=None):
        """get rate-limit info {...} for a particular tweepy call; returns False if no data"""
        if TpObject._tweepy_limits is None or not self.__profile_str in TpObject._tweepy_limits:
            return False
        if tweepy_method_str is None:
            return TpObject._tweepy_limits[self.__profile_str]
        else:
            if tweepy_method_str in TpObject._tweepy_limits[self.__profile_str]: 
                return TpObject._tweepy_limits[self.__profile_str][tweepy_method_str]
            else:
                return False

    @property 
    def last_api(self):
        """
            returns a tuple e.g. ('GET', '/users/status.json') for last API call; False if data is not available
            needs TWEEPY_HAS_LAST_REQUEST
        """
        return self._last_api

    @property
    def last_response_header(self):
        return self.__last_response_header

    def _api(self, tweepy_method_name=None, api_params_list=None, api_params_dict=None):
        """call tweepy objects dynamically and save/return results"""
        if self.__api is None:
            raise Exception('no API object; need authentication')
        if tweepy_method_name is None:
            return self.__api
        else:
            # dynamically call the Tweepy API; if API not found; throws AttributeError 
            func= getattr(self.__api, tweepy_method_name)
            if isinstance(api_params_dict,dict):
                if isinstance(api_params_list,list):
                    api_result= func(*api_params_list, **api_params_dict)
                else:
                    # only dict given; list is null
                    api_result= func(**api_params_dict)
            elif isinstance(api_params_list,list):
                api_result= func(*api_params_list)
            else:
                # neither dict nor list given
                api_result= func()
            
            self.__update_last_api_info(tweepy_method_name, self.__api)
            return api_result


class TpMyself(TpObject):
    def __init__(self, token_dict):
        super(TpMyself,self).__init__(token_dict)

    def get_me(self):
        return self._api('me')

    def get_my_name(self):
        return self._api('me').name
