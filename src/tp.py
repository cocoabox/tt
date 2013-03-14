import sys
sys.path.append('../lib/tweepy')

import tweepy
import urllib
import time
import re
import utils
from httplib import HTTPResponse
from datetime import datetime


class TpManager(object):
    # change consumer keys and secret below; or can be overridden during get_access(), get_request() 
    consumer_key = ''
    consumer_secret = ''

    # need to patch tweepy/binder.py and add the following line:
    # -> self.api.last_request= (self.method, self.path)   <---
    #    self.api.last_response = resp
    #
    TWEEPY_HAS_LAST_REQUEST= True 

    # key-value pairs of 'profile_str':API_OBJECT call get_api_object() to get 
    _api_objects= None
    # key-value pairs of 'profile_str':{'GET api/path':INFO}; call tp_object_instance.get_tweepy_limit() to get data 
    _api_limits= None
    # key-value pairs of 'profile_str':{'tweepy_method_name':INFO}; call tp_object_instance.get_tweepy_limit() to get data 
    _tweepy_limits= None
    # tuple ('GET','api/path'). if not initialized then None
    _last_api= None

    @classmethod 
    def make_profile_str(cls, token_dict, tweepy_parser_instance=None):
        """generates a string to identify an API-end user (usually by access token)""" 
        if isinstance(token_dict, dict) and 'key' in token_dict and 'secret' in token_dict:
            return '%s|%s|%s' % (
                    token_dict['key'], 
                    token_dict['secret'], 
                    '' if tweepy_parser is None else tweepy_parser_instance.__class__.__name__
            )
        else:
            return False

    @classmethod
    def get_api_object(cls, token_dict, tweepy_parser_instance=None, api_opts=None): 
        """caches and retrieves Tweepy API object;
            tweepy_pars_instance must be an instance of tweepy.parsers.Parser
        """
        api_opts= {} if not api_opts else api_opts
        # this string should differ between each "account" you are impersonating
        profile_str= cls.make_profile_str(token_dict)
        if not profile_str:
            raise Exception('unknown token given')
        if tweepy_parser_instance is not None and not isinstance(tweepy_parser_instance,tweepy.parsers.Parser):
            raise TypeError('expecting tweepy_parser_instance to be None or tweepy.parsers.Parser()')

        if cls._api_objects is None:
            cls._api_objects= {} 
        if not profile_str in cls._api_objects:
            # create a new API object  
            auth= tweepy.OAuthHandler(cls.consumer_key, cls.consumer_secret)
            auth.set_access_token(token_dict['key'], token_dict['secret'])
            cls._api_objects[profile_str]= tweepy.API(auth_handler=auth, parser=tweepy_parser_instance, **api_opts) 

        return cls._api_objects[profile_str]

    @classmethod 
    def get_request(cls, consumer_key=None, consumer_secret=None, signin_with_twitter=False, secure=True):
        """returns request token as (request_url, request_token_obj_as_str), or False if fail"""
        # precedence: given consumer key/secret > TpManager consumer key/secret
        if consumer_key is None:
            consumer_key = cls.consumer_key
        if consumer_secret is None:
            consumer_secret = cls.consumer_secret
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.secure = secure
        try:
            request_url = auth.get_authorization_url(signin_with_twitter)
            return (request_url, auth.request_token.to_string())
        except tweepy.TweepError as e:
            return False
    
    @classmethod
    def get_access(cls, request_token, consumer_key=None, consumer_secret=None, pin_str=None, secure=True):
        """get access token; returns {key:xx,secret:xx} or False if fail"""
        
        # precedence: given consumer key/secret > TpManager consumer key/secret
        if consumer_key is None:
            consumer_key = cls.consumer_key
        if consumer_secret is None:
            consumer_secret = cls.consumer_secret
        
        # recreate the oauth request token (warning: might be expired)
        request_token_obj= None
        if isinstance(request_token, basestring):
            # str should be a urlencoded string generated from tweepy.oauth.OAuthToken.to_string()
            request_token_obj= tweepy.oauth.OAuthToken.from_string(request_token)
        elif isinstance(request_token, tweepy.oauth.OAuthToken):
            # just use the OAuthToken instance as-is
            request_token_obj= request_token
        else:
            raise TypeError('unknown request_token; expecting a string, dict, or OAuthToken')
        
        if not isinstance(request_token_obj, tweepy.oauth.OAuthToken):
            raise Exception('failed to recreate request token object')

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.secure = secure
        auth.request_token = request_token_obj
        try:
            token_obj= auth.get_access_token(verifier=pin_str)
            if token_obj == False:
                return False
            else:
                return {'key':token_obj.key, 'secret':token_obj.secret}
        except tweepy.TweepError as e:
            return False

    @classmethod
    def update_api_limits(cls, profile_str, tweepy_method_str, api_method_path_str, response_header_dict):
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
            if cls._api_limits is None:
                cls._api_limits = {}
            if not profile_str in cls._api_limits:
                    cls._api_limits[profile_str]= {}
            cls._api_limits[profile_str][api_method_path_str]= limit_info 

        # update "tweepy call" limit info 
        if cls._tweepy_limits is None:
            cls._tweepy_limits = {}
        if not profile_str in cls._tweepy_limits:
                cls._tweepy_limits[profile_str]= {}
        cls._tweepy_limits[profile_str][tweepy_method_str]= limit_info

"""TpObject is a wrapper around tweepy API objects but provides multi-profile (access_token) facilities at runtime"""
class TpObject(object):
    # number of retries and sec to sleep inbetween retries (but not upon rate-limit failures)
    RETRY_MAX = 5
    RETRY_SLEEP = 10 
    # can be 'RAW', 'MODEL', 'JSON' (default=None=Model Parser)
    TWEEPY_PARSER = None 
    # default tweepy.API() construction parameters; can be user-overridden in: __init__(..api_opts={xx})
    API_OPTS = {}

    def __set_access_token(self, token_dict):
        self.__profile_str= self.make_profile_str(token_dict)
        if False!= self.__profile_str:
            # instantiate the tweepy parser if needed
            tweepy_parser_instance = None
            if self.TWEEPY_PARSER == 'RAW':
                tweepy_parser_instance = tweepy.parsers.RawParser()
            elif self.TWEEPY_PARSER == 'MODEL':
                tweepy_parser_instance = tweepy.parsers.ModelParser()
            elif self.TWEEPY_PARSER == 'JSON':
                tweepy_parser_instance = tweepy.parsers.JSONParser() 
            else:
                raise ValueError('invalid value of TWEEPY_PARSER; expecting "RAW","MODEL","JSON"')
            self.__api= TpManager.get_api_object(token_dict, tweepy_parser_instance, api_opts=self.__api_opts)
            return True
        else:
            # token_dict seems to be invalid
            return False

    def __try_another_access_token(self):
        """go to the next access token in the list; returns False if no more tokens are available"""
        if not isinstance(self.__access_tokens, list):
            return False

        found_current_token = False

        # find current position
        for token_dict in self.__access_tokens:
            if self.make_profile_str(token_dict) == self.__profile_str:
                found_current_token = True
            else:
                if found_current_token:
                    # the next position after the current token is what we need
                    return self.__set_access_token(token_dict)

        # if we arrive at here, then we have no more tokens
        return False

    def add_token(self, tokens):
        """adds an access token to the current access_token list for subsequent consumption"""
        if isinstance(tokens, list):
            for token in tokens:
                self.add_token(token)
        elif isinstance(tokens, dict):
            # TODO: check if this token is already in the list
            self.__access_tokens.append(token)
        else:
            raise TypeError('expecting tokens to be of list or dict type')

    def __init__(self, tokens, api_opts=None):
        """initializes using one access token or multiple access tokens (in terms current one fails)"""
        self.__last_response_header= {}
        self.__api= None 
        self.__api_opts= {} if not api_opts else api_opts
        self.__access_tokens=[]
        self.add_token(tokens)
        
        # initially use the first token
        if len(self.__access_tokens > 0):
            self.__set_access_token(self.__access_tokens[0])

    def update_last_api_info(self, tweepy_method_str, api_object=None):
        api_name= None
        self.__last_tweepy_method= tweepy_method_str
        if TpManager.TWEEPY_HAS_LAST_REQUEST:
            if hasattr(api_object, 'last_request'):
                if isinstance(api_object.last_request, tuple):
                    # uninitialized api_object.last_request may not be a tuple
                    self._last_api= api_object.last_request
                    api_name='%s %s' % self._last_api
            else:
                raise NotImplementedError('tweepy API object has no attribute last_request')
            
        if api_object is not None:
            # [('header_key':'value'), (xx,yy), ...]
            headers_list= api_object.last_response.getheaders()
            self.__last_response_header= {}
            for tupl in headers_list:
                self.__last_response_header[tupl[0]] = tupl[1]
            
            # save API limiting info    
            TpManager.update_api_limits(self.__profile_str, tweepy_method_str, api_name, self.__last_response_header)

    def get_api_limit(self, api=None):
        """
            get rate-limit info {...} for a particular API e.g ("GET", "statuses/home_timeline").
            api='path/to/api' or ('HTTP_METHOD','path/to/api'); returns False if no data
            note: since the user may be using other clients, data returned here are not 100% reliable
        """
        if TpManager._api_limits is None or not self.__profile_str in TpManager._api_limits:
            return False

        if api is None:
            return TpManager._api_limits[self.__profile_str]
        else:
            if isinstance(api,tuple):
                # ste.g. GET statuses/home_timeline
                search_key= '%s %s' % (api[0],api[1])
                if search_key in TpManager._api_limits[self.__profile_str]: 
                    return TpManager._api_limits[self.__profile_str][search_key]
                else:
                    return False

            elif isinstance(api,basestring):
                for search_key in TpManager._api_limits[self.__profile_str]: 
                    regex_result= re.search(r'(OPTIONS|GET|HEAD|POST|PUT|DELETE|TRACE|CONNECT) (.*)', search_key) 
                    if regex_result.group(1) != '' and regex_result.group(2)==api:
                        return TpManager._api_limits[self.__profile_str][search_key]
                return False
            else:
                raise TypeError('expecting api to be tuple or string')

    def get_tweepy_limit(self, tweepy_method_str=None):
        """get rate-limit info {...} for a particular tweepy call; returns False if no data"""
        if TpManager._tweepy_limits is None or not self.__profile_str in TpManager._tweepy_limits:
            return False
        if tweepy_method_str is None:
            return TpManager._tweepy_limits[self.__profile_str]
        else:
            if tweepy_method_str in TpManager._tweepy_limits[self.__profile_str]: 
                return TpManager._tweepy_limits[self.__profile_str][tweepy_method_str]
            else:
                return False

    @property 
    def last_api(self):
        """returns a tuple e.g. ('GET', '/users/status.json') for last API call; False if n/a; needs TWEEPY_HAS_LAST_REQUEST"""
        return self._last_api

    @property
    def last_response_header(self):
        return self.__last_response_header

    # twitter returned 500 internal server error (server down?)
    ERROR_SERVER_DOWN = -1
    # rate limit exceeded AND quota from all provided tokens has been depleted
    ERROR_RATE_LIMIT = -2
    ERROR_BAD_CREDENTIALS = -3
    # programming error; API was called with wrong parameters; or API endpoint not found
    ERROR_BAD_CALL = -4
    ERROR_REFUSED = -5
    # entity uploaded was not processed (e.g. uploading a ZIP as a photo)
    ERROR_BAD_UPLOAD = -6
     
    def _api(self, tweepy_method_name=None, *args, **kwargs):
        """call tweepy method dynamically and save/return results. returns: 
                tweepy.API instance ... if called without parameters
                tuple (ERROR_xx, response_code, ..) ... on error
                dict or list ... if successful
        """
        if self.__api is None:
            raise Exception('no API object; need authentication')
        if tweepy_method_name is None:
            return self.__api
        else:
            give_up = False
            retry_count= 0
           
            # dynamically call the Tweepy API; if API not found; throws AttributeError 
            func= getattr(self.__api, tweepy_method_name)

            while not give_up:
                try:
                    api_result = func(*args, **kwargs)
                    # tweepy call was successful; exit loop
                    break

                except TweepyError, te:
                    # TODO: identify 500 errors and rate_limited errors
                    if te.response and te.response.status in (500, 502, 503, 504):
                        retry_count += 1
                        # failures due to "poor API call parameters" should not yield retries; only 500 errors!
                        if retry_count >= self.RETRY_MAX:
                            give_up = True
                            api_result = (self.ERROR_SERVER_DOWN, te.response.status)
                        else:
                            if self.RETRY_SLEEP:
                                sleep(self.RETRY_SLEEP)
                    elif te.response and te.response.status == 401:
                        # unauthorized; credential error 
                        give_up = True
                        api_result = (self.ERROR_BAD_CREDENTIALS, te.response.status)
                    
                    elif te.response and te.response.status == 403:
                        # refused, access not allowed
                        give_up = True
                        api_result = (self.ERROR_REFUSED, te.response.status)

                    elif te.response and te.response.status in (404, 406):
                        give_up = True
                        api_result = (self.ERROR_BAD_CALL, te.response.status)

                    elif te.response and te.response.status == 422:
                        # uploaded entity (photo) could not be processed
                        give_up = True
                        api_result = (self.ERROR_BAD_UPLOAD, te.response.status)

                    elif te.response and te.response.status in (429, 420):
                        # rate-limited; API 1.0 gives 420; API 1.1 gives 429
                        rate_limit_info = {
                                'limit': te.response.getheader('X-Rate-Limit-Limit', None),
                                'remaining': te.response.getheader('X-Rate-Limit-Remaining', None),
                                'time_reset': te.response.getheader('X-Rate-Limit-Reset', None),
                        }
           
                        # all subsequent calls will be done using this "next token"
                        if not self.__try_another_access_token():
                            # no more access tokens available
                            give_up = True
                            api_result = (self.ERROR_RATE_LIMIT, te.response.status, rate_limit_info)
                    else:
                        # unknown status code 
                        raise te
                except Exception, e:
                    raise e
            # -- end while --
                
            self.update_last_api_info(tweepy_method_name, self.__api)
            return api_result


class TpMyself(TpObject):
    # having Tweepy returned a JSON object is more efficient
    TWEEPY_PARSER= 'JSON'

    def __init__(self, tokens):
        super(TpMyself,self).__init__(tokens)

    def get_me(self):
        """ returns a JSON object representing the current user"""
        return self._api('me')


class TpTimeline(TpObject):
    TWEEPY_PARSER= 'JSON'

    def __init__(self, tokens):
        super(TpTimeline,self).__init__(tokens)

    def __process_timeline(self, timeline_list, entities_opts={}, make_time=False):
        """parse timeline for entities (requires: entities_opts=True or {...}) and make """
        if isinstance(timeline_list, list):
            for status_obj in api_result:
                if isinstance(status_obj, dict):
                    if entities_opts or isinstance(entities_opts, dict):
                        # useable: status_obj['text'], status_obj['user']['screen_name'], ...
                        entities_opts_ = entities_opts if isinstance(entities_opts, dict) else {} 
                        (html_text, xml_text) = process_entities(status_obj, entities_opts_)
                        status_obj['html_text'] = html_text
                        status_obj['xml_text'] = xml_text
                else:
                    raise TypeError('expecting a dict in status_obj') 

        return api_result

    def get_timeline(self, user_id='HOME', opts_dict={}, html_opts_dict={}):
        """get timeline as list of dict"""
        tweepy_method_name= 'home_timeline' if user_id == 'HOME' else 'user_timeline'
        # TODO: validate opts_dict
        opts_dict_= opts_dict.copy()

        # call API; hoping to get a list of Status
        api_result= self._api(tweepy_method_name, **opts_dict_)
        if isinstance(api_result, list):
            for status_obj in api_result:
                if isinstance(status_obj, dict):
                    # useable: status_obj['text'], status_obj['user']['screen_name'], ...
                    (html_text, xml_text) = self.__process_entities(status_obj, html_opts_dict)
                    status_obj['html_text'] = html_text
                    status_obj['xml_text'] = xml_text
                else:
                    raise TypeError('expecting a dict in status_obj') 

        return api_result


class TpFriends(TpObject):
    # having Tweepy returning a JSON object is more efficient
    TWEEPY_PARSER= 'JSON'

    def __init__(self, tokens):
        super(TpMyself,self).__init__(tokens)

    def get_friend_list(self):
        """ returns a JSON object representing the current user"""
        return self._api('me')
        


