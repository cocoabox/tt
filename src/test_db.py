#!/usr/bin/env python
import sys
from datetime import datetime
import random
from pprint import pprint
from db import DbTweets, DTweets_part
import utils
import timeit

def make_random_text():
    words = ["lorem", "ipsum", "dolor", "sit", "amet,", "consectetur", "adipisicing", "elit,", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua.", "Ut", "enim", "ad", "minim", "veniam,", "quis", "nostrud", "exercitation", "ullamco", "laboris", "nisi", "ut", "aliquip", "ex", "ea", "commodo", "consequat.", "Duis", "aute", "irure", "dolor", "in", "reprehenderit", "in", "voluptate", "velit", "esse", "cillum", "dolore", "eu", "fugiat", "Nonea", "pariatur.", "Excepteur", "sint", "occaecat", "cupidatat", "non", "proident,", "sunt", "in", "culpa", "qui", "officia", "deserunt", "mollit", "anim", "id", "est", "laborum"] 
    punc = ['!','?','.']

    out = []
    for i in range(random.randint(8, 15)):
        word = words[random.randint(0, len(words)-1)]
        if not i:
            word = word.title()
        out.append(word)
    return ' '.join(out) + punc[random.randint(0, len(punc)-1)]

def merge_dicts(probability=50, orig={}, *args):
    if random.randint(0,100) < probability:
        return orig
    out_list = orig.items()
    for a in args:
        if isinstance(a, dict):
            out_list += a.items()
    return dict(out_list)        

def make_sample_tweet_dict():
    tweet_with_mention = {
        "text": "@rno Et demi!",
        "entities": {
          "media": [
          ],
          "urls": [
          ],
          "user_mentions": [
            {
              "id": 22548447,
              "id_str": "22548447",
              "screen_name": "rno",
              "name": "Arnaud Meunier",
              "indices": [
                0,
                4
              ]
            }
          ],
          "hashtags": [
          ]
        }
    }
    tweet_with_media = {
     "text": "#Photos on Twitter: taking flight http://t.co/qbJx26r",
        "entities": {
          "media": [
            {
              "id": 76360760611180544,
              "id_str": "76360760611180544",
              "media_url": "http://p.twimg.com/AQ9JtQsCEAA7dEN.jpg",
              "media_url_https": "https://p.twimg.com/AQ9JtQsCEAA7dEN.jpg",
              "url": "http://t.co/qbJx26r",
              "display_url": "pic.twitter.com/qbJx26r",
              "expanded_url": "http://twitter.com/twitter/status/76360760606986241/photo/1",
              "sizes": {
                "large": {
                  "w": 700,
                  "resize": "fit",
                  "h": 466
                },
                "medium": {
                  "w": 600,
                  "resize": "fit",
                  "h": 399
                },
                "small": {
                  "w": 340,
                  "resize": "fit",
                  "h": 226
                },
                "thumb": {
                  "w": 150,
                  "resize": "crop",
                  "h": 150
                }
              },
              "type": "photo",
              "indices": [
                34,
                53
              ]
            }
          ],
          "urls": [
          ],
          "user_mentions": [
          ],
          "hashtags": [
          ]
        }
     
    }

    tweet_with_coord = {
        "coordinates": 
            [
                -75.14310264,
                40.05701649
            ]
        ,
        "type":"Point"
    }

    tweet_is_reply = {
        'in_reply_to_screen_name': 'twitterapi',
        'in_reply_to_status_id': 114749583439036416,
        'in_reply_to_status_id_str': '114749583439036416',
        'in_reply_to_user_id': 819797, 
    }
    tweet_was_retweeted = {
        "current_user_retweet": {
          "id": 26815871309,
          "id_str": "26815871309"
        },
        "retweeted": True,
    }    
    id = random.randint(100000000, sys.maxint-1)
    tdict = {
        'retweeted': False,
        'favorite_count': None,    
        'user': {"statuses_count":3080, "favourites_count":22, "protected":False, "profile_text_color":"437792", "profile_image_url":"...", "name":"Twitter API", "profile_sidebar_fill_color":"a9d9f1", "listed_count":9252, "following":True, "profile_background_tile":False, "utc_offset":-28800, "description":"The Real Twitter API. I tweet about API changes, service issues and happily answer questions about Twitter and our API. Don't get an answer? It's on my website.", "location":"San Francisco, CA", "contributors_enabled":True, "verified":True, "profile_link_color":"0094C2", "followers_count":665829, "url":"http:\/\/dev.twitter.com", "default_profile":False, "profile_sidebar_border_color":"0094C2", "screen_name":"twitterapi", "default_profile_image":False, "notifications":False, "display_url":None, "show_all_inline_media":False, "geo_enabled":True, "profile_use_background_image":True, "friends_count":32, "id_str":"6253282", "entities":{"hashtags":[], "urls":[], "user_mentions":[]}, "expanded_url":None, "is_translator":False, "lang":"en", "time_zone":"Pacific Time (US &amp; Canada)", "created_at":"Wed May 23 06:01:13 +0000 2007", "profile_background_color":"e8f2f7", "id":6253282, "follow_request_sent":False, "profile_background_image_url_https":"...", "profile_background_image_url":"...", "profile_image_url_https":"..."},
        'created_at': datetime(
            random.randint(2013,2014),
            random.randint(1,12),
            random.randint(1,28),
            random.randint(0,23),
            random.randint(0,59),
            random.randint(0,59),
            random.randint(0,59)
            ),
        'favorited': True if random.randint(0,1) else False,
        'id': id,
        'id_str': str(id),
        'retweet_count': random.randint(0,100),
        'text': make_random_text(),
        "source":"""
            <a href="http://itunes.apple.com/us/app/twitter/id409789998?mt=12" rel="nofollow">Twitter for Mac</a>
        """.strip(),
    }

    tdict = merge_dicts(30, tdict, tweet_with_mention)  
    tdict = merge_dicts(20, tdict, tweet_with_media)  
    tdict = merge_dicts(20, tdict, tweet_is_reply)  
    tdict = merge_dicts(10, tdict, tweet_was_retweeted)
    tdict = merge_dicts(5, tdict, tweet_with_coord)

    return tdict


def collect_user_info_from_tweet(tweet_obj):
    """salvage useable user info from a tweet (mentions, user);
        returns dict {'user_id':{'prop1':xx, ...}, ...}
        """
    users_collected = {tweet_obj['user']['id']: tweet_obj['user']}
    for user_mentioned in tweet_obj['entities']['user_mentions']:
        users_collected[user_mentioned['id']] = {
                'id': user_mentioned['id'],
                'screen_name': user_mentioned['screen_name'],
                'name': user_mentioned['name'],
                }
    return users_collected

def make_dtweet_item(tweet_obj,
        timeline_type=DTweets_part.TIMELINE_HOME,
        timeline_owner=None):
    """from a Tweepy-returned dict, create a DTweets_part-friendly row"""
    (html_text, xml_text) = utils.process_entities(tweet_obj)
    return {
            'tweet_id': tweet_obj['id'],
            'timeline_type': timeline_type,
            'timeline_owner': timeline_owner,
            'plain_text': tweet_obj['text'],
            'html_text': html_text,
            'xml_text': xml_text,
            'coordinates': tweet_obj['coordinates'] if 'coordinates' in tweet_obj else None,
            'date': tweet_obj['created_at'],
            'in_reply_to_tweet': tweet_obj['in_reply_to_status_id'] if 'in_reply_to_status_id' in tweet_obj else None,
            'in_reply_to_user': tweet_obj['in_reply_to_user_id'] if 'in_reply_to_user_id' in tweet_obj else None,
            'user': tweet_obj['user']['id'],
            'is_retweet': 1 if tweet_obj['retweeted'] else 0,
            'source': tweet_obj['source'],
            'retweeted_count': tweet_obj['retweet_count'],
            'fav_count': tweet_obj['favorite_count'],
            'is_my_fav': tweet_obj['favorited'],
            }

def add_tweets_to(dbtweet_obj, count=20):
    lst = []
    for i in range(count):
        sys.stdout.flush()
        lst.append(make_dtweet_item(make_sample_tweet_dict())) 
    t.insert(lst)
    sys.stdout.write('...round done\n')    
if __name__ == '__main__':

    t = DbTweets(directory='/tmp/tt/', verbose=False)
    
    times = timeit.repeat(lambda:add_tweets_to(t, 10), number=1, repeat=2000)
    print "done!"

    for i in times:
        print i
