from tp import TpTimeline
import utils

# ---------------------------
"""
test_tweet_media = {
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

test_tweet_mention = {
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

print process_entities(test_tweet_mention, {
    'xml_full': False,
    'html_photo_link': 'link'
})

exit()
"""
# ---------------------------
import os
from tp import TpManager,TpTimeline
try:
    import simplejson as json
except:
    import json

def load_key_secret(what, which_pair=None):
    filename = '../private/tt_test_%s.json' % what
    if not os.path.isfile(filename):
        return ('', '')
    txt_file = open(filename)
    dict= json.load(txt_file) 

    key=''
    if not 'key' in dict:
        # need to select a pair
        if not which_pair:
            key = dict.keys()[0]
        else:
            if which_pair in dict:
                key = which_pair
            else:
                key = dict.keys()[0]
        dict = dict[key]

    txt_file.close()
    return (dict['key'], dict['secret'])

def save_access(key, secret):
    txt_file = open('../private/tt_test_access.json', 'w')
    txt_file.write(
        json.dumps({'key':key,'secret':secret}, sort_keys=True, indent=4)
    )
    txt_file.close()

# --- main ---
want_secure = True 

(access_token_key, access_token_secret) = load_key_secret('access')
(consumer_key, consumer_secret) = load_key_secret('consumer')

print "consumer key: ", consumer_key
if consumer_key:
    TpManager.consumer_key = consumer_key
    TpManager.consumer_secret = consumer_secret
else:
    print "no consumer key"
    exit(1)

inp = raw_input('access token key [blank=%s]? ' % (access_token_key if access_token_key else 'none')).strip()
if inp:
    access_token_key = inp
    while not access_token_secret:
        access_token_secret = raw_input('access token secret? ').strip()

if not access_token_key:
    request_tuple= TpManager.get_request(consumer_key, consumer_secret, secure=want_secure)
    if not request_tuple:
        print "ERROR: failed to request" 
        exit(1) 
    print "---request---"
    print request_tuple

    pin = ''
    while not pin:
        pin = raw_input('pin? ').strip()

    access_token = TpManager.get_access(request_tuple[1], consumer_key, consumer_secret, pin_str=pin, secure=want_secure)

    print "---access token (save)---"
    print access_token

else:
    if access_token_secret=='':
        access_token_secret = raw_input('access token secret? ').strip()
    access_token= {'key': access_token_key, 'secret': access_token_secret}

save_access(access_token['key'], access_token['secret'])


api_opts = { 'secure': want_secure }
tl = TpTimeline(access_token, api_opts)

tl_list = tl.get_my_mentions()
#tl_list = tl.get_timeline('raise_wave')
if isinstance(tl_list, list):
    print "---timeline---"
    for tweet_dict in tl_list:
        print tweet_dict['html_text']
        print "---"
else:
    print "---error---"
    print tl_list

print "---limit---"
print tl.get_tweepy_limit()
