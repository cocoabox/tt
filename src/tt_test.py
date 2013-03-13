from tp import TpTimeline

t = {
        'text': 'foo-xxxxx-hello world',
        'entities': {
            'media': [ 
                {'indices': [3,9], 'text':'seg:XXXXX'},    
                {'indices': [10,15], 'url':'haallo!!'}    
            ],
            'urls': [ 
                {'indices': [0,3],
                    "url": "http://t.co/0JG5Mcq",
                    "display_url": "blog.twitter.com/2011/05/twitte",
                    "expanded_url": "http://blog.twitter.com/2011/05/twitter-for-mac-update.html" 
                }    
            ]
         }
    }

print TpTimeline._TpTimeline__process_entities(t)

exit()

# ---------------------------
from tp import TpMyself,TpObject

init_using = {'key':'', 'secret':''}

access_token_key='402106111-TiKN8VuCPCyKyDdmKzFriN0wRi6ga9uT6iFbP41K'
access_token_secret='foMOcftZ0XfTs46FXfkPGinuBSkmwyTJI5aKSYMcMjw'

#access_token_key = raw_input('access token key [blank=none]? ').strip()
if ''==access_token_key:
    # get request 
    request_tuple= TpMyself.get_request()
    print "---request---"
    print request_tuple

    pin = raw_input('pin? ').strip()
    access_token= TpMyself.get_access(request_tuple[1], pin)

    print "---access token (save)---"
    print access_token
    init_using= access_token

else:
    if access_token_secret=='':
        access_token_secret = raw_input('access token secret? ').strip()
    init_using['key']= access_token_key
    init_using['secret']= access_token_secret

print "---myself is---"
myself= TpMyself(init_using)
me_obj= myself.get_me()
print myself.get_my_name()

print "---api limit---"
print myself.get_api_limit()

print "---tweepy limit---"
print myself.get_tweepy_limit()
