from tp import TpTimeline

t = {"text": "@rno Et demi!",
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
    
 
print TpTimeline._TpTimeline__process_entities(t, {
    'xml_full': False,
    'html_photo_link': 'thumb'
})

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
