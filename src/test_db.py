import random
def make_random_text(max_len=140):
    words = ["lorem", "ipsum", "dolor", "sit", "amet,", "consectetur", "adipisicing", "elit,", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua.", "Ut", "enim", "ad", "minim", "veniam,", "quis", "nostrud", "exercitation", "ullamco", "laboris", "nisi", "ut", "aliquip", "ex", "ea", "commodo", "consequat.", "Duis", "aute", "irure", "dolor", "in", "reprehenderit", "in", "voluptate", "velit", "esse", "cillum", "dolore", "eu", "fugiat", "nulla", "pariatur.", "Excepteur", "sint", "occaecat", "cupidatat", "non", "proident,", "sunt", "in", "culpa", "qui", "officia", "deserunt", "mollit", "anim", "id", "est", "laborum"] 
    punc = ['!','?','.']

    out = []
    for i in range(random.randint(4, 12)):
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
        "coordinates": {
        [
            -75.14310264,
            40.05701649
        ]}
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
        'created_at': datetime(
            random.randint(2013,2014),
            random.randint(1,12),
            random.randint(1,28),
            random.randint(0,23),
            random.randint(0,59),
            random.randint(0,59),
            random.randint(0,59)
            ),
        'favorited': True if raqndom.randint(0,1) else False,
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
    tdict = merge_dicts(5, tweet_with_coord)

    return tdict

if __name__ == '__main__':
    # test the script
    print make_sample_tweet_dict()
