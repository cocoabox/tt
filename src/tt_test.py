from tp import TpMyself

# get request 
request_tuple= TpMyself.get_request()
print "---request---"
print request_tuple

pin = raw_input('pin? ').strip()
access_token= TpMyself.get_auth_token(request_tuple[1], pin)

print "---access token (save)---"
print access_token



