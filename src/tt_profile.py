#!env python
#
# add, delete, or update tt profiles  
# usage:
#   tt_profile.py [add|auth|delete|update|list] <profile_alias> ...
# 
# adding a profile:
#   tt_profile.py add <profile_alias> [--purpose=..] [--priority=NN] 
# 
# to authorize a Twitter account to a profile:
#   tt_profile.py auth <profile_alias> 
#       returns 0 if current authentication data is valid; 1 otherwise (will print auth URL to stdout)
#   tt_profile.py auth <profile_alias> --pin=NN 
#       returns 0 if authorization is complete; 1 otherwise
#
# delete a profile:
#   tt_profile.py delete <profile_alias>
#
# updating a profile:
#   tt_profile.py update <profile_alias> [--purpose=..] [--priority=NN] 

import sys
import re 
import pprint
from tt import TtConsoleApp 

class TtProfile(TtConsoleApp):
    param_criteria = {
        'add': [
            ('<profile_alias>', str),
            ('[--purpose,-p]', str), 
            ('[--priority,-r]', int),
        ],
        'auth': [
            ('<profile_alias>', str),
            ('--pin,-i', int),
        ],
        'delete': [
            ('<profile_alias>', str),
        ],
        'update': [
            ('<profile_alias>', str),
            ('[--purpose,-p]', str),
            ('[--priority,-r]', int),
        ],
        'list': [],
    }
    
    default_action = 'list'

    def __init__(self):
        super(TtProfile, self).__init__()
        
        print self.get_parameters()


TtProfile()
