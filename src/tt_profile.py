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
from tt import TtArgParser 

def add_profile(alias_str, purpose_str, priority_int):
    pass

def auth_profile(alias_str, pin=None):
    pass

def delete_profile(alias_str):
    pass

def update_profile(alias_str, purpose_str, priority_int):
    pass

def list_profiles():
    pass


#
# -- main --
#
ttap = TtArgParser(definitions={    
        'add': [
            ('<profile_alias>   a unique name to identify your profile', str),
            ('[--purpose,-p]', str), 
            ('[--priority,-r=nnn] relative order in which this profile will be used', int, 1),
        ],
        'auth': [
            ('<profile_alias>', str),
            ('[--pin,-i]', int, 'verification code given by Twitter. Omit if you don\'t have one', None),
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
    }, default_action='list')

ttap.parse()

if ttap['action'] == 'add':
    add_profile(ttap['profile_alias'], ttap['purpose'], ttap['priority'])

elif ttap['action'] == 'auth':
    auth_profile(ttap['pin'])

elif ttap['action'] == 'delete':
    delete_profile(ttap['profile_alias'])

elif ttap['action'] == 'list':
    list_profiles()
   
