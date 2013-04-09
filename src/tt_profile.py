#!env python
# manages tt profiles
import re 
from tt import Tt_ConsoleApp
from db import DProfiles
from tp import TpManager 

class Tt_Profile(Tt_ConsoleApp):
    def add_profile(self, alias_str, purpose_str, priority_int, secure_bool=True):
        profiles = DProfiles(verbose=self.verbosity>=2)
        if profiles.get(profile_alias=alias_str) is not None:
            raise Exception('a profile with the same alias already exists: %s' % alias_str)

        # request_tuple = (request_url, request_token_str)
        TpManager.set_api_credentials(self.api_credentials)
        request_tuple = TpManager.get_request(signin_with_twitter=False, secure=secure_bool)
        if not request_tuple:
            raise Exception('unable to generate request token (internet connection broken?)')
        
        profiles.insert({
                "profile_alias": alias_str,
                "auth_flag": DProfiles.FLAG_REQUESTED,
                "auth_data": request_tuple[1],
        })
        self.debug_msg(request_tuple)
        print "auth_url:%s" % request_tuple[0]
        return 0

    def auth_profile(self, alias_str, pin=None, secure_bool=True):
        pass

    def delete_profile(self, alias_str):
        pass

    def update_profile(self, alias_str, purpose_str, priority_int):
        pass

    def list_profiles(self):
        pass

    def __init__(self):
        common_args = {
                'secure': ('-s,--secure', {'help': 'requests HTTPs connection', 'required': False, 'default': True, 'action': 'store_true'}),
                'priority': ('-r,--priority', {'help': 'order in which this profile is to be used (1=use first, 10=use last)', 'required': False, 'default': 5}),
                'purpose': ('-p,--purpose', {'help': 'comma-separated list of numerical twitter user IDs', 'required': False, 'default': ''}),
                }
        super(Tt_Profile, self).__init__(description_str='maintains TT profiles', args={
                'add': {
                    'help': 'adds a new profile by linking a twitter account',
                    'args': {
                        '-a,--alias': {'help': 'name of the new profile; must be unique', 'required': True},
                        common_args['secure'][0]: common_args['secure'][1],
                        common_args['purpose'][0]: common_args['purpose'][1],
                        common_args['priority'][0]: common_args['priority'][1],
                        }
                    },
                'auth': {
                    'help': 'completes account linking',
                    'args': {
                        '-a,--alias': {'help': 'name of the profile specified during add', 'required': True},
                        '-i,--pin': {'help': 'PIN given by twitter', 'required': True},
                        common_args['secure'][0]: common_args['secure'][1],
                        }
                    },
                'update': {
                    'help': 'changes settings of a profile',
                    'args': {
                        '-a,--alias': {'help': 'name of the profile to update', 'required': True},
                        common_args['purpose'][0]: common_args['purpose'][1],
                        common_args['priority'][0]: common_args['priority'][1],
                        }
                    },
                'delete': {
                    'help': 'removes a profile (does not unlink twitter account)',
                    'args': {
                        '-a,--alias': {'help': 'name of the profile to delete', 'required': True},
                        }
                    },
                'list': {
                    'help': 'lists all profiles'
                    },
        })
       
        if self.command == 'add':
            self.add_profile(self.arg('alias'), self.arg('purpose'), self.arg('priority'), self.arg('secure', True))
        else:
            print 'UNIMPLEMENTED COMMAND: %s' % self.command
    
    

#
# main
#
Tt_Profile()
