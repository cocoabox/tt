#!/usr/bin/env python
# manages tt profiles
import re 
import sys
from tt import Tt_ConsoleApp, Tt_UserError
from db import DProfiles
from tp import TpManager, TpMyself 

class Tt_Profile(Tt_ConsoleApp):
    def add_profile(self, alias_str, purpose_str, priority_int, secure_bool=True):
        profiles = self._get_DProfiles() 
        if profiles.get(profile_alias=alias_str) is not None:
            raise Tt_UserError('a profile with the same alias already exists: %s' % alias_str)

        # request_tuple = (request_url, request_token_str)
        TpManager.set_api_credentials(self.api_credentials)
        request_tuple = TpManager.get_request(signin_with_twitter=False, secure=secure_bool)
        if not request_tuple:
            raise Tt_UserError('unable to generate request token (internet connection broken?)')
        
        profiles.insert({
                "profile_alias": alias_str,
                "auth_flag": DProfiles.FLAG_REQUESTED,
                "auth_data": request_tuple[1],
        })
        if self.output_type == 'text':
            this.output('authorization URL: %s' % request_tuple[0])
        else:
            this.output(authorization_url=request_tuple[0])
        return 

    def auth_profile(self, alias_str, pin=None, secure_bool=True):
        # load profile from DB
        profiles = self._get_DProfiles() 
        prof = profiles.get(profile_alias=alias_str)
        if prof is None:
            raise Tt_UserError('profile not found: %s' % alias_str)
        if prof['auth_flag'] != DProfiles.FLAG_REQUESTED:
            raise Tt_UserError('profile is not flagged as REQUESTED')
        
        request_token = prof['auth_data']
        self.debug_msg('getting access token')
        TpManager.set_api_credentials(self.api_credentials)
        
        access_token_dict = TpManager.get_access(request_token, pin_str=pin, secure=secure_bool)
        # access_token_dict = {'key':xxx, 'secret':xxx} or False
        
        if not isinstance(access_token_dict, dict):
            raise Exception('unable to authenticate')

        self.debug_msg('got access token:', access_token_dict)
        # test if we can call a simple API
        myself = TpMyself(access_token_dict, {'secure': secure_bool})
        my_info_dict = myself.get_me()
        print "myself is:", my_info_dict 

        profiles.update(profile_alias=alias_str, profile_info={
            'auth_data': access_token_dict,
            'auth_flag': DProfiles.FLAG_AUTHENTICATED,
            'user_id': my_info_dict['id'],
            })
        self.output('done') 

    def delete_profile(self, alias_str):
        profiles = self._get_DProfiles()
        if not profiles.delete(profile_alias=alias_str):
            raise Tt_UserError('unable to delete profile with alias (no such profile?): %s' % alias_str)
        self.output('done') 

    def update_profile(self, alias_str, purpose_str=None, priority_int=None):
        # load profile from DB
        profiles = self._get_DProfiles() 
        prof = profiles.get(profile_alias=alias_str)
        if prof is None:
            raise Tt_UserError('profile not found: %s' % alias_str)
        
        profile_info_dict = {}
        if purpose_str is not None:
            profile_info_dict['purpose'] = purpose_str
        if priority_int is not None:
            profile_info_dict['priority'] = priority_int

        if not profiles.update(profile_alias=alias_str, profile_info=profile_info_dict):
            raise Tt_UserError('unable to update profile with alias: %s' % alias_str)
        self.output('done') 

    def list_profiles(self):
        profiles = self._get_DProfiles()
        rows = profiles.get()
        if not rows:
            raise Tt_UserError('unable to get profiles')

        # without this line, column order will be in a mess! 
        self.output(profiles=['Profile alias', 'Authorization', 'User ID', 'Priority', 'Purpose'])

        for db_row in rows:
            status_str = 'Unknown'
            if db_row['auth_flag'] == DProfiles.FLAG_AUTHENTICATED:
                status_str = 'OK'
            elif db_row['auth_flag'] == DProfiles.FLAG_REQUESTED:
                status_str = 'Need PIN'

            # output a row
            self.output(profiles={
                'Profile alias': db_row['profile_alias'] ,
                'Authorization': status_str,
                'User ID': db_row['user_id'] if db_row['auth_flag'] == DProfiles.FLAG_AUTHENTICATED else 'n/a',
                'Priority': db_row['priority'],
                'Purpose': db_row['purpose'],
                })
            

    def __init__(self):
        common_args = {
                'secure': ('-s,--secure', {
                    'help': 'requests HTTPs connection',
                    'required': False,
                    'default': True,
                    'action': 'store_true'
                    }),
                'priority': ('-r,--priority', {
                    'help': 'order in which this profile is to be used (1=use first, 10=use last)',
                    'required': False,
                    'default': 5
                    }),
                'purpose': ('-p,--purpose', {
                    'help': 'comma-separated list of numerical twitter user IDs',
                    'required': False,
                    'default': ''
                    }),
                }
        super(Tt_Profile, self).__init__(
                description_str='maintains TT profiles', 
                output_type='text',
                args={
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
      
        try:
            if self.command == 'add':
                self.add_profile(self.arg('alias'),
                        self.arg('purpose'),
                        self.arg('priority'),
                        self.arg('secure', True)
                        )
            elif self.command == 'auth':
                self.auth_profile(self.arg('alias'),
                        self.arg('pin'), 
                        self.arg('secure')
                        )
            elif self.command == 'update':
                self.update_profile(self.arg('alias'),
                        self.arg('purpose', None),
                        self.arg('priority', None)
                        )
            elif self.command == 'list':
                self.list_profiles()
            else:
                raise Tt_UserError('UNIMPLEMENTED COMMAND: %s' % self.command)
            self.print_output()
        except Tt_UserError as e:
            self.output("[Error] %s" % e.message, error=True) 


    
    

#
# main
#
Tt_Profile()
