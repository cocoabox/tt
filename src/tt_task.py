#!/usr/bin/env python
# executes a tt task 
import re 
import sys
from tt import Tt_ConsoleApp, Tt_UserError
from db import DPeople, DTweets 
from tp import TpManager, TpTimeLine 

class Tt_Task(Tt_ConsoleApp):
    def __force_user_id(self, screen_name):
        """given a numeric or textual screen name, returns numeric twitter user ID"""
        # is numeric?
        try:
            int(screen_name)
            return screen_name
        except ValueError:
            pass
        # TODO: database people: set cache expired
        # call users/show API for expired or empty entries

    def get_home_timeline(self, alias_str, timeline_owner=None, since_id=None, max_id=None, secure_bool=True):
        access_token_dict = self.get_access_token(alias_str)
        if not isinstance(access_token_dict, dict):
            raise Tt_UserError('unable to authenticate')

        timeline = TpTimeLine(access_token_dict, {'secure': secure_bool})
        self.output('done') 

    def __init__(self):
        common_args = {
                'secure': ('-s,--secure', {
                    'help': 'requests HTTPs connection',
                    'required': False,
                    'default': True,
                    'action': 'store_true'
                    }),
                'alias': ('-a,--alias', {
                    'help': 'profile to use for making the request',
                    'required': True,
                    }),
                'id': ('-i,--id': {'help': 
                    'twitter screen name or numerical twitter ID (e.g. @cocoa_box), leave blank for authenticating user', 
                    'required': False,
                    }),
                }

        super(Tt_Task, self).__init__(
                description_str='executes a TT task', 
                output_type='text',
                args={
                'timeline': {
                    'help': 'fetches timeline',
                    'args': {
                        '-s,--since': {'help': 'since tweet ID', 'required': False},
                        '-x,--max': {'help': 'max tweet ID', 'required': False},
                        common_args['id'][0]: common_args['id'][1],
                        common_args['secure'][0]: common_args['secure'][1],
                        }
                    },
                'followers': {
                    'help': 'gets list of followers',
                    'args': {
                        common_args['id'][0]: common_args['id'][1],
                        common_args['secure'][0]: common_args['secure'][1],
                        }
                    },
                }
            )
      
        try:
            if self.command == 'timeline':
                self.get_home_timeline(self.arg('alias'),
                        max_id=self.arg('max', None),
                        since_id=self.arg('since', None),
                        secure_bool=self.arg('secure', True)
                        )
            else:
                raise Tt_UserError('UNIMPLEMENTED COMMAND: %s' % self.command)
            self.print_output()
        except Tt_UserError as e:
            self.output("[Error] %s" % e.message, error=True) 


    
    

#
# main
#
Tt_Task()
