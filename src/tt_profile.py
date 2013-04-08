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
import argparse

class Tt_Profile(object):
    def add_profile(self, alias_str, purpose_str, priority_int):
        pass

    def auth_profile(self, alias_str, pin=None):
        pass

    def delete_profile(self, alias_str):
        pass

    def update_profile(self, alias_str, purpose_str, priority_int):
        pass

    def list_profiles(self):
        pass
    
    def main(self):
        parser = argparse.ArgumentParser(description='maintains TT profiles', add_help=True, epilog='For more help about a command, append the -h switch after the command.')
        parser.add_argument('-v', '--verbose', action='count', help='prints debug message')
        subparsers = parser.add_subparsers()

        subparser_add = subparsers.add_parser('add', help='adds a new profile by linking another Twitter account', add_help=True)
        subparser_add.set_defaults(command='add')
        subparser_add.add_argument('-a', '--alias', help='name of the profile', required=True)
        subparser_add.add_argument('-p', '--purpose', help='specifies the user with which this profile will be used', default='')
        subparser_add.add_argument('-r', '--priority', type=int, help='specfies the order in which this profile will be used when calling APIs (1=highest, 10=lowest)', default=5)

        subparser_auth = subparsers.add_parser('auth', help='completes account linking by providing a PIN', add_help=True)
        subparser_auth.set_defaults(command='auth')
        subparser_auth.add_argument('-a', '--alias', help='name of the profile', required=True)
        subparser_auth.add_argument('-i', '--pin', help='the PIN number provided by twitter', required=True)

        subparser_update = subparsers.add_parser('update', help='changes profile settings', add_help=True)
        subparser_update.set_defaults(command='update')
        subparser_update.add_argument('-a', '--alias', help='name of the profile', required=True)
        subparser_update.add_argument('-p', '--purpose', help='specifies the user with which this profile will be used', default=None)
        subparser_update.add_argument('-r', '--priority', type=int, help='specfies the order in which this profile will be used when calling APIs (1=highest, 10=lowest)', default=None)

        subparser_delete = subparsers.add_parser('delete', help='deletes a profile', add_help=True)
        subparser_delete.set_defaults(command='delete')
        subparser_delete.add_argument('-a', '--alias', help='name of the profile', required=True)

        subparser_list = subparsers.add_parser('list', help='lists all profiles', add_help=True)
        subparser_list.set_defaults(command='list')

        self.__args = vars(parser.parse_args())        # gives a nested dict object
        self.__verbose = self.__args['verbose']
        self.__command = self.__args['command']

        if self.__args['command'] == 'add':
            self.add_profile(alias_str=self.__args['alias'], purpose_str=self.__args['purpose'], priority_int=self.__args['priority'])

        elif self.__args['command'] == 'auth':
            self.auth_profile(alias_str=self.__args['alias'], pin=self.__args['pin'])

        elif self.__args['command'] == 'delete':
            self.auth_profile(alias_str=self.__args['alias'])

        elif self.__args['command'] == 'update':
            self.add_profile(alias_str=self.__args['alias'], purpose_str=self.__args['purpose'], priority_int=self.__args['priority'])
    
        elif self.__args['command'] == 'list':
            self.list_profiles()

    def __init__(self):
        self.__args = None
        self.main()


#
# main
#
Tt_Profile()
