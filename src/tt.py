import argparse
import sys
import os
import json

class Tt_ConsoleApp(object):
    def debug_msg(self, message_str, verbosity=1):
        """prints a debug message given required verbosity(default=1)"""
        if verbosity >= self.__verbosity:
            if isinstance(message_str, list):
                for item in message_str:
                    print item 
            else: 
                print message_str

    def __add_arguments(self, add_to, args_dict):
        if not isinstance(args_dict, dict):
            raise TypeError('expecting args_dict to be an instance of dict')

        for arg_name, arg_data in args_dict.iteritems(): 
            arg_names = arg_name.split(',')
            arg_data_ = arg_data if isinstance(arg_data, dict) else {}
            add_to.add_argument(*arg_names, **arg_data_)

    def __get_api_credentials(self):
        """loads API credentials from JSON file"""
        if not os.path.isfile(self.__api_credentials_path):
            raise Exception('directory: file %s does not exist' % self.__api_credentials_path)
        if not os.access(self.__api_credentials_path, os.R_OK):
            raise Exception('directory: file %s is inaccessible' % self.__api_credentials_path)

        txt_file = open(self.__api_credentials_path)
        txt_dict = json.load(txt_file)
        txt_file.close()
        if not 'key' in txt_dict or not 'secret' in txt_dict:
            raise Exception('unable to read consumer key and consumer secret from: %s' % self.__api_credentials_path)
        self.__api_credentials = txt_dict 

    def __init__(self, description_str=None, args=None, has_commands=None,
            need_api_credentails=True, api_credentials_path=None):
        """parses command line arguments, determine verbosity, etc

            Keyword arguments:
            description_str -- description to be printed in help screen (default None)
            args -- dict corresponding to arguments accepted. if command-enabled then {'cmd1':{'help':.., 'args': ARGS, ..}
                where ARGS is {'-s,--spam':{THIS_ARG_DEFINITION}} and THIS_ARG_DEFINITION is a dict corresponding to calling-parameters of 
                parser.add_argument(); if not command-enabled then this should be ARGS 
            has_commands -- whether commands is enabled; if None then will be auto-detected (default None)
            need_api_credentails -- whether Twitter API credentials are required (default True)
            api_credentials_path -- full-path to the JSON file containing Twitter API credentails (default None)
            """
        common_args = {
                'verbose': ('-v,--verbose', {'dest': 'verbosity_', 'action': 'count', 'help': 'prints debug message', 'default': 0})
                }
        self.__api_credentials_path = os.path.realpath(os.getcwd() + '/../var/api_credentials.json') if api_credentials_path is None else api_credentials_path
        self.__consumer_key = None
        self.__consumer_secret = None
        if need_api_credentails:
            self.__get_api_credentials()

        self.__has_commands = has_commands 
        self.__command = None
        self.__verbosity = 0
        this_app_str = os.path.basename(sys.argv[0])
        parser = argparse.ArgumentParser(description=description_str, add_help=True,
                epilog='For more help about a command, type "%s [command] --help"' % this_app_str)
        for ca_k, ca_v in common_args.iteritems():
            self.__add_arguments(parser, {ca_v[0]: ca_v[1]})

        if not isinstance(args, dict):
            raise TypErorr('expecting args to be an instance of dict')

        # detect whether we have commands
        if not isinstance(self.__has_commands, bool):
            self.__has_commands = True 
            for k, v in args.iteritems():
                if k[0] == '-':
                    self.__has_commands = False
                    break
                if 'args' in v and isinstance(v['args'], dict):
                    self.__has_commands = True
                    break

        if self.__has_commands: 
            subparsers = parser.add_subparsers()
            for k, v in args.iteritems():
                # k="cmd", v={"help"=..., "args":{..}}
                subparser = None
                if 'help' in v.keys():
                    subparser = subparsers.add_parser(k, help=v['help'], add_help=True)
                else:
                    subparser = subparsers.add_parser(k)
                subparser.set_defaults(command_=k)
                for ca_k, ca_v in common_args.iteritems():
                    self.__add_arguments(subparser, {ca_v[0]: ca_v[1]})
                if 'args' in v.keys():
                    self.__add_arguments(subparser, v['args'])
        else: 
            # has_commands == False
            self.__add_arguments(parser, args)
        
        self.__args = vars(parser.parse_args())
        self.__verbosity = self.__args['verbosity_']
        if self.__has_commands:
            self.__command = self.__args['command_']

    @property 
    def verbosity(self):
        return self.__verbosity

    @property 
    def command(self):
        return self.__command

    @property 
    def api_credentials(self):
        return self.__api_credentials
    @property 
    def args(self):
        return self.__args

    def arg(self, arg_name, default_val=None):
        if arg_name in self.__args:
            return self.__args[arg_name]
        else:
            return default_val



