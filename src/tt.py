import argparse
import sys
import os
import json
from db import DProfiles

sys.path.append('../lib/prettytable')
import prettytable

class Tt_UserError(Exception):
    def __init__(self, message):
        self.__msg = message

    @property
    def message(self):
        return self.__msg

    def __str__(self):
        return repr(self.__msg)

class Tt_ConsoleApp(object):
    def _get_DProfiles(self):
        return DProfiles(verbose=self.verbosity>=3)

    def get_access_tokens(self, profile_alias_str=None):
        """obtains a list of access tokens in order of priority; to get one token, specify
            profile_alias_str
        """
        profiles = self._get_DProfiles() 
        return profiles.get_access_tokens(profile_alias=profile_alias_str)
    
    def output(self, *args, **kwargs):
        """prints an output message, error message, or formattable output (list or dict)
            to print to stdout, * call output('spam', ...) 
            to print to stderr, * call output('spam', ..., error=True)
            * requires output_type='text' during construction
            to print formattable output, call output(my_table=[dict_row1, dict_row2, ...]),
                followed by print_output() 
        """
        if not isinstance(self.__output, dict):
            self.__output = {}
        for unnamed_arg in args:
            if self.__output_type == 'text':
                output_to = sys.stderr if 'error' in kwargs and kwargs['error'] else sys.stdout
                for stuff in args:
                    output_to.write(str(stuff))
                output_to.write('\n')
            elif self.__output_type == 'json':
                if not 'messages' in self.__output:
                    self.__output['messages'] = []
                self.__output['messages'].append(
                        args[0] if len(args) == 1 else args
                        )

        for k, v in kwargs.iteritems():
            if k in self.__output:
                self.__output[k].append(v)
            else:
                self.__output[k] = [v]

    def print_output(self, trim_empty_lists=False):
        def need_new_table(existing_cols, this_row_dict):
            for c in this_row_dict:
                if not c in existing_cols:
                    return True
            return False

        def out_table(out_str, table_instance=None):
            if table_instance is None:
                return out_str
            else:
                return '%s\n%s' % (out_str, table_instance.get_string())

        if not self.__output:
            return

        if self.__output_type == 'json':
            # tidy up output
            for k in self.__output:
                v = self.__output[k] 
                if isinstance(v, list):
                    if not len(v) and trim_empty_lists:
                        self.__output.pop(k, None)
                    if len(v) == 1:
                        self.__output[k] = v[0]
        
            sys.stdout.write(json.dumps(self.__output))
            return 

        elif self.__output_type == 'text':
            out_str = ''
            # print to stdout
            for data_name, data_rows in self.__output.iteritems():
                sys.stdout.write('%s:' % data_name)
                columns = []
                table_ins = None
                for row in data_rows:
                    if isinstance(row, list):
                        columns = row
                        out_str = out_table(out_str, table_ins)
                        table_ins = prettytable.PrettyTable(columns)
                        continue
                    elif need_new_table(columns, row):
                        columns = row.keys()
                        out_str = out_table(out_str, table_ins)
                        table_ins = prettytable.PrettyTable(columns)

                    table_row = []
                    for c in columns:
                        table_row.append(row[c] if c in row else '')
                    table_ins.add_row(table_row)
                if table_ins:
                    out_str = out_table(out_str, table_ins)
                sys.stdout.write(out_str + '\n') 
                out_str = ''

    def debug_msg(self, *args, **kwargs):
        """prints a debug message given required verbosity(default=1), e.g.
            self.debug_msg('some list:', ['a','b'], verbosity=1)
        """
        verbosity = kwargs['verbosity'] if 'verbosity' in kwargs else 1
        if verbosity <= self.__verbosity:
            sys.stdout.write('[%s] ' % self.__class__.__name__)
            for stuff in args:
                sys.stdout.write('%s ' % str(stuff))
            sys.stdout.write('\n')

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
            need_api_credentails=True, api_credentials_path=None, output_type='text'):
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
        self.__output = None
        self.__output_type = output_type
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

    @property
    def output_type(self):
        return self.__output_type



