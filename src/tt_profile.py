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

class TtProfile(object):
    arguments = {
        'add': [
            # valid specifiers are:
            #   [spam]      optional
            #   <spam>      required
            #
            # optionally, specify how many times an argument should appear
            #   [spam]x3    for positional args: should appear 3 times in a row; 
            #               for parametric args: can appear three times max, excess are discarded
            #   [spam]...   for positional args: appears N times until next argument hits
            #               for parametric args: can appear as many times as it appears  
            #
            # "spam" above should be 
            #   --spam,-s   accepts either --spam="HAM", -s"HAM", --spam "HAM", -s "HAM"     
            #   --spam      accepts only --spam="HAM", --spam "HAM"
            #   -s          accepts only -s"HAM", -s "HAM"
            #
            # definitions:
            #   ("[spam]", str)       positional argument. see notes below
            #   ('[--spam]', str)     accepts --spam="HAM"
            #
            # notes on positional arguments
            #   PA PA .. pp pp pp ... PA    is OK
            #   PA PA .. pp PA pp ... PA    is unacceptable since it's impossible to determine the position of the indiciated element
            #               ^^
            ('<profile_alias>', str),
            ('[--purpose,-p]', str),
            ('[--priority,-r]', int),
            ('<needed>', int),
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
        '__default_action': 'list'
    }
    def __init__(self):
        self.parse_args()
    
    def parse_args(self):
        if not isinstance(self.arguments, dict):
            raise TypeError('expecting arguments to be of type dict')
        default_action = None if not '__default_action' in self.arguments else self.arguments['__default_action']
        action = sys.argv[1] 

        if action != '__default_action' and action in self.arguments:
            # prepare argument specifications
            spec_list = []
            regex = re.compile('^(\[|<)(\-\-([A-Z,a-z,0-9,_]+)|\-([A-Z,a-z,0-9])|\-\-([A-Z,a-z,0-9,_]+)\,\-([A-Z,a-z,0-9])|([A-Z,a-z,0-9,_]+))(\]|>)([\.]{3}|x([0-9]+))?$')
            for criteria in self.arguments[action]:
                if isinstance(criteria, tuple):
                    matches = regex.search(criteria[0])
                    if not matches:
                        raise ValueError(criteria[0]+': invalid specification; expecting "[--spam,-s]", "<--required-r>", etc') 
                    m = matches.groups()
                    is_required = m[0] == '<' and m[7] == '>'
                    positional_name = m[6]
                    is_positional = True if m[6] else False
                    long_name = m[2] if not m[4] else m[4]
                    short_name = m[3] if not m[5] else m[5]
                    if m[8] == '...':
                        cnt = '...'
                    elif m[9]:
                        cnt= int(m[8])
                    else:
                        cnt = None
                    spec_list.append({
                        'is_positional': is_positional,
                        'positional_name': positional_name,
                        'long': long_name,
                        'short': short_name,
                        'count': cnt,
                        'type': criteria[1], 
                    })
                else:
                    raise TypeError('expecting a tuple')

            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(spec_list)

            # prepare positional elements
            posi_begin_list = []
            posi_end_list = []
            para_list = []
            
            for spec in spec_list:
                if spec['is_positional']:
                    if not len(para_list):
                        posi_begin_list.append(spec)
                    else:
                        posi_end_list.append(spec)
                else:
                    if len(posi_end_list):
                        raise Exception('expecting positional arguments to be in the front and end of the list only')
                    else:
                        para_list.append(spec)
            
            # process each argv
            params = []
            pob_idx = 0
            poe_idx = 0
            par_idx = 0
            arg_idx = 1
            
            def get_arg(idx):
                if idx >= len(sys.argv):
                    return None
                else:
                    return (sys.argv[idx], idx+1)

            def get_parametric_arg(arg_idx):
                """returns (spam, HAM, new_idx); if no match then (None, None, arg_idx); if no more args then None"""
                key_str = val_str = None
                # e.g. --spam="HAM" (+1)     --spam "HAM" (+2)      -p"HAM" (+1)     -p "HAM" (+2)
                (arg, arg_idx) = get_arg(arg_idx)
                if not arg:
                    return None
                regex = re.compile('^\-\-([A-Z,a-z,0-9,_])([=]?(.*))?$') 
                matches = regex.search(arg)
                if matches is not None:
                    # --spam=HAM or --spam
                    key_str = matches[0]
                    val_str = matches[2]
                    if val_str is None:
                        (val_str, arg_idx) = get_arg(new_idx)
                    return (key_str, val_str, arg_idx)

                regex_short = re.compile('^\-([A-Z,a-z,0-9])(.*)$') 
                matches = regex_short.search(arg)
                if matches is not None:
                    # -s"HAM" or -s 
                    new_idx += 1
                    key_str = matches[0]
                    val_str = matches[1]
                    if val_str is None:
                        new_idx += 1 
                        val_str = get_arg(new_idx)
                    return (key_str, val_str, new_idx)
                
                # nothing matched
                return (None, None, arg_idx)

            # begin parsing here
            arg_idx = 1
            params = []
            for spec in posi_begin_list:
                this_param = None
                if isinstance(spec['count'], int):
                    rep_count = spec['count']
                    this_param = []
                elif spec['count'] == '...':
                    rep_count = -1      # endless
                    this_param = []
                else:
                    rep_count = 1
                while rep_count:
                    (arg_val, arg_idx) = get_arg(arg_idx)
                    if isinstance(this_param, list):
                        this_param.append(arg_val)
                    else:
                        this_param = arg_val
                    # look at next args; if we have no more args (or next arg is --spam) then exit
                    arg_data = get_parametric_arg(arg_idx)
                    if arg_data is None or (isinstance(arg_data, tuple) and arg_data[0] is not None):
                        break
                    if isinstance(rep_count, int):
                       rep_count -= 1

                params.append(this_param)       
                print params    


























TtProfile()


