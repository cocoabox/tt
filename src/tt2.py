import re
import sys
import pprint

def pp(what):
    pp = pprint.PrettyPrinter(indent=4) 
    pp.pprint(what)

class CArgDefinitionError(Exception):
    def __init__(self, param_def, message=None):
        self.__param_def = param_def
        super(CArgDefinitionError, self).__init__(self, message)

    @property
    def param_def(self):
        return self.__parse_param_def


class CPositionalArgRequired(CArgDefinitionError):
    def __init__(self, param_def):
        super(CPositionalArgRequired, self).__init__(
                param_def, message='positional argument %s must be required (use <> instead of [])' % param_def['out_key'] 
        )


class CPositionalArgMustBeFinite(CArgDefinitionError):
    def __init__(self, param_def):
        super(CPositionalArgRequired, self).__init__(
                param_def, message='positional argument %s must repeat finitely (use xNN instead of ...)' % param_def['out_key'] 
        )


class CArgRuntimeError(Exception):
    def __init__(self, param_def, argv_item, message=None):
        self.__param_def = param_def
        self.__argv_item = argv_item 
        super(CArgRuntimeError, self).__init__(self, message)

    @property
    def param_def(self):
        return self.__parse_param_def
    
    @property
    def argv_item(self):
        return self.__argv_item


class CArgRequiredButMissing(CArgRuntimeError):
    def __init__(self, param_def):
        super(CArgRequiredButMissing, self).__init__(
                param_def,
                None,
                'parameter %s is required but not supplied' % param_def['out_key']
        )


class CArgNotEnoughRepeats(CArgRuntimeError):
    def __init__(self, param_def, supplied_list=None):
        num_repeat_str = ''
        if param_def['count'] == '...':
            num_repeat_str = '(infinite number of items)'
        elif isinstance(param_def['count'], num):
            num_repeat_str = '%d items' % param_def['count']
        else:
            num_repeat_str = '1 item'

        super(CArgNotEnoughRepeats, self).__init__(
                param_def,
                None,
                'parameter %s requires %s but only %d were supplied' % (
                    param_def['out_key'],
                    num_repeat_str,
                    len(supplied_list) if supplied_list else 'None',
                )
        )
        self.__supplied_list = supplied_list
    
    def supplied_list(self):
        return self.__supplied_list

    def supplied_count(self):
        return len(self.__supplied_list) if isinstance(self.__supplied_list, list) else -1


class CArgParser(object):
    def __init__(self, app_title_str=None, app_purpose_str=None, param_def=None, default_action=None):
        self.__app_title = app_title_str
        self.__app_purpose = app_purpose_str
        self.__param_def = param_def 
        self.__default_action = default_action
    
    def __find_param_def(self, param_defs, long_name=None, short_name=None):
        for arg_info in param_defs:
            if long_name is not None and arg_info['long_name'] == long_name:
                return arg_info
            if short_name is not None and arg_info['short_name'] == short_name:
                return arg_info
        return None
    
    def __convert_arg_type(self, value_str, arg_info):
        if not isinstance(arg_info, dict):
            raise TypeError('Expecting arg_info to be an instance of dict')
        if not isinstance(value_str, basestring):
            raise TypeError('Expecting value_str to be an instance of str')

        if arg_info['type'] == bool:
            return False if val_str.lower() == 'false' or value_str == '0' else True

        elif arg_info['type'] == int:
            try:
                return int(value_str)
            except:
                raise Exception('expecting integer type for argument: %s' % arg_info['out_key']) 
        elif arg_info['type'] == float:
            try:
                return float(value_str)
            except:
                raise Exception('expecting float type for argument: %s' % arg_info['out_key']) 
        else:
            return value_str

    def __get_default_arg_value(arg_info):
        return arg_info['default'] if 'default' in arg_info else None

    def __consume_one_arg(self, arg_index, arg_defs = []):
        if arg_index >= len(sys.argv):
            return None
        
        rawarg_text = sys.argv[arg_index]
        arg_info = {
                'is_named': False,
                'name': None,
                'value': None,
                'type': None,
                'no_rawargs_consumed': 1,
                'arg_def': None,
        }

        regex = re.compile('^(\-\-([A-Z,a-z,0-9,_]+)([=]?(.*))?)|(\-([A-Z,a-z,0-9])(.*))$')
        matches = regex.search(rawarg_text)
        if matches is not None:
            arg_info['is_named'] = True
            if matches[0]:
                # matched --spam=EGG or --spam
                arg_info['name'] = matches[1]
                arg_info['value'] = matches[3]
                call_par = {'long_name': matches[1]}
            elif matches[4]:
                # matched -sEGG or -s
                arg_info['name'] = matches[5]
                arg_info['value'] = matches[6]
                call_par = {'short_name': matches[5]}
            else:
                raise Exception('Unexpected regular expression match while parsing: %s' % rawarg_text) 

            arg_def = self.__find_arg_def(arg_defs, **call_par)
            if not arg_def:
                raise Exception('unexpected argument: %s' % arg_info['name'])
            
            arg_info['arg_def'] = arg_def

            if arg_info['value'] is None:
                # --spam (no value)
                next_arg_info = self.__consume_one_arg(self, arg_index+1, arg_defs)
                
                if next_arg_info is None:
                    # --spam <EOL>
                    if arg_def['type'] == bool:
                        # --spam doesnt have an explicit value; but flag --spam alone implies TRUE
                        arg_info['value'] = True 
                    else:
                        arg_info['value'] = self.__get_default_arg_value(arg_info)
                        if arg_info['value'] is None:
                            raise Exception('ran out of raw arguments while getting value of: %s' % arg_info['out_key'])
                else: 
                    # next_arg_info is not none
                    if next_arg_info['is_named']:
                        # e.g. --spam --HAM 
                        if arg_def['type'] == bool:
                            # --spam doesnt have an explicit value; but flag --spam alone implies TRUE
                            arg_info['value'] = True 
                        else:
                            arg_info['value'] = self.__get_default_arg_value(arg_info)
                            if arg_info['value'] is None:
                                raise Exception(
                                    'argument %s should precede a value instead of argument %s' % (
                                        arg_info['out_key'], next_arg_info['out_key']
                                    )
                                )
                    else:
                        # next argument is unnamed - e.g. --spam HAM
                        arg_info['value'] = self.__convert_arg_type(next_arg_info['value'], arg_info)
                        arg_info['no_rawargs_consumed'] += 1
            else:
                # e.g. --spam="HAM" i.e. arg_info['value'] is not none
                arg_info['value'] = self.__convert_arg_type(arg_info['value'], arg_info)
        else:
            # matched a un-named raw argument
            arg_info['is_named'] = False
            arg_info['value'] = rawarg_text
        
        return arg_info    
    
    def __is_action_enabled(self):
        return isinstance(self.__param_def, dict)

    def __get_action(self):
        print "__get_action()"
        if len(sys.argv) == 1:
            return self.__default_action

        arg_info = self.__consume_one_arg(1)
        print "arg_info:", arg_info
        if arg_info is None:
            return None
        else:
            return arg_info['value'] if not arg_info['is_named'] else None
 
    def __get_action_help(self, action=None):
        """get a help string for an action; if action is None then returns
            a dict of help texts for all actions
        """
        if not self.__is_action_enabled():
            raise Exception('not action enabled')
        
        all_action_help_dict = {} if action is None else None

        regex = re.compile('^([A-Z,a-z,0-9,_]+)\s+(.*)$')
        for action_as_defined in self.__param_def:
            # e.g. "spam  helpText: eat some spam"
            action_name = action_as_defined
            action_help = None

            matches = regex.search(param_def_name)
            if matches:
                mg = matches.groups()
                action_name = mg[0]
                action_help = mg[1]
            
            if action_name == action and action is not None:
                return action_help

            if isinstance(all_action_help_dict, dict):
                all_action_help_dict[action_name] = action_help
        return all_action_help_dict
 
    def __get_param_def(self, action=None):
        """returns a list of tuples [('[--foo]', str), (..), ..]"""
        if not self.__is_action_enabled():
            return self.__param_def
        else:
            if not action:
                raise ValueError('Expecting action to be not None')

            regex = re.compile('^([A-Z,a-z,0-9,_]+)\s+(.*)$')
            for action_as_defined in self.__param_def:
                # e.g. "spam  helpText: eat some spam"
                action_name = action_as_defined
                action_help = None

                matches = regex.search(action_as_defined)
                if matches:
                    mg = matches.groups()
                    action_name = mg[0]
                    action_help = mg[1]
                
                if action_name == action:
                    return self.__param_def[action_as_defined]
        return None

    def __parse_param_def(self, user_action=None):
        """for given user action, get the corresponding param defintion;
            gives list of parsed param def. if user_action is invalid, throws
            an Exception
        """
        print "__parse_param_def(%s)" % user_action
        if self.__is_action_enabled():
            print "\tis action enabled"
            # __param_def is a dict
            if user_action is None:
                param_def = {}
                for action in self.__param_def:
                    param_def[action] = self.__parse_param_def_list(
                            self.__param_def[action]
                    )
                return param_def

            else:
                def_list = self.__get_param_def(user_action)
                if not def_list:
                    raise Exception('Action not found: %s' % user_action)
                return self.__parse_param_def_list(def_list)

        else:
            print "\tnot action enabled"
            # not action-enabled; __param_def is a list
            return self.__parse_param_def_list(self.__param_def)
    
    def __parse_param_def_list(self, in_list):
        """returns a list of processed param definitions, given in_list=
            [(def1),(def2),...]
        """
        if not isinstance(in_list, list):
            raise TypeError('expecting in_list to be of type list')

        param_def_list = []
        # try to match: "[--spam|--spam,-s|-s=ham]REPEAT  HELP"   where REPEAT is "x123" or "..." 
        regex = re.compile('^(\[|<)(\-\-([A-Z,a-z,0-9,_]+)|\-([A-Z,a-z,0-9])|\-\-([A-Z,a-z,0-9,_]+)\,\-([A-Z,a-z,0-9])|([A-Z,a-z,0-9,_]+))(=([A-Z,a-z,0-9._,\.,\s,\-]+))?(\]|>)([\.]{3}|x([0-9]+))?([\s]+(.*))?$')
        for definitions in in_list: 
            if isinstance(definitions, tuple):
                matches = regex.search(definitions[0])
                if not matches:
                    raise ValueError(definitions[0]+': invalid parameter definition') 
                m = matches.groups()
                is_required = m[0] == '<' and m[9] == '>'
                positional_name = m[6]
                is_positional = True if m[6] else False   
                long_name = m[2] if not m[4] else m[4]
                short_name = m[3] if not m[5] else m[5]
                placeholder = m[8] if m[8] else None
                if m[10] == '...':
                    # m[10] may be '...' or 'x12345'
                    cnt = '...'
                elif m[11]:
                    # in case of "x12345" , m[11] is the integer val  
                    cnt = int(m[11])
                else:
                    cnt = None 
                 
                if is_positional:
                    out_key = positional_name
                else:
                    out_key = long_name if long_name else short_name 
                
                help_text = m[13] if m[13] else ''
                default = definitions[2] if len(definitions) > 2 else None 
                if default and is_positional:
                    raise ValueError('unnamed arguments cannot have default values')

                param_def_list.append({
                        'is_positional': is_positional,
                        'is_required': is_required,
                        'positional_name': positional_name,
                        'long': long_name,
                        'short': short_name,
                        'count': cnt,
                        'type': definitions[1],
                        'out_key': out_key,
                        'help': help_text, 
                        'default': default,
                        'placeholder': placeholder
                })
            else:
                raise TypeError('expecting a tuple')
        return param_def_list         

    def parse(self):
        """parses current parameter definition, then process the command line
            to give processed user parameters
        """
        def get_param_repeat_count(param_def):
            """from a parameter definition, gets the number of repeats; 
                returns an integer that can be used in a decrement counter
            """
            if isinstance(param_def['count'], int):
                return param_def['count']
            elif param_def['count'] == '...':
                return -1
            else:
                return 1

        # parse() ---
        param_def_list = self.__parse_param_def(self.__get_action())
        if not isinstance(param_def_list, list):
            # TODO: print usage
            raise Exception('Specify an action!')

        # [positional1] [positional2] ... [--named1] [--named2] ... [--namedN] [positionalN-1] [positional[N]
        # '--------------v--------------' '------------------v---------------' '--------------v-------------'
        #              list1                               list2                            list3
        list1 = []
        list2 = []
        list3 = []
        for param_def in param_def_list:
            if param_def['is_positional']:
                if not len(list2):
                    list1.append(param_def)
                else:
                    list3.append(param_def)
            else:
                if len(list3):
                    raise Exception('Unexpected positional argument')
                else:
                    list2.append(param_def)
        
        arg_idx = 1 if not self.__is_action_enabled() else 2
        all_params = {}
        #
        # process list1
        # <Pos1>xNN <Pos2>xNN .. <PosN-1>xNN [PosN]..
        #
        list1_counter = 0
        for param_def in list1:
            if arg_idx >= len(sys.argv):
                print "no more argv"
                break
            print "list1[%d]: %s" % (list1_counter, param_def['out_key'])
            out_key = param_def['out_key']
            list1_counter += 1
            is_last_list1_item = list1_counter < len(list1)
            remain_count = get_param_repeat_count(param_def)
            is_finite = remain_count > 0
            is_repeating = remain_count != 1

            print "  repeat %d items, is_finite? %s, is_repeating? %s" % (remain_count, is_finite, is_repeating)
            # validate this param-def
            if is_last_list1_item:
                if not param_def['is_required']:
                    # [definition error] positional argument %s (%d) (1,2,..,N-1) must be required' 
                    raise CPositionalArgRequired(param_def)
                if not is_finite:
                    # [definition error] positional argument %s (%d) (1,2,..,N-1) must repeat finitely
                    raise CPositionalArgMustBeFiniteudefinition(param_def)
            this_param = [] if is_repeating else None
            while remain_count:
                print "  .. remaining %d items" % remain_count
                user_arg_info = self.__consume_one_arg(arg_idx, list1)
                if is_repeating:
                    this_param.append(user_arg_info['value'])
                else:
                    this_param = user_arg_info['value']
                remain_count -= 1
                arg_idx += user_arg_info['no_rawargs_consumed']     
                # ~ might have consumed >1 command line args, e.g. -v, --help, etc
                if arg_idx >= len(sys.argv):
                    break
            # finished collecting current item in list1
            all_params[param_def['out_key']] = this_param

        #
        # post-process list1: see if we have gathered all required parameters
        #
        list1_count = 0
        for param_def in list1:
            list1_counter += 1
            is_last_list1_item = list1_counter < len(sys.argv)
            is_required = (not is_last_list1_item) or (is_last_list1_item and param_def['is_required'])
            repeats_required = get_param_repeat_count(param_def)        # gives 1, 2,3,4...,N, -1
            out_key = param_def['out_key']

            if not out_key in all_params:
                if is_required:
                    raise CArgRequiredButMissing(param_def)
                else:
                    all_params[out_key] = None if repeats_required == 1 else []
            
            if repeats_required > 1:
                if len(all_params[out_key]) != repeats_required:
                    raise CArgNotEnoughRepeats(param_def, all_params[out_key])

        print "--- list1 finished ---"
        pp(all_params)




