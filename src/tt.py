import sys
import re 

#
# TODO: verbosity support [--verbose,-v], help support [--help,-?], usage support 
# TODO: better exceptions 
#
class TtArgParserError(Exception):
    def __init__(self, message=None, error_detail=None):
        Exception.__init__(self, message)
        self.__error_detail = error_detail
    
    @property
    def error_detail():
        return self.__error_detail

class TtTypeError(TtArgParserError):
    def __init__(self, param_name, wanted_type):
        error_detail=(param_name, wanted_type) 
        if isinstance(wanted_type, basestring):
            error_str = 'Regular expression mismatch for parameter %s' % param_name
        else:
            error_str = 'Expecting parameter %s to be of type %s' % (param_name,wanted_type.__name__) 
        super(TtTypeError, self).__init__(error_str, error_detail)

class TtBadActionError(TtArgParserError):
    def __init__(self, action_name=None):
        error_str = 'Bad action' if not action_name else 'Bad action: %s' % action_name 
        super(TtBadActionError, self).__init__(error_str, action_name)


class TtExtraNamedArgError(TtArgParserError):
    def __init__(self, error_detail=None):
        if isinstance(error_detail, basestring):
            error_str = 'Unexpected extra named argument: %s' % error_detail
        elif isinstance(error_detail, list):
            error_str = 'Unexpected extra named %s: %s' % (
                    'arguments' if len(error_detail) > 1 else 'argument',
                    ', '.join(error_detail)
            )
        else:
            error_str = 'Unexpected extra named argument(s)'
        super(TtExtraNamedArgError, self).__init__(error_str, error_detail)


class TtMissingNamedArgError(TtArgParserError):
     def __init__(self, error_detail=None):
        if isinstance(error_detail, basestring):
            error_str = 'Missing named argument: %s' % error_detail
        elif isinstance(error_detail, list):
            error_str = 'Missing named %s: %s' % (
                    'arguments' if len(error_detail) > 1 else 'argument',
                    ', '.join(error_detail)
            )
        else:
            error_str = 'Missing named argument(s)'
        super(TtMissingNamedArgError, self).__init__(error_str, error_detail)


class TtMissingUnnamedArgError(TtArgParserError):
      def __init__(self, arg_name=None):
        if isinstance(arg_name, basestring):
            error_str = 'Missing positional argument: %s' % arg_name 
        elif isinstance(arg_name, list):
            error_str = 'Missing positional %s: %s' % (
                    'arguments' if isinstance(arg_name, list) and len(arg_name) > 1 else 'argument',
                    ', '.join(arg_name)
            )
        else:
            error_str = 'Missing positional argument(s)'
        super(TtMissingUnnamedArgError, self).__init__(error_str, arg_name)  


class TtExtraUnnamedArgError(TtArgParserError):
    pass


class TtExpectingMoreArgsError(TtArgParserError):
    pass


class TtUnusedParameters(TtArgParserError):
    pass


class TtUnnamedArgPositionError(TtArgParserError):
    pass


class TtArgParser(object):
    # should be a list or dict
    # valid specifiers are:
    #   [spam]      optional
    #   <spam>      required
    #
    # optionally, specify how many times an argument should appear
    #   [spam]x3    for unnamed args: should appear 3 times in a row; 
    #               for parametric args: can appear three times max, excess are discarded
    #   [spam]...   for unnamed args: appears N times until next argument hits
    #               for parametric args: can appear as many times as it appears  
    #
    # "spam" above should be 
    #   --spam,-s   accepts either --spam="HAM", -s"HAM", --spam "HAM", -s "HAM"     
    #   --spam      accepts only --spam="HAM", --spam "HAM"
    #   -s          accepts only -s"HAM", -s "HAM"
    #
    # definitions:
    #   ("[spam]", str)       unnamed argument. must appear either in the start or end of param list
    #   ('[--spam]', str)     accepts --spam="HAM"
    def __init__(self, definitions=None, default_action=None):
        self.__param_def = definitions
        self.__default_action = default_action 
        self.__param_results = None
        self.__error = None
        self.__verbosity = 0
        self.__help = 0
    
    @property
    def default_action(self):
        return self.__default_action
    
    @default_action.setter
    def set_default_action(self, default_action=None):
        if self.__default_action != default_action:
            self.__default_action = default_action
            self.__param_results = None
    
    @property
    def definitions(self):
        return self.__param_def
    
    @definitions.setter
    def set_definitions(self, definitions):
        self.__param_def = definitions
        self.__param_results = None
        return True

    def __get_arg(self, idx):
        """gets argument and return (arg_text, next_idx)"""
        if idx >= len(sys.argv):
            return (None,-1)
        else:
            idx = self.__process_special_arg(idx)
            if idx == -1:
                return (None, -1)
            return (sys.argv[idx], idx + 1)
   
    def __get_action(self):
        """gets argv[1] and returns it"""
        (action, next_idx) = self.__get_arg(1)
        if next_idx == -1:
            # no argv[1]
            return self.__default_action
        else:
           return action

    def __get_named_arg(self, arg_idx, wanted_args_list=[]):
        """for "--spam=HAM" returns (spam, HAM, new_idx, matched_idx_in_wanted_args_list);
            if no match then (None, None, arg_idx, -1);
            if no more args then (None, None, -1, -1)
            if found unnamed arg, then (None, 'spam', arg_idx, -2)
        """
       #print '\t__get_named_args(arg_idx=%d)'%arg_idx
        spec_idx = -1
        key_str = val_str = None
        # e.g. --spam="HAM" (+1)     --spam "HAM" (+2)      -p"HAM" (+1)     -p "HAM" (+2)
        (arg, arg_idx) = self.__get_arg(arg_idx)
        if arg_idx == -1:
            # no more args available
            return (None, None, -1, -1)
        regex = re.compile('^\-\-([A-Z,a-z,0-9,_]+)([=]?(.*))?$') 
        matches = regex.search(arg)
        if matches is not None:
            matches = matches.groups()
           #print "\t...long match:", matches
            # --spam=HAM or --spam
            key_str = matches[0]
            val_str = matches[2]
            (wanted_type, spec_idx) = self.__get_wanted_arg(wanted_args_list, long_name=key_str)
            if wanted_type is None:
                return (key_str, val_str, arg_idx, -1)
            
            # if we want a bool, then the second argument is not necessary unless --spam=false 
            if val_str is None:
                if wanted_type == bool:
                    val_str = True
                else:
                    # --spam "ham"   (very fishy! might cause parsing problems)
                    (val_str, arg_idx) = self.__get_arg(arg_idx)
            else:
                # we got a val_str already;
                if wanted_type == bool:
                    val_str = not (val_str == 'false' or val_str == '0')

            return (key_str, val_str, arg_idx, spec_idx)

        regex_short = re.compile('^\-([A-Z,a-z,0-9])(.*)$') 
        matches = regex_short.search(arg)
        if matches is not None:
            matches = matches.groups()
           #print "\t...short match:", matches
            # -s"HAM" or -s 
            key_str = matches[0]
            val_str = matches[1]

            (wanted_type, spec_idx) = self.__get_wanted_arg(wanted_args_list, short_name=key_str)
            if not wanted_type:
                return (key_str, val_str, arg_idx, -1)

            # if we want a bool, then the second argument is not necessary unless --spam=false 
            if val_str is None:
                if wanted_type == bool:
                    val_str = True
                else:
                    # --spam "ham"   (very fishy! might cause parsing problems)
                    (val_str, arg_idx) = __get_arg(arg_idx)
            else:
                # we got a val_str already;
                if wanted_type == bool:
                    val_str = not (val_str == 'false' or val_str == '0')
           
            return (key_str, val_str, arg_idx, spec_idx)

        # matched a unnamed param 
       #print "\t...no match (positional arg)"
        return (None, arg, arg_idx, -2)

    def __convert_param_val(self, spec, val=None):
        """for a param-spec, convert given value into the correct type"""
        if not val:
            return spec.get('default',None)
        else:
            if '__call__' in dir(spec['type']):     # lambda or function
                callback_result = spec['type'](val)
                if callback_result is None:
                    raise TtTypeError(spec['out_key'], spec['type'])
                else:
                    return callback_result

            if isinstance(spec['type'], basestring):
                regex = re.compile(spec['type'])
                matches = regex.search(val)
                if matches:
                    return(matches.groups())
                else:
                    raise TtTypeError(spec['out_key'], spec['type'])

            if spec['type'] == int:
                try:
                    return int(val)
                except:
                    if spec['default'] is None:
                        raise TtTypeError(spec['out_key'], spec['type'])
                    else:
                        return spec['default']

            elif spec['type'] == float:
                try:
                    return float(val)
                except:
                    if spec['default'] is None:
                        raise TtTypeError(spec['out_key'], spec['type'])
                    else:
                        return spec['default']

            elif spec['type'] == bool:
                return False if val == 'false' or val == 'no' else True

            else:
                return str(val)
    
    def __make_param_def_list(self, in_list):
        """returns a list of processed param definitions, given in_list=[(def1),(def2),...]"""
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
                is_unnamed = True if m[6] else False
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
                 
                if is_unnamed:
                    out_key = positional_name
                else:
                    out_key = long_name if long_name else short_name 
                
                help_text = m[13] if m[13] else ''
                default = definitions[2] if len(definitions) > 2 else None 
                if default and is_unnamed:
                    raise ValueError('unnamed arguments cannot have default values')

                param_def_list.append({
                        'is_unnamed': is_unnamed,
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

    def is_action_enabled(self):
        return isinstance(self.__param_def, dict)

    def __make_param_def(self):
        """returns a param_def_list based on current action (if action is enabled)
            if no action is selected, returns a dict {action1:[..], action2:[].., ..}
        """ 
        action = self.__get_action()

        if self.is_action_enabled() and action is None:
            param_def_list = {} 
            for action in dict:
                param_def_list[action] = self.__make_param_def_list(self.__param_def[action])
        
        elif self.is_action_enabled() and action is not None:
            if not action in self.__param_def:
                raise TtBadActionError(action)
            param_def_list = self.__make_param_def_list(self.__param_def[action])
        else:
            param_def_list = self.__make_param_def_list2(self.__param_def)

        return param_def_list
    
    def __get_wanted_arg(self, param_def_list, long_name=None, short_name=None):
        """from a spec list, find (wanted_type, matching_idx); returns (None, -1) if key not found"""
        idx = 0
        if bool(long_name) ^ bool(short_name):
            for spec in param_def_list:
                if (long_name and spec['long'] == long_name) or (short_name and spec['short'] == short_name): 
                    return (spec['type'], idx)
                idx += 1
            return (None, -1)
        else:
            raise ValueError('either long_name or short_name must be None')


    def parse(self):
        """parses argv and give a list of parsed parameters {param1:xx, param2:xx, ..}"""
       #print "---Args---"
       #print sys.argv

        def initialize_rep_count(spec):
            """returns a rep_count for counting down""" 
            if isinstance(spec['count'], int):
                rep_count = spec['count']
                this_param = []
            elif spec['count'] == '...':
                rep_count = -1      # this way, rep_count will never reach 0 
                this_param = []
            else:
                rep_count = 1
            return rep_count
           
        # __make_param_def() automatically queries $1 for action
        param_def_list = self.__make_param_def()

        # break up param_def_list into the following three segments
       #print '---> make lists'
        posi_begin_list = []
        posi_end_list = []
        para_list = []
                
        for spec in param_def_list:
            if spec['is_unnamed']:
                if not len(para_list):
                    posi_begin_list.append(spec)
                else:
                    posi_end_list.append(spec)
            else:
                if len(posi_end_list):
                    raise TtUnnamedArgPositionError()
                else:
                    para_list.append(spec)

        arg_idx = 2 if self.is_action_enabled() else 1
       
        
       #print '---> unnamed arguments' 
        # begin parsing here
        params = {}
        for spec in posi_begin_list:
           #print "     - def:", spec
            this_param = None
            rep_count = initialize_rep_count(spec)
            if rep_count != 1:
                this_param = []
           #print "---> will collect %d beginning positional parameters" % rep_count 
            while rep_count:
               #print "     - rep_count:", rep_count
                (arg_val, arg_idx) = self.__get_arg(arg_idx)
                arg_val = self.__convert_param_val(spec, arg_val)
                if arg_idx == -1:       # no more args
                    break
                rep_count -= 1
                if isinstance(this_param, list):
                    this_param.append(arg_val)
                else:
                    this_param = arg_val
                # look at next args; if we have no more args (or next arg is --spam) then exit
                par = self.__get_named_arg(arg_idx)
                if par[3] == -1:
                    break

            if rep_count > 0:
                raise TtMissingUnnamedArgError(spec['positional_name'])
            params[spec['out_key']] = this_param
        
       #print '---> named parameters:', para_list
        starting_arg_idx = arg_idx
        while len(para_list):
           #print "     -> getting parameter #", arg_idx 
            (key, val, arg_idx, para_idx) = self.__get_named_arg(arg_idx, para_list)
           #print "     -> arg_idx:",arg_idx,", para_idx:",para_idx
            if arg_idx == -1:
                # EOL 
                break
            elif para_idx == -2:
                # matched unnamed var; need to roll back one arg_idx
                if arg_idx == starting_arg_idx + 1:
                    raise TtExtraUnnamedArgError(error_detail=val)
                arg_idx -= 1
                break
            elif para_idx == -1:
                raise TtExtraNamedArgError(error_detail=key)

            matching_spec = para_list[para_idx]
            rep_count = initialize_rep_count(matching_spec)
            out_val = self.__convert_param_val(matching_spec, val)

            if rep_count == 1:
                params[matching_spec['out_key']] = out_val 
            else:
                if not isinstance(params[matching_spec['out_key']], list):
                   params[matching_spec['out_key']] = []
                if len(params[matching_spec['out_key']]) < rep_count or rep_count == -1:
                    params[matching_spec['out_key']].append(out_val) 

        # see if we have collected all required named parameters
        missing_para_items = []
        for para_item in para_list:
            if para_item['is_required']:
                if not para_item['out_key'] in params:
                    missing_para_items.append(para_item['out_key'])
        if len(missing_para_items):
            raise TtMissingNamedArgError(missing_para_items)
            
                    

        # if we run out of params, and there is a required arg, then fail
        if arg_idx == -1:
            missing_param_names = []
            for spec_ in posi_end_list:
                if spec_['is_required']:
                    missing_param_names.append(spec_['out_key'])
            if len(missing_param_names):
                raise TtMissingUnnamedArgError(missing_param_names)
        
        #print "===> trailing positional parameters"
        # trailing unnamed parameters 
        for spec in posi_end_list:
            this_param = None
            rep_count = initialize_rep_count(spec) 
           #print "---> will collect %d trailing positional parameters" % rep_count 
            # will collect rep_count nubmer of arguments until rep_count reaches 0
            while rep_count and arg_idx != -1:
                (arg_val, arg_idx) = self.__get_arg(arg_idx)
                print "Received:",arg_val
                arg_val = self.__convert_param_val(spec, arg_val)
                if arg_idx == -1:
                    break
                if isinstance(this_param, list):
                    this_param.append(arg_val)
                else:
                    this_param = arg_val
                if isinstance(rep_count, int):
                   rep_count -= 1
                if arg_idx == -1:
                    if rep_count > 0:
                        raise TtExpectingMoreArgsError(error_detail=rep_count)
                    break
            params[spec['out_key']] = this_param
        
        if arg_idx != -1 and arg_idx < len(sys.argv):
            unused_list = []
            while arg_idx < len(sys.argv) and arg_idx != -1:
                new_idx = self.__process_special_arg(arg_idx)
                if new_idx == arg_idx:
                    # if argument hasnt been consumed, then mark as "unused"
                    unused_list.append(sys.argv[arg_idx])
                    arg_idx += 1
                else:
                    arg_idx = new_idx
            if len(unused_list):
                raise TtUnusedParameters(error_detail=unused_list) 
        
        for param_def in param_def_list:
            if param_def['default'] is not None and not param_def['out_key'] in params:
                params[param_def['out_key']] = param_def['default']
            
        self.__param_results = params
        return self.__param_results 

    def __process_special_arg(self, arg_idx):
        if arg_idx >= len(sys.argv):
            return -1
        arg_str = sys.argv[arg_idx]
        if arg_str in ('--help', '-?', '--verbose', '-v'):
            if arg_str == '--help' or arg_str == '-?':
                raise Exception('help wanted')
            elif arg_str =='--verbose' or arg_str=='-v':
                self.__verbosity += 1
                arg_idx += 1
                if arg_idx == len(sys.argv):
                    return -1
                while True:
                    new_idx = self.__process_special_arg(arg_idx)
                    if new_idx == arg_idx or new_idx == -1:
                        arg_idx = new_idx
                        break
        return arg_idx

    @property
    def results(self):
        return self.__param_results

    def __getitem__(self, val):
        if val == 'action':
            return self.__get_action()
        if isinstance(self.__param_results, dict) and val in self.__param_results:
            return self.__param_results[val]
        else:
            return None
