import sys
import re 

class CbParamError(Exception):
    ERROR_BAD_ACTION = -1
    ERROR_POSITIONAL_ARG_EXTRA = -2
    ERROR_POSITIONAL_ARG_MISSING = -3
    ERROR_NAMED_ARG_EXTRA = -4
    ERROR_NAMED_ARG_MISSING = -5
    
    def __init__(self, message, error_type, error_detail):
        Exception.__init__(self, message)
        self.__error_type = error_type
        self.__error_detail = error_detail
    
    @property
    def error_type():
        return self.__error_type
    
    @property
    def error_detail():
        return self.__error_detail


class TtConsoleApp(object):
    # should be a list or dict
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
    #   ("[spam]", str)       positional argument. must appear either in the start or end of param list
    #   ('[--spam]', str)     accepts --spam="HAM"
    param_criteria = None
    default_action = None

    def __get_parameter_error(self):
        if isinstance(self.__parameters, tuple):
            return self.__parameters
   
    def __set_parameter_error(self, error_code, msg=None):
        self.__parameters = (error_code, msg)

    def __init__(self):
        self.__parameters = None

    @classmethod
    def __get_arg(cls, idx):
        if idx >= len(sys.argv):
            return (None,-1)
        else:
            return (sys.argv[idx], idx+1)
   
    @classmethod
    def __get_wanted_arg(cls, spec_list, long_name=None, short_name=None):
        """from a spec list, find (wanted_type, matching_idx); returns (None, -1) if key not found"""
        idx = 0
        if bool(long_name) ^ bool(short_name):
            for spec in spec_list:
                if (long_name and spec['long'] == long_name) or (short_name and spec['short'] == short_name): 
                    return (spec['type'], idx)
                idx += 1
            return (None, -1)
        else:
            raise ValueError('either long_name or short_name must be None')

    @classmethod 
    def __get_action(cls):
        (action, next_idx) = cls.__get_arg(1)
        if next_idx == -1:
            # no argv[1]
            return self.default_action
        elif not action in cls.param_criteria:
           # no such action
           return None
        else:
           return action

    @classmethod
    def __get_named_arg(cls, arg_idx, wanted_list=[]):
        """returns (spam, HAM, new_idx, matched_pos);
            if no match then (None, None, arg_idx, -1);
            if no more args then (None, None, -1, -1)
            if found positional arg, then (None, 'spam', arg_idx, -2)
        """
        spec_idx = -1
        key_str = val_str = None
        # e.g. --spam="HAM" (+1)     --spam "HAM" (+2)      -p"HAM" (+1)     -p "HAM" (+2)
        (arg, arg_idx) = cls.__get_arg(arg_idx)
        if arg_idx == -1:
            # no more args available
            return (None, None, -1, -1)
        regex = re.compile('^\-\-([A-Z,a-z,0-9,_]+)([=]?(.*))?$') 
        matches = regex.search(arg)
        if matches is not None:
            matches = matches.groups()
            # --spam=HAM or --spam
            key_str = matches[0]
            val_str = matches[2]
            (wanted_type, spec_idx) = cls.__get_wanted_arg(wanted_list, long_name=key_str)
            if wanted_type is None:
                return (key_str, val_str, arg_idx, -1)
            
            # if we want a bool, then the second argument is not necessary unless --spam=false 
            if val_str is None:
                if wanted_type == bool:
                    val_str = True
                else:
                    # --spam "ham"   (very fishy! might cause parsing problems)
                    (val_str, arg_idx) = cls.__get_arg(arg_idx)
            else:
                # we got a val_str already;
                if wanted_type == bool:
                    val_str = not (val_str == 'false' or val_str == '0')

            return (key_str, val_str, arg_idx, spec_idx)

        regex_short = re.compile('^\-([A-Z,a-z,0-9])(.*)$') 
        matches = regex_short.search(arg)
        if matches is not None:
            matches = matches.groups()
            # -s"HAM" or -s 
            key_str = matches[0]
            val_str = matches[1]

            (wanted_type, spec_idx) = cls.__get_wanted_arg(wanted_list, short_name=key_str)
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

        # matched a positional param 
        return (None, arg, arg_idx, -2)

    
    def get_parameters(self):
        """prepare parameter specification"""
        # determine criteria list
        if isinstance(self.param_criteria, dict):
            # need an action
            action = self.__get_action()
            param_criteria_list = self.param_criteria[action] 
            starting_arg_idx = 2
        elif isinstance(self.param_criteria, list):
            param_criteria_list = self.param_criteria
            starting_arg_idx = 1
        else:
            raise TypeError('expecting param_criteria to be either a dict or list)')

        spec_list = []
        regex = re.compile('^(\[|<)(\-\-([A-Z,a-z,0-9,_]+)|\-([A-Z,a-z,0-9])|\-\-([A-Z,a-z,0-9,_]+)\,\-([A-Z,a-z,0-9])|([A-Z,a-z,0-9,_]+))(\]|>)([\.]{3}|x([0-9]+))?$')
        for criteria in param_criteria_list: 
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
                    cnt = int(m[9])
                else:
                    cnt = None 
                
                if is_positional:
                    out_key = positional_name
                else:
                    out_key = long_name if long_name else short_name 
                spec_list.append({
                        'is_positional': is_positional,
                        'positional_name': positional_name,
                        'long': long_name,
                        'short': short_name,
                        'count': cnt,
                        'type': criteria[1],
                        'out_key': out_key,
                })
            else:
                raise TypeError('expecting a tuple')
        
        # break up spec_list into the following three segments
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
            arg_idx = starting_arg_idx
            
            # begin parsing here
            params = {}
            for spec in posi_begin_list:
                print "spec:", spec
                print "arg_idx:", arg_idx
                this_param = None
                if isinstance(spec['count'], int):
                    rep_count = spec['count']
                    this_param = []
                elif spec['count'] == '...':
                    rep_count = -1      # this way, rep_count will never reach 0 
                    this_param = []
                else:
                    rep_count = 1
                print "will collect:", rep_count, "elements"    
                while rep_count:
                    (arg_val, arg_idx) = self.__get_arg(arg_idx)
                    if isinstance(this_param, list):
                        this_param.append(arg_val)
                    else:
                        this_param = arg_val
                    # look at next args; if we have no more args (or next arg is --spam) then exit
                    print "looking forward"
                    par = self.__get_named_arg(arg_idx)
                    if par[3] == -1:
                        break
                    if isinstance(rep_count, int):
                       rep_count -= 1
                    print "remaining repeats:", rep_count   
                print "### posi_begin: ",spec['out_key'],'=',this_param
                params[spec['out_key']] = this_param
           
            print "--- middle ---\n", para_list,"-----" 
            starting_arg_idx = arg_idx
            while len(para_list):
                print "### arg_idx=",arg_idx
                (key, val, arg_idx, para_idx) = __get_named_arg(arg_idx, para_list)
                if arg_idx == -1:
                    # EOL 
                    break
                elif para_idx == -2:
                    # matched positional var; need to roll back one arg_idx
                    if arg_idx == starting_arg_idx + 1:
                        raise Exception('found extra positional parameter: %s' % val)
                    arg_idx -= 1
                    break
                elif para_idx == -1:
                    raise Exception('found unexpected argument: %s=%s' % (key,val))
                matching_spec = para_list[para_idx]
                print "matching spec:", matching_spec
                
                out_type = matching_spec['type']
                out_val = None
                if out_type == bool:
                    out_val = val
                elif out_type == int:
                    out_val = int(val)
                elif out_type == str:
                    out_val = val

                params[matching_spec['out_key']] = out_val

            # trailing positional parameters 
            print "### posi_end scan!"
            for spec in posi_end_list:
                if arg_idx == -1:
                    raise Exception('no more arguments; expecting positional arguments')
                print "spec:", spec
                print "arg_idx:", arg_idx
                this_param = None
                if isinstance(spec['count'], int):
                    rep_count = spec['count']
                    this_param = []
                elif spec['count'] == '...':
                    rep_count = -1      # this way, rep_count will never reach 0 
                    this_param = []
                else:
                    rep_count = 1
                print "will collect:", rep_count, "elements"    
                while rep_count:
                    (arg_val, arg_idx) = __get_arg(arg_idx)
                    if arg_idx == -1:
                        print "no more params. exiting"
                        break
                    if isinstance(this_param, list):
                        this_param.append(arg_val)
                    else:
                        this_param = arg_val
                    if isinstance(rep_count, int):
                       rep_count -= 1
                    if arg_idx == -1:
                        if rep_count > 0:
                            raise Exception('EOL hit; expecting %s more elements') 
                        break
                params[spec['out_key']] = this_param
            
            if arg_idx != -1 and arg_idx < len(sys.argv):
                raise Exception('unused parameters')
        
        return params















