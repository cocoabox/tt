import sqlite3
import string
import random
import os
import time
import sys
import hashlib
from datetime import datetime
try:
    import simplejson as json
except:
    import json


def make_dict(tup_list, omit_if_none=True):
    '''creates a dict from a tuple list [('key':val),...]'''
    res = {}
    for i in tup_list:
        if i[1] is None and omit_if_none:
            continue
        res[i[0]] = i[1]
    return res    


# TODO: create singleton to reuse database connection (if same DSN)
class DObject(object):
    def debug_msg(self, *args):
        if not self.__verbose:
            return True
        sys.stdout.write('[%s] ' % self.__class__.__name__)
        for stuff in args:
            sys.stdout.write('%s ' % str(stuff))
        sys.stdout.write('\n')
    
    def close(self):
        if isinstance(self.__db, sqlite3.Connection):
            self.__db.close()
            self.__db = None

    def get_db(self):
        '''get database instance; if not established then create'''
        if not isinstance(self.__db, sqlite3.Connection):
            fn = self.__dsn[0]
            if fn[0:2] == '..':
                # convert .. to "parent_path_of_this_script"
                parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
                fn = parent_dir + fn[2:]
            
            self.__db= sqlite3.connect(fn, isolation_level=self.__dsn[1])
            self.__db.row_factory= sqlite3.Row
            self.__run_init_queries()
        return self.__db 

    def __run_init_queries(self):
        '''initializes DB; just have a connection before calling this method'''
        query_list= []
        if not self.__init_queries is None:
            if isinstance(self.__init_queries, str):
                query_list= [self.__init_queries]
            elif isinstance(self.__init_queries,list):
                query_list= self.__init_queries
            else:
                raise TypeError('expecting type: list, str')

            return self.q_multiple(query_list, result_type='NUMBER_OF_ROWS_AFFECTED')
        else:
            self.debug_msg('no initial queries')
            return True
    
    @property
    def dsn(self):
        return self.__dsn[0]

    def __init_dsn(self, dsn):
        if isinstance(dsn, tuple):
            self.__dsn = dsn
        elif isinstance(dsn, basestring):
            self.__dsn = (dsn,'DEFERRED')

        # ensure directory is useable; if not, raise an exception
        dirname = os.path.dirname(self.__dsn[0])
        if not os.path.isdir(dirname):
            try:
                os.makedirs(dirname)
            except:
                raise Exception('unable to create directory %s' % dirname)

    def __init__(self, dsn, init_queries=None, verbose=False):
        '''initializes a DObject'''
        self.__verbose = verbose
        self.__init_queries = init_queries
        self.__db = None
        self.__init_dsn(dsn)

    def q_multiple(self, q_list, result_type='ALL_ROWS', auto_commit=False):
        '''executes more than one queries, and return a list of results'''
        q_list_= q_list if isinstance(q_list,list) else [q_list]
        result_list= []

        for q_list_item in q_list_:
            if isinstance(q_list_item,tuple) or isinstance(q_list_item,list):
                result_list.append(self.q(q_list_item[0], q_list_item[1], result_type, auto_commit=False))
            elif isinstance(q_list_item,str):
                result_list.append(self.q(q_list_item, None, result_type, auto_commit=False))
            else:
                raise TypeError('list item should be a string or tuple')

        if auto_commit:
            self.debug_msg("Committing...")
            self.__db.commit()

        return result_list

    def q(self, query, params=None, result_type='ALL_ROWS', auto_commit=False):
        '''executes a query

            named arguments:
                query -- one single SQL statement
                params -- a dict or tuple; for named parameters, params={'param1':xx,'param2':yy, ...}
                    for unnamed parameters, params=(xx, yy, ..)
                    concatenate tuples and dicts with a list [{..},{..},..] 
                result_type -- string or int; should be "ALL_ROWS", "ALL_DICTS","ONE_ROW", "ONE_DICT", N, "NUMBER",
                    "CURSOR", 'LAST_ROWID','NUMBER_OF_ROWS_AFFECTED'
                    for "ONE_DICT" or "ALL_DICTS", json strings are automatically expanded into objects 
        '''
        def dict_factory(cursor, row):
            '''create a dict from a row, with auto-detection of JSON, datetime '''
            result_dict = {}
            # string formats accepted
            formats = [
                    # json -> list or dict 
                    (regex.compile('^\{.*\}$|^\[.*\]$'), 
                        lambda in_str: json.loads(in_str)
                    ),
                    # datetime string
                    (regex.compile('^[0-9]{4}\-[0-9]{2}\-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'),
                        lambda in_str: datetime.strptime(in_str, '%Y-%m-%d %H:%M:%S')
                    ),
                ]
            for idx, col in enumerate(cursor.description):
                field_data = row[idx]
                # try to convert; if fail then fallback to raw values
                if instance(field_data, basestring):
                    for test_format in formats:
                        if test_format[0].search(field_data):
                            try:
                                field_data = test_format[1](field_data)
                                break
                            except ValueError:
                                pass
                result_dict[col[0]] = field_data 
            return result_dict

       # -- type check --
        if not params is None:
            if not isinstance(params,dict) and not isinstance(params,tuple) and not isinstance(params,list):
                raise TypeError('params should be of either dict,tuple,list type')
            if isinstance(params,list) and isinstance(params[0],dict):
                #combine dicts
                params_={}
                for params_item in params:
                    if not isinstance(params_item, dict):
                        raise TypeError('expecting param list member to be of dict type')
                    params_.update(params_item)
            elif isinstance(params,list) and isinstance(params[0],tuple):
                #combine tuples
                params_=()
                for params_item in params:
                    if not isinstance(params_item, tuple):
                        raise TypeError('expecting param list member to be of tuple type')
                    params_ += params_item
            else:
                #probably a scalar type
                params_= params
        if not isinstance(query,str):
                raise TypeError('query should be a str')

        result= False
        self.get_db().row_factory = dict_factory if result_type == 'ONE_DICT' else sqlite3.Row 
        cursor= self.get_db().cursor()
        
        self.debug_msg('Query:', query.strip('\n\t '))
        if params is not None:
            self.debug_msg('Parameters:', params)
            
        if params is None:
            cursor.execute(query)
        else:
            cursor.execute(query, params_)

        if 'NUMBER' == result_type:
            row= cursor.fetchone()
            result= None if row is None else row[0]
        elif 'ONE_ROW' == result_type or 'ONE_DICT' == result_type: 
            result= cursor.fetchone()
        elif 'ALL_ROWS' == result_type or 'ALL_DICT' == result_type:
            result= cursor.fetchall()
        elif isinstance(result_type,int):
            # fetch N rows
            result= cursor.fetchmany(result_type)
        elif 'LAST_ROWID'== result_type:
            result= cursor.lastrowid
        elif 'NUMBER_OF_ROWS_AFFECTED'== result_type:
            result= cursor.rowcount
        elif 'CURSOR'== result_type:
            result= cursor
        else:
            raise ValueError('unknown result_type; expecting NUMBER,ONE_ROW,ALL_ROWS...')

        if auto_commit:
            self.debug_msg('Committing...')
            self.__db.commit()

        self.debug_msg("Returning:", result)

        return result

    @classmethod
    def _make_set_clause(cls, kv_pairs_dict, accepted_columns_list=None,
            table_name='', parameter_prefix_str='par'):
        '''generates a 'colA=:par1, colB=:par2,...' and parameter dictionary.
            if kv_pairs_dict contains extra column other than accepted_columns_list
            specifies, then they are discarded

            named arguments:
            kv_pairs_dict -- what is going to be written to the database in 
                'column_name':value format
            accepted_columns_list -- a list containing column names accepted. 
                names ending with a star are ignored.if not specified, then column-
                name check will be performed
                e.g. ['key_col*', 'col1',...]
            table_name --
            parameter_prefix_str -- (Default 'par')
        '''
        sql_str_list= []
        params_dict= {}
        par_count= 0
        
        accepted_columns_list2 = []
        json_column_list = []
        for k in accepted_columns_list:
            if isinstance(k, basestring):
                if k[-1] == '*':
                    continue
                elif k[-1] == '#':
                    json_column_list.append(k[0:-1])
                    accepted_columns_list2.append(k[0:-1])
                else:
                    accepted_columns_list2.append(k)
            elif isinstance(k, tuple):
                col_name = k[0]
                if col_name[-1] == '*':
                    continue
                elif col_name[-1] == '#':
                    col_name = col_name[0:-1] 
                    json_column_list.append(col_name)
                    accepted_columns_list2.append(col_name)
                else:
                    accepted_columns_list2.append(col_name)

        for column_name in kv_pairs_dict:
            if (accepted_columns_list is None 
                    or column_name in accepted_columns_list2 
                    or column_name in json_column_list
                    ): 
                param_name = '%s_%s' % (parameter_prefix_str, par_count)
                if table_name == '':
                    sql_str_list.append('`%s`=:%s' % (column_name, param_name))
                else:
                    sql_str_list.append(
                            '`%s`.`%s`=:%s' % (table_name, column_name, param_name))
                    
                # by default, just store whatever is passed, with exceptions below 
                val = kv_pairs_dict[column_name]
                if isinstance(val, datetime):
                    # convert datetime type to sqlite-friendly string
                    val.strftime('%Y-%m-%d %H:%M:%S')
                elif column_name in json_column_list:
                    # user explicitely requested the type to be JSONized
                    val = json.dumps(val) 
                else:
                    if isinstance(val, dict) or isinstance(val, list):
                        val = json.dumps(val)
                        self.debug_msg('''
                                WARNING: column %s: writing a list or dict type;
                                encoding as JSON
                                ''' % column_name)
                params_dict[param_name] =val  
                par_count+= 1
        
        return (','.join(sql_str_list), params_dict)

    @classmethod
    def _make_insert_clause(cls, kv_pairs_dict, parameter_prefix_str='par'):
        '''generates a '(col1,col2,..) VALUES (:par1,:par2,...)'
            and parameter dictionary
        '''
        column_names_list=[]
        par_names_list= []
        params_dict= {}
        par_count= 0
        
        for column_name,data in kv_pairs_dict.iteritems():
            param_name= '%s_%d' % (parameter_prefix_str, par_count)
            par_names_list.append(':'+param_name)
            column_names_list.append('`%s`' % column_name)
            if isinstance(data, datetime):
                params_dict[param_name] = data.strftime('%y-%m-%d %H:%M:%S')
            elif isinstance(data, dict) or isinstance(data, list): 
                params_dict[param_name] = json.dumps(data)
            else:
                params_dict[param_name] = data 
            par_count+= 1

        return (
                '(%s) VALUES (%s)' % (
                ','.join(column_names_list),
                ','.join(par_names_list)),
                params_dict
                )
        
    @classmethod
    def _make_where_clause(cls, criteria_dict, omit_if_null=True,
            parameter_prefix_str='wherepar', glue_str='AND', table_name=None):
        '''makes a 'col=:wherepar1 AND col2=:wherepar2' and a parameter dictionary
            None values are converted to "IS NULL" statements; or omitted if omit_if_null
        '''
        glue_str = glue_str.strip()

        criteria_list= []
        params_dict= {}
        par_count= 0

        for col_name in criteria_dict:
            if criteria_dict[col_name] is None:
                if omit_if_null:
                    continue
                else:
                    if table_name:
                        criteria_list.append('`%s`.`%s` IS NULL' % (table_name, col_name))
                    else:
                        criteria_list.append('`%s` IS NULL' % col_name)
                    continue

            param_name= '%s_%d' % (parameter_prefix_str, par_count)
            params_dict[param_name]= criteria_dict[col_name]
            if table_name:
                criteria_list.append('`%s`.`%s`=:%s' % (table_name, col_name, param_name))
            else:
                criteria_list.append('`%s`=:%s' % (col_name, param_name))
            par_count+= 1

        return ((' %s ' % glue_str).join(criteria_list), params_dict)

    @classmethod
    def _make_in_clause(cls, col_name, valu_list, parameter_prefix_str='inpar'):
        '''generates a ('col IN (:par1,:par2, ..)', {'par1':xx, 'par2':xx, ...}) tuple'''
        par_count = 0
        pars = {}
        sql_list = []
        for valu in valu_list:
            par_name = '%s_%d' % (parameter_prefix_str, par_count)
            sql_list.append(':' + par_name)
            pars [par_name] = valu
            par_count += 1
        return ('%d IN (%s)' % (col_name, ', '.join(sql_list)), pars)  

    @classmethod
    def validate_dict(cls, subject_dict, search_for, on_extra='discard'):
        '''returns true if subject_dict contains all keys in search_for_list;
            search_for: list of 'key' or list of ('key','default_value').
            valid values for on_extra: 'discard' (default), 'fail', 'ignore'
        '''
        def strip_trailing_star(in_str):
            if in_str[-1] == '*' or in_str[-1] == '#':
                return in_str[0:-1]
            else:
                return in_str
        if on_extra not in ['ignore', 'fail', 'discard'] :
            raise ValueError('expecting on_extra to be either ignore, fail or discard')
        
        for wanted in search_for:
            if isinstance(wanted, tuple) and len(wanted) == 2:
                wanted_key = strip_trailing_star(wanted[0])
                if not wanted_key in subject_dict:
                    # use default value (2nd member in the wanted tuple)
                    subject_dict[wanted_key] = wanted[1]
            elif isinstance(wanted, basestring):
                if not strip_trailing_star(wanted) in subject_dict:
                    return False
            else:
                raise TypeError('search_for contains an unrecognized type; expecting string or 2-tuple')
        # if a key in subject is not expected, then return False
        if on_extra in ['fail', 'discard']:
            search_for2 = []
            for search_for_item in search_for:
                if isinstance(search_for_item, basestring):
                    search_for2.append(strip_trailing_star(search_for_item))
                elif isinstance(search_for_item, tuple):
                    search_for2.append(strip_trailing_star(search_for_item[0]))
                else:
                    raise TypeError('expecting member of search_for to be either string or tuple')
            for key in subject_dict:
                if not key in search_for2:
                    if on_extra == 'fail':
                        return False
                    elif on_extra == 'discard':
                        subject_dict.pop(key, None)

        return True

class DProfiles(DObject):
    DB_FILENAME= 'tt_setup.db'
    INIT_QUERIES= ['''
        CREATE TABLE IF NOT EXISTS `profiles`(
            profile_alias TEXT PRIMARY KEY, 
            auth_data TEXT,
            auth_flag INTEGER,
            user_id INTEGER,
            purpose TEXT,
            priority INTEGER
        )
        ''']
    FLAG_NO_AUTH = 0
    FLAG_REQUESTED = 1
    FLAG_AUTHENTICATED = 2
    # keys that should exist in a row; (foo,XX) = non-required keys with default value XX 
    ROW_REQUIREMENT = [
            'profile_alias*',
            ('user_id', 0),
            ('purpose', ''),
            ('priority', 0), 
            ('auth_flag', FLAG_NO_AUTH),
            ('auth_data#', None),
            ]

    def __init__(self, directory='../var', verbose=False):
        super(DProfiles,self).__init__(directory+'/'+self.DB_FILENAME,
                self.INIT_QUERIES, verbose)
    
    def exists(self, profile_alias):
        return 1 == self.q('''
            SELECT COUNT(*) FROM profiles WHERE profile_alias=:profile_alias
        ''', {"profile_alias": profile_alias}, type='NUMBER')

    def insert(self, profile_info, auto_close=True):
        '''inserts a row into the profiles table; 
            profile_info = {'profile_alias':xx,'auth_data':xx,'user_id':xx}
        '''
        if not self.validate_dict(profile_info, self.ROW_REQUIREMENT, 
                on_extra='fail'):
            raise Exception('expected keys not found (or extra keys found) in profile_info')

        insert_tuple = self._make_insert_clause(profile_info)
        # insert_tuple=(sql_str,param_dict)

        res = self.q('''INSERT OR REPLACE INTO `profiles` %s 
            ''' % insert_tuple[0], insert_tuple[1], 'NUMBER_OF_ROWS_AFFECTED',
            auto_commit=True)
        if auto_close:
            self.close()
    
        return res

    def get_access_tokens(self, profile_alias=None):
        if profile_alis is None:
            rows = self.q('''
                    SELECT profile_alias, auth_flag, auth_data FROM profiles
                    WHERE auth_flag=:auth_flag
                    ORDER BY priority ASC
                    ''', 
                    {'auth_flag': FLAG_AUTHENTICATED},
                    'ALL_ROWS'
                    )
        else:
            rows = self.q('''
                    SELECT profile_alias, auth_flag, auth_data FROM profiles
                    WHERE profile_alias=:profile_alias
                    ''', 
                    {'profile_alias': profile_alias},
                    'ALL_ROWS'
                    )
        if not rows:
            self.debug_msg('unable to get access token: row is non-existent')
            return False
        
        result = []
        for row in rows:
            if row['auth_flag'] != FLAG_AUTHENTICATED:
                self.debug_msg('''
                    unable to get access token: profile %s not flagged as
                    AUTHENTICATED
                    ''' % row['profile_alias'])
                return False
            try:
                result.append(json.loads(row['auth_data']))
            except ValueError as e:
                self.debug_msg('''unable to get access token: json decoding
                    failed: %s
                    ''' % row['auth_data'])
        
        return result

    def get(self, profile_alias=None, auth_flag=None):
        if profile_alias is not None:
            return self.q('''
                    SELECT * FROM profiles WHERE profile_alias=:profile_alias
                    ''', {'profile_alias': profile_alias}, 'ONE_ROW')

        if auth_flag is None:
            return self.q('SELECT * FROM profiles', {}, 'ALL_ROWS')
        else:
            (where_str, where_dict) = self._make_where_clause(
                {'auth_flag':auth_flag},
                omit_if_null=True
            )
            return self.q('SELECT * FROM profiles WHERE %s' % where_str, 
                    where_dict, 'ALL_ROWS'
                    )
        
    def delete(self, profile_alias):
        return self.q('''
            DELETE FROM profiles WHERE profile_alias=:profile_alias
            ''', {'profile_alias': profile_alias}, 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True)

    def update(self, profile_alias, profile_info, auto_close=True):
        update_tuple= self._make_set_clause(
                profile_info, self.ROW_REQUIREMENT)
        result = self.q(
                "UPDATE profiles SET %s WHERE profile_alias=:profile_alias" % update_tuple[0], 
                [{'profile_alias':profile_alias}, update_tuple[1]],
                'NUMBER_OF_ROWS_AFFECTED', 
                auto_commit=True
                )
        if auto_close:
            self.close()
        return result 


class DPeople(DObject):
    DB_FILENAME= 'tt_main.db'
    INIT_QUERIES= ['''
        CREATE TABLE IF NOT EXISTS `people`(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER,
            nick_name TEXT,
            screen_name TEXT,
            user_id INTEGER,
            flags INTEGER
        )
        ''','''
        CREATE INDEX IF NOT EXISTS people__user_id ON people(
            user_id 
        )
        ''','''
        CREATE INDEX IF NOT EXISTS people__screen_name ON people(
            screen_name 
        )
        ''','''
        CREATE INDEX IF NOT EXISTS people__person_id ON people(
            person_id
        )
        ''','''
        CREATE INDEX IF NOT EXISTS people__nick_name ON people(
            nick_name
        )
        ''']
    FLAG_MYSELF=1

    def __init__(self, directory='../var', filename=None):
        if filename is None:
            filename = self.DB_FILENAME
        super(DPeople,self).__init__(directory+'/' + filename, self.INIT_QUERIES)

    def insert(self, person_info):
        '''inserts a new person; returns person_id'''
        if not 'nick_name' in person_info:
            person_info['nick_name']= person_info['user_name']
        
        if not self.validate_dict(
                    user_info,
                    ['user_name', 'user_id', 'flags','nick_name'],
                    on_extra='fail'):
            raise Exception(
                    'expected keys not found (or extra keys found) in user_info')
        
        insert_tuple= self._make_insert_clause(user_info)
        # do not check for existing user; faster
        last_rowid= self.q('''
            INSERT OR REPLACE INTO `people` p1 %s
            ''' % insert_tuple[0], insert_tuple[1], 'LAST_ROWID', auto_commit=False)
    
        if 0== self.q('''
                UPDATE people SET person_id=:person_id WHERE id=:id
                ''', {'person_id': last_rowid, 'id': last_rowid},
                'NUMBER_OF_ROWS_AFFECTED', auto_commit=True):
            self.get_db().rollback()
            return False

        return last_rowid

    def get_person_from_user(self, user_id=None, user_name=None):
        '''get person_id and nick_name for given user_name(s).
            to retrieve multiple user_names, use [name1,name2,..].
            returns a dict {person_id:xx,nick_name:xx,user_id:xx} or a list of dicts
        '''
        sql_str= ''
        params= {}
        is_single= True
        if user_id is not None:
            if isinstance(user_id,list):
                sql_param_tuple= self._make_in_clause(user_id)
                sql_str= '''
                    SELECT person_id,nick_name,user_id FROM people WHERE user_id %s
                    ''' % sql_param_tuple[0]
                params.update(sql_param_tuple[1])
                is_single= False
            else:
                sql_str= 'SELECT person_id,nick_name,user_id FROM people WHERE user_id=:user_id'
                params= {'user_id':user_id} 
        elif user_name is not None:
            if isinstance(user_id,list):
                sql_param_tuple= self._make_in_clause(user_id)
                sql_str= 'SELECT person_id,nick_name,user_id FROM people WHERE user_name %s'% sql_param_tuple[0]
                params.update(sql_param_tuple[1])
                is_single= False
            else:
                sql_str= 'SELECT person_id,nick_name,user_id FROM people WHERE user_name=:user_name'
                params= {'user_name':user_name} 
        else:
            ValueError('expecting either user_id or user_name to be not None')
            
        row_list= self.q(sql_str, params, 'ALL_ROWS')
        return row_list[0] if is_single else row_list

    def get_users_of(self, person_id=None, nick_name=None):
        if not person_id is None:
            # search by person_id
            return self.q('''
                SELECT * FROM `people` p WHERE p.person_id=:person_id
                ''',{'person_id':person_id},'ALL_ROWS')

        elif not nick_name is None:
            # search by nick name
            return self.q('''
                SELECT * FROM `people` p WHERE p.nick_name=:nick_name
                ''',{'nick_name':nick_name},'ALL_ROWS')

        else:
            raise Exception('must either provide person_id or nick_name')

    def get_people(self):
        return self.q('SELECT DISTINCT person_id,nick_name FROM people','ALL_ROWS')

    def add_user(self, person_id, user_info):
        if not self.validate_dict(user_info, ['user_name', 'user_id', 'flags'], on_extra='fail'):
            raise Exception('expected keys not found (or extra keys found) in user_info')
        user_info['person_id']= person_id

        insert_tuple= self._make_insert_clause(user_info)
        return self.q('''
            INSERT OR REPLACE INTO `people` %s
            ''' % insert_tuple[0], insert_tuple[1], 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True)


'''programmer-friendly interface to partitioned tweets'''        
class DTweets(object):
    # 0=no partitioning, 1=16 parts, 2=256 parts, n=2^(4n) parts
    DEFAULT_PARTITION_SCALE = 1
    
    def __init__(self,
            isolation_mode='DEFERRED', directory='../var/tweets',
            partition_scale=DEFAULT_PARTITION_SCALE, verbose=False):
        '''initializes a partitioned DTweets instance'''
        self.__directory = directory
        self.__parts = {}
        self.__verbose = verbose
        self.__isolation_mode = isolation_mode
        self.__partition_scale = partition_scale

    def __assert_dtweet_friendly(self, tweet):
        if isinstance(tweet, dict):
            if not 'tweet_id' in tweet and not 'plain_text' in tweet:
                raise ValueError('''
                    expecting tweet to be a dict containing "tweet_id" and 
                    "plain_text"
                    ''')
        elif isinstance(tweet, sqlite3.Row):
            if not 'tweet_id' in tweet.keys() and not 'plain_text' in tweet.keys():
                raise ValueError(
                        'expecting tweet to be a sqlite3.Row instance containing "tweet_id" and "plain_text"')
        else:
            raise TypeError('expecting tweet to be either a dict or sqlite3.Row')

    @classmethod
    def compute_hash(cls, tweet, binary_out=False):
        '''computes the MD5 hash for tweet
            tweet may be a dict, sqlite3.Row, textual/numerical tweet ID.
            Tweepy-returned tweet objects be processed with utils.prepare_DTweet_item() first
        '''
        tweet_id = None
        if isinstance(tweet, sqlite3.Row) and 'tweet_id' in tweet:
            tweet_id = tweet['tweet_id']
        elif isinstance(tweet, dict) and 'tweet_id' in tweet:
            tweet_id = tweet['tweet_id']
        elif isinstance(tweet, basestring):
            tweet_id = tweet
        elif isinstance(tweet, int):
            tweet_id = int(tweet)
        else:
            raise TypeError('expecting tweet to be an instance of sqlite3.Row, dict, basestring or int')

        m = hashlib.md5()
        m.update(str(tweet_id))
        return m.digest() if binary_out else m.hexdigest()

    @classmethod
    def compute_partition_name(cls, tweet, partition_scale=DEFAULT_PARTITION_SCALE):
        '''for a tweet, returns the N-character long partition name'''
        if not partition_scale:
            # 0 or None = no partition
            return 'data'
        hash_str = cls.compute_hash(tweet)
        return hash_str[0:partition_scale]

    def __make_db_fullpath(self, tweet=None, partition_name=None): 
        '''generates filename for database, given tweet obj (dtweet-friendly dict)'''
        if not partition_name:
            partition_name = self.__class__.compute_partition_name(
                tweet, self.__partition_scale) 
        return os.path.join(self.__directory, '%s.db' % partition_name) 

    def __get_file_list(self):
        '''get a list of ('FFFF', 'spam/tweets/FFFF.db files') tuples'''
        file_list = []
        for f in os.listdir(self.__directory):
            if os.path.isfile(f):
                f_full = os.path.abspath(f)
                db_filename_wo_ext = os.path.splitext(os.path.basename(f_full))[0]
                file_list.append((db_filename_wo_ext, f_full))
        return file_list

    def __get_part_instance(self, tweet=None, part_name=None):
        '''instantiates a DTweets_part using a tweet or a partition name; then
            add it to the partition-instance list
        '''
        if part_name:
            if part_name in self.__parts:
                return self.__parts[part_name]
        else:
            part_name = self.__class__.compute_partition_name(tweet)
            if part_name in self.__parts:
                return self.__parts[part_name]
            
        inst = DTweets_part(
                filename=self.__make_db_fullpath(tweet, part_name),
                isolation_mode=self.__isolation_mode,
                verbose=self.__verbose)
        self.__parts[part_name] = inst
        return self.__parts[part_name]

    def __close_part_instance(self, part_name=None):
        '''closes currently-opened connections'''
        if part_name:
            self.__parts[part_name].close()
        else:
            for inst in self.__parts:
                self.__parts[inst].close()

    @classmethod
    def __tidy_todo_list(cls, in_list):
        '''groups a bunch of tweet objects/IDs by partition.
            returns dict {'partname1':[tweet1, tweet2,...], ...}
        '''
        out_dict = {}
        for in_item in in_list:
            key = DTweets.compute_partition_name(in_item)
            if not key in out_dict:
                out_dict[key] = []
            out_dict[key] = in_item
        return out_dict

    def q(self, query, params=None, result_type='ALL_ROWS', auto_commit=False,
            partition_name=None):
        '''executes a query on one or more partitions; to execute on all 
            partitions, leave partition_name None.
        '''
        todo_list = []
        if not partition_name:
            # perform query on all partitions
            file_list = self.__get_file_list()
            for tup in file_list:
                # example: tup = ('FFFF', 'spam/tweets/FFFF.db')
                todo_ist.append(self.__get_part_instance(part_name=tup[0]))
        elif isinstance(partition_name, list):
            for partition_name_ in partition_name:
                todo_list.append(
                        self.__get_part_instance(part_name=partition_name_str))
        else:
            todo_list = [self.__get_part_instance(part_name=partition_name)]
        # call q() on each partition 
        result_list = []
        for part_inst in todo_list:
            result_list.append(
                    part_inst.q(query, params, result_type, auto_commit))
        # result_list = [rowset1, rowset2, 1, ...]
        self.__close_part_instance()
        return result_list    

    def insert(self, tweet_obj):
        '''inserts one or more tweets into the partitoned database.
            returns 1 or list of 1 on success
        '''
        todo_list = []
        if isinstance(tweet_obj, list):
            todo_list = tweet_obj
        else:
           todo_list = [tweet_obj]
        # group tweets by partitions 
        todo_dict = self.__class__.__tidy_todo_list(todo_list)
        # execute each query
        result_list = [] 
        for partition_name, tweet_list in todo_dict.iteritems():
            part = self.__get_part_instance(part_name=partition_name)
            res = part.insert(tweet_list)
            # important! close the connection or you will get random OperationalError
            result_list.append(res)
        
        self.__close_part_instance()
        return result_list if isinstance(tweet_obj, list) else result_list[0]   

    def get_by_id(self, tweet_id):
        '''get tweet details of one or more tweet ids
            returns sqlite3.Row or list of sqlite3.Row's 
        ''' 
        todo_list = tweet_id if isinstance(tweet_obj, list) else [tweet_id]
        todo_dict = self.__class__.__tidy_todo_list(todo_list)
        result_list = []

        for partition_name, tweet_list in todo_dict.iteritems():
            part = self.__get_part_instance(part_name=partition_name)
            res = part.insert(tweet_list)
            result_list.append(res)      
            
        self.__close_part_instance()
        return result_list if isinstance(tweet_obj, list) else result_list[0]   

    def apply_to_all_tweets(self, callback, *args, **kwargs):
        '''callback will be called with params: (tweet_obj, *args, **kwargs).
            return values of callbacks are stored in a list then finally
            returned. to stop iteration, have callback return False
        '''
        if not hasattr(callback, '__call__'):
            raise TypeError('expecting callback to be callable')

        give_up = False
        result_list = []
        for tup in self.__get_file_list():
            # example: tup = ('FFFF', 'spam/tweets/FFFF.db')
            if give_up:
                break
            inst = self.__get_part_instance(part_name=tup[0])
            tweet_list = inst.get_all()
            inst.close()

            if isinstance(tweet_list, list):
                for tweet in tweet_list:
                    result = callback(tweet, *args, **kwargs)
                    if not result:
                        give_up = True
                        break
                    else:
                        result_list.append(result)
            else:
                raise Exception('unable to get a list of tweets')
        return result_list        

    def apply_to_all_partitions(self, callback, auto_close=True, *args, **kwargs):
        '''callback will be called with params: (part_name, part_inst, *args, **kwargs).
            return values of callbacks are stored in a list then finally
            returned. to stop iteration, have callback return False
        '''
        if not hasattr(callback, '__call__'):
            raise TypeError('expecting callback to be callable')

        result_list = []
        for tup in self.__get_file_list():
            # example: tup = ('FFFF', 'spam/tweets/FFFF.db')
            inst = self.__get_part_instance(part_name=tup[0])
            result = callback(tup[0], inst, *args, **kwargs)
            inst.close()
            if not result:
                break
            else:
                result_list.append(result)

        if auto_close:
            self.__close_part_instance() 
        return result_list        


'''internal partitioned tweet database'''
class DTweets_part(DObject):
    # since we'll be generating filename at runtime, leaving this blank
    DB_FILENAME = None  
    
    INIT_QUERIES = ['''
        CREATE TABLE IF NOT EXISTS tweets(
            tweet_id INTEGER PRIMARY KEY,
            user INTEGER,
            in_reply_to_tweet INTEGER,
            in_reply_to_user INTEGER,
            plain_text TEXT,
            html_text TEXT,
            xml_text TEXT,
            coordinates TEXT,
            date TEXT,
            is_retweet INTEGER,
            source TEXT,
            retweeted_count INTEGER,    /* subject to change! */
            fav_count INTEGER,          /* subject to change! */
            is_my_fav INTEGER,          /* subject to change! */
            last_update TEXT          
        )
        ''','''
        CREATE INDEX IF NOT EXISTS tweets__tweet_id ON tweets(
            tweet_id
        )
        ''','''
        CREATE INDEX IF NOT EXISTS tweets__user ON tweets(
            user 
        )
        ''','''
        CREATE INDEX IF NOT EXISTS tweets__in_reply_to_user ON tweets(
            in_reply_to_user 
        )
        ''','''
        CREATE INDEX IF NOT EXISTS tweets__in_reply_to_tweet ON tweets(
            in_reply_to_tweet
        )
        ''','''
        CREATE TRIGGER IF NOT EXISTS tweets_last_updated
        AFTER UPDATE ON tweets 
        BEGIN
            UPDATE tweets SET last_update = DATETIME('now');
        END;
        ''','''
        CREATE TRIGGER IF NOT EXISTS tweets_last_updated2
        AFTER INSERT ON tweets 
        BEGIN
            UPDATE tweets SET last_update = DATETIME('now');
        END;
        ''']
    ROW_REQUIREMENT = [
            'tweet_id*',
            'plain_text',
            ('html_text', ''),
            ('xml_text', ''),
            ('coordinates#', ''),
            'date', 
            ('in_reply_to_tweet', None),
            ('in_reply_to_user', None),
            'user',
            ('is_retweet', 0),
            ('source', ''),
            ('retweeted_count', 0),
            ('fav_count', 0),
            ('is_my_fav', 0),
            # `last_update` is automatically updated and does not require input
            ]
        
    def __init__(self, filename='../var/tweets/data.db', isolation_mode='DEFERRED',
            verbose=False):
        init_queries = self.INIT_QUERIES
        
        super(DTweets_part, self).__init__(
                (filename, isolation_mode), init_queries, verbose)

    def get_by_id(self, tweet_id):
        '''get one tweet given one tweet_id; returns one sqlite3.Row instance'''
        if not (isinstance(tweet_id, int) or isinstance(tweet_id, basestring)):
            raise TypeError('expecting tweet_id to be an int or string')

        return self.q('SELECT * FROM tweets WHERE tweet_id=:tweet_id',
               {'tweet_id': tweet_id}, 'ONE_ROW')

    def get_by_user(self, user_id):
        '''get tweets given one user or more users (give a list in user_id)
            returns list of sqlite3.Row instance
        '''
        if isinstance(user_id, int) or isinstance(user_id, basestring):
            return self.q('SELECT * FROM tweets WHERE user=:user_id',
                   {'user_id': user_id}, 'ALL_ROWS')
        elif isinstance(user_id, list):
            (sql, par) = self.__class__._make_in_clause('user', user_id)
            return self.q('SELECT * FROM tweets WHERE %s' % (sql), 
                    par, 'ALL_ROWS'
                    )
        else:
            raise TypeError('expecting user_id to be int, string, or list instance')

    def get_replies_of(self, tweet_id=None, user_id=None):
        '''get reply-tweets given one tweet_id or one user_id
            returns list of sqlite3.Row instance
        '''
        pars = {}
        if tweet_id is not None: 
            col_name = 'in_reply_to_tweet'
            pars = {'valu': tweet_id}
        elif user_id is not None:
            col_name = 'in_reply_to_user'
            pars = {'valu': user_id}
        else:
            raise ValueError('expecting tweet_id or user_id to be non-None')

        return self.q('SELECT * FROM tweets WHERE %s=:valu' % (col_name), 
                pars, 'ALL_ROWS'
                )

    def get_all(self):
        '''gets a list of sqlite3.Row objects for everything in the table'''
        return self.q('SELECT * FROM tweets', result_type='ALL_ROWS')

    def __preflight_tweet_list(self, tweet_list):
        '''make sure the tweet list is OK; raises exception if not'''
        # preflight
        for tweet_dict in tweet_list:
            # columns OK?
            if not self.validate_dict(tweet_dict, self.ROW_REQUIREMENT, on_extra='discard'):
                raise Exception('expected keys not found in tweet_dict')

    def update(self, tweets, auto_close=True):
        '''update entries. tweets must be a list of dicts, or a dict representing a 
            row in the table. Returns true on success (or a list of bools)
        '''
        lst = tweets if isinstance(tweets, list) else [tweets]
        self.__preflight_tweet_list(lst)
        result = []

        # update one by one; but do everything in one transaction to maintain speed 
        failed = False
        for tweet_dict in lst:
            tweet_id = tweet_dict['tweet_id']
            (set_clause, params) = self._make_set_clause(
                    tweet_dict, self.ROW_REQUIREMENT)
            this_result = self.q(
                    'UPDATE tweets SET %s WHERE tweet_id=:tweet_id' % set_clause,
                    [params, {'tweet_id': tweet_id}], 
                    result_type='NUMBER_OF_ROWS_AFFECTED', auto_commit=False)
            result.append('this_result')
        
        if auto_close:
            self.close()
        return result if isinstance(tweets, list) else result[0]

    def update_perspective_fields(self, tweet_id=None, retweeted_count=None,
            fav_count=None, is_my_fav=None, items=None, auto_close=True):
        '''updates perspective fields in one or more rows. only provided fields
            are updated. to update multiple rows, set items = {'tweet_id1': {..}}
            returns a dict of {'tweet_id1':True,..} if items set, or just bool
        '''
        if not items:
            items = {tweet_id: {}}
            if retweeted_count is not None:
                items[tweet_id]['retweeted_count'] = retweeted_count
            if fav_count is not None:
                items[tweet_id]['fav_count'] = fav_count
            if is_my_fav is not None:
                items[tweet_id]['is_my_fav'] = is_my_fav 

        sql = 'UPDATE tweets SET %s WHERE tweet_id=:tweet_id'
        result_dict = {} 
        for k, v in items.iteritems():
            (set_clause, params_dict) = self._make_set_clause(v)
            this_result = self.q(
                    'UPDATE tweets SET %s WHERE tweet_id=:tweet_id' % set_clause,
                    [params, {'tweet_id': k}],
                    result_type='NUMBER_OF_ROWS_AFFECTED', auto_commit=False)
            result_dict[k] = this_result
        #
        if auto_close:
            self.close()
        return result_dict if items else result_dict[tweet_id]    

    def insert(self, tweets, on_operational_error='fail'):
        '''inserts one or more tweets into the table. returns True if 
            all items are properly inserted.
            on_operational_error = 'fail' or 'continue'
            Note: Please close the connection after finished inserting!
        '''
        lst = tweets if isinstance(tweets, list) else [tweets]
        self.__preflight_tweet_list(lst)
        
        # add one by one; but do everything in one transaction to maintain speed 
        failed = False
        for tweet_dict in lst:
            insert_tuple = self._make_insert_clause(tweet_dict)
            insert_res = False
            try:
                insert_res = self.q(
                        '''INSERT OR REPLACE INTO tweets %s''' % insert_tuple[0], 
                        insert_tuple[1], 'NUMBER_OF_ROWS_AFFECTED',
                        auto_commit=False
                        )
            except sqlite3.OperationalError as e:
                sys.stderr.write('unable to insert: %s\n' % str(e))
                if on_operational_error == 'continue':
                    sys.stderr.write('...ignoring and continue\n')
                    continue
                elif on_operational_error == 'fail':
                    failed = True
                    break
                else:
                    raise ValueError('''
                        expecting on_operational_error to be "continue" or "fail"
                        ''')
            except Exception as e:
                sys.stderr.write('exception: ' + str(e))
                failed = True
                break

            if not insert_res:
                failed = True
                break
            
        # end of transaction
        if failed:
            self.get_db().rollback()
            return False
        else:
            self.get_db().commit()
            return True


class DThread(DObject):
    DB_FILENAME= 'tt_main.db'
    INIT_QUERIES= ['''
        CREATE TABLE IF NOT EXISTS `threads`(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id INTEGER,
            root_tweet_id INTEGER,
            parent_tweet_id INTEGER
        )
        ''','''
        CREATE INDEX IF NOT EXISTS threads__root ON threads(
            root_tweet_id
        )
        ''','''
        CREATE INDEX IF NOT EXISTS threads__parent ON threads(
            parent_tweet_id
        )
        ''']

    def __init__(self, directory='../var'):
        super(DThread,self).__init__(directory+'/'+ self.DB_FILENAME, self.INIT_QUERIES+DTweets.INIT_QUERIES)

    def get_thread_tree(self, tweet_id, get_content_bool=False):
        pass

    def get_child_count(self, tweet_id):
        pass


class DSchedules(DObject):
    DB_FILENAME= 'tt_setup.db'
    INIT_QUERIES= ['''
        CREATE TABLE IF NOT EXISTS `schedules`(
            schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            interval INTEGER,
            last_run INTEGER,
            profile_id INTEGER,
            action TEXT,
            target TEXT,
            priority INTEGER
        )
        ''','''
        CREATE INDEX IF NOT EXISTS schedules__last_run ON schedules(
            last_run 
        )
        ''','''
        CREATE INDEX IF NOT EXISTS schedules__action ON schedules(
            action, target, interval 
        ''']

    def __init__(self, directory='../var'):
        super(DSchedules,self).__init__(directory+'/'+self.DB_FILENAME, self.INIT_QUERIES)

    def insert(self, schedule_info):
        insert_tuple= self._make_insert_clause(schedule_info,['interval','profile_id','action','target','priority'])
        return self.q('''
            INSERT OR REPLACE INTO schedule %s
            ''' % insert_tuple[0], insert_tuple[1], 'LAST_ROWID')
    
    def get_schedules(self, runnable_only=True):
        '''get a list of scheduled tasks (at this moment). returns a list of dicts'''
        return self.q('''
            SELECT * FROM schedules s
            WHERE %s
            ORDER BY s.priority DESC
            ''' % '''
            s.last_run IS NULL OR s.last_run <= strftime('%s', 'now') - s.interval
            ''' if runnable_only else '1', {}, 'ALL_ROWS')

    def set_run_just_now(self, schedule_id):
        current_timestamp= int(time.time())
        update_tuple= self._make_set_clause({'last_run': current_timestamp})
        return 1== self.q('''
            UPDATE schedules SET %s WHERE schedule_id=:schedule_id
            '''% update_tuple[0],
            [{'schedule_id':schedule_id}, update_tuple[1]],
            'NUMBER_OF_ROWS_AFFECTED', 
            auto_commit=True
            )

    def delete(self, schedule_id):
        '''delete scheduled task by ID'''
        return self.q('''
            DELETE FROM schedules WHERE schedule_id=:schedule_id
            ''', {'schedule_id': schedule_id}, 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True)

    def update(self, schedule_id, schedule_info, reset_last_run=True):
        '''modifies scheduled task info'''
        schedule_info2= schedule_info.copy()
        if reset_last_run:
            schedule_info2['last_run']= None

        update_tuple= self._make_set_clause(
                schedule_info2, 
                accepted_columns_list=['interval','profile_id','action','target','priority']
                )

        return 1 == self.q('''
            UPDATE schedules SET %s WHERE schedule_id=:schedule_id
            ''' % update_tuple[0], 
            [{'schedule_id':schedule_id}, update_tuple[1]], 
            'NUMBER_OF_ROWS_AFFECTED',
            auto_commit=True
            )

'''
    Non-partitioned store of tweet-timeline relationships.
    Depending on how the tweet was fetched, it can appear on more than one timelines
'''
class DTimelines(DObject):
    DB_FILENAME='tt_timelines.db'
    INIT_QUERIES = ['''
        CREATE TABLE IF NOT EXISTS timelines(
            tweet_id INTEGER PRIMARY KEY,
            home_timeline INTEGER,       /* null, or set to authenticating user ID */
            user_timeline INTEGER,       /* null, or set to owner user ID */
            mentions_timeline INTEGER,   /* null, or set to authenticating user ID */
            last_update TEXT
        )
        ''','''
        CREATE INDEX IF NOT EXISTS timelines__home_timeline ON timelines(
            home_timeline
        )
        ''','''
        CREATE INDEX IF NOT EXISTS timelines__user_timeline ON timelines(
           user_timeline 
        )
        ''','''
        CREATE INDEX IF NOT EXISTS timelines__mentions_timeline ON timelines(
           mentions_timeline 
        )
        ''','''
        CREATE TRIGGER IF NOT EXISTS timelines_last_updated
        AFTER UPDATE ON timelines 
        BEGIN
            UPDATE timelines SET last_update = DATETIME('now');
        END;
        ''']
    ROW_REQUIREMENT = [
            'tweet_id',
            ('home_timeline', None),
            ('user_timeline', None),
            ('mentions_timeline', None),
            # last_update is automatically updated and does not require input
            ]

    def __init__(self, directory='../var', verbose=False):
        super(DTimelines, self).__init__(
                os.path.join(directory, self.DB_FILENAME),
                self.INIT_QUERIES, 
                verbose)
    
    def insert(self, tweet_ids, home_timeline=1, user_timeline=None,
            mentions_timeline=None, auto_close=True):
        '''insert one or more tweet_ids; do not pass a tweet-dict list directly.
            call utils.extract_tweet_id() to preprocess a tweet-dict list
        '''
        lst = tweet_ids if isinstance(tweet_ids, list) else [tweet_ids]
        result_dict = {}
        fail_count = 0
        
        kv = make_dict([
            ('tweet_id', 0),
            ('home_timeline', home_timeline),
            ('user_timeline', user_timeline),
            ('mentions_timeline', mentions_timeline),
            ], omit_if_none=True)

        for tweet_id in lst:
            if isinstance(tweet_id, int) or isinstance(tweet_id, str): 
                kv['tweet_id'] = tweet_id
            else:
                raise TypeError('expecting tweet_id to be int or str')
            (columns_clause, params) = self.__class__._make_insert_clause(kv)
            res = self.q('INSERT OR REPLACE INTO timelines %s' % columns_clause, 
                    params, 'NUMBER_OF_ROWS_AFFECTED', auto_commit=False)
            if not res:
                fail_count += 1
            result_dict[tweet_id] = res
        
        if fail_count:
            self.get_db().rollback
        else:
            self.get_db().commit()
        if auto_close:
            self.close()

        # returns a list of results, or one result 
        return result_dict if isinstance(tweet_ids, list) else (
                result_dict[tweet_ids])

    def update(self, tweet_ids, home_timeline=1, user_timeline=None,
            mentions_timeline=None, auto_close=True):
        lst = tweet_ids if isinstance(tweet_ids, list) else [tweet_ids]
        result_dict = {}
        fail_count = 0

        kv = make_dict([
            ('tweet_id', 0),
            ('home_timeline', home_timeline),
            ('user_timeline', user_timeline),
            ('mentions_timeline', mentions_timeline),
            ], omit_if_none=True)

        for tweet_id in lst:
            kv['tweet_id'] = tweet_id
            (set_clause, params) = self.__class__._make_set_clause(kv,
                    self.ROW_REQUIREMENT)
            res = self.q('''
                    UPDATE timelines SET %s WHERE tweet_id=:tweet_id
                    ''' % columns_clause, 
                    params, 'NUMBER_OF_ROWS_AFFECTED',
                    auto_commit=False)
            if not res:
                fail_count += 1
            result_dict[tweet_id] = res
        
        if fail_count:
            self.get_db().rollback
        else:
            self.get_db().commit()
        if auto_close:
            self.close()

        # returns a list of results, or one result 
        return result_dict if isinstance(tweet_ids, list) else (
                result_dict[tweet_ids])
    
    def get_home_timeline(self):
        '''gets a list of tweet_IDs corresponding to the home timeline.
            returns False on failure
        '''
        rows = self.q('SELECT tweet_id FROM timelines WHERE home_timeline=1',
                None, 'ALL_ROWS')
        if rows:
            res_list = []
            for row in rows:
                res_list.append(rows['tweet_id'])
            return res_list    
        else:
            return False

    def get_user_timeline(self, user_id):
        '''gets a list of tweet_IDs corresponding to user_id's timeline.
            returns False on failure
        '''
        rows = self.q('''
            SELECT tweet_id FROM timelines WHERE user_timeline=:user_id
            ''', {'user_id': user_id}, 'ALL_ROWS')
        if rows:
            res_list = []
            for row in rows:
                res_list.append(rows['tweet_id'])
            return res_list    
        else:
            return False

    def get_mentions_timeline(self, user_id):
        '''gets a list of tweet_IDs corresponding to user_id's mentions 
            timeline. Returns False on failure
        '''
        rows = self.q('''
            SELECT tweet_id FROM timelines WHERE mentions_timeline=:user_id
            ''', {'user_id': user_id}, 'ALL_ROWS')
        if rows:
            res_list = []
            for row in rows:
                res_list.append(rows['tweet_id'])
            return res_list    
        else:
            return False




