import sqlite3
import string
import random
import os
import time

# TODO: create singleton to reuse database connection (if same DSN)
class DObject(object):
    def get_db(self):
        """get database instance; if not established then create"""
        if not isinstance(self.__db, sqlite3.Connection):
            self.__db= sqlite3.connect(self.__dsn[0], isolation_level=self.__dsn[1])
            self.__db.row_factory= sqlite3.Row
            self.__run_init_queries()
        return self.__db 

    def __run_init_queries(self):
        """initializes DB; just have a connection before calling this method"""
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
            print "no initial queries"
            return True
    
    def __init_dsn(self,dsn):
        if isinstance(dsn,tuple):
            self.__dsn= dsn
        elif isinstance(dsn,str):
            self.__dsn=(dsn,'DEFERRED')
        # ensure directory is useable; if not, raise an exception
        dirname = os.path.dirname(self.__dsn[0])
        if not os.path.isdir(dirname):
            try:
                os.makedirs(dirname)
            except:
                raise Exception('unable to create directory %s' % dirname)

    def __init__(self, dsn, init_queries=None):
        """initializes a DObject"""
        self.__init_queries= init_queries
        self.__db= None
        self.__init_dsn(dsn)

    def q_multiple(self, q_list, result_type='ALL_ROWS', auto_commit=False):
        """executes more than one queries, and return a list of results"""
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
            print "Committing..."
            self.__db.commit()

        return result_list

    def q(self, query, params=None, result_type='ALL_ROWS', auto_commit=False):
        """
            executes a query.
            query: string
                one single SQL statement
            params: dict or tuple
                for named parameters, params={'param1':xx,'param2':yy, ...}
                for unnamed parameters, params=(xx, yy, ..)
                concatenate tuples and dicts with a list [{..},{..},..] 
            result_type: string or int
                should be "ALL_ROWS", "ONE_ROW", N, "NUMBER", "CURSOR", 'LAST_ROWID','NUMBER_OF_ROWS_AFFECTED'
        """
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
        cursor= self.get_db().cursor()
        print 'Query: '+query
        if params is None:
            print '(no parameters)'
        else:
            print 'Parameters:'
            print(params)
        
        if params is None:
            cursor.execute(query)
        else:
            cursor.execute(query, params_)

        if 'NUMBER'== result_type:
            row= cursor.fetchone()
            result= None if row is None else row[0]
        elif 'ONE_ROW'== result_type:
            result= cursor.fetchone()
        elif 'ALL_ROWS'== result_type:
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
            print "Committing..."
            self.__db.commit()
        print "Returning:"
        print(result)
        return result

    @classmethod
    def _make_set_clause(cls, kv_pairs_dict, accepted_columns_list=None, table_name='', parameter_prefix_str='par'):
        """generates a 'colA=:par1, colB=:par2,...' and parameter dictionary"""
        sql_str_list= []
        params_dict= {}
        par_count= 0

        for column_name in kv_pairs_dict:
            if accepted_columns_list is None or column_name in accepted_columns_list: 
                param_name= '%s_%s' % (parameter_prefix_str, par_count)
                if table_name== '':
                    sql_str_list.append('`%s`=:%s' % (column_name, param_name))
                else:
                    sql_str_list.append('`%s`.`%s`=:%s' % (table_name, column_name, param_name))
                params_dict[param_name]= kv_pairs_dict[column_name]
                par_count+= 1

        return (','.join(sql_str_list), params_dict)

    @classmethod
    def _make_insert_clause(cls, kv_pairs_dict, parameter_prefix_str='par'):
        """generates a '(col1,col2,..) VALUES (:par1,:par2,...)' and parameter dictionary"""
        column_names_list=[]
        par_names_list= []
        params_dict= {}
        par_count= 0

        for column_name in kv_pairs_dict:
            param_name= '%s_%d' % (parameter_prefix_str, par_count)
            par_names_list.append(':'+param_name)
            column_names_list.append('`%s`' % column_name)
            params_dict[param_name]= kv_pairs_dict[column_name]
            par_count+= 1

        return ('(%s) VALUES (%s)' % (','.join(column_names_list), ','.join(par_names_list)), params_dict)
    
    @classmethod
    def _make_in_clause(cls, data_list, parameter_prefix_str='par'):
        """makes a 'IN (:par1,:par2,...)' and {'par1':xx,'par2':xx} tuple"""
        par_names_list= []
        params_dict= {}
        par_count= 0

        for data_list_item in data_list:
            param_name= '%s_%d' % (parameter_prefix_str, par_count)
            params_dict[param_name]= data_list_item
            par_names_list.append(':'+param_name);
            par_count+= 1

        return ('IN (%s)' % ','.join(par_names_list), params_dict)

    @classmethod
    def _has_all_keys(cls, subject_dict, search_for, fail_if_extra=False):
        """returns true if subject_dict contains all keys in search_for_list;
            search_for: list of 'key' or list of ('key','default_value')
        """
        for wanted in search_for:
            if isinstance(wanted,tuple) and len(wanted)==2:
                if not wanted[0] in subject_dict:
                    subject_dict[wanted[0]] = wanted[1]
            elif isinstance(wanted,basestring):
                if not wanted in subject_dict:
                    return False
            else:
                raise TypeError('search_for contains an unrecognized type; expecting string or 2-tuple')
        # if a key in subject is not expected, then return False
        if fail_if_extra:
            for key in subject_dict:
                if not key in search_for_list:
                    return False

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
    FLAG_NONE= 0
    FLAG_REQUESTED= 1
    FLAG_AUTHENTICATED= 2
    def __init__(self, directory='../var'):
        super(DProfiles,self).__init__(directory+'/'+self.DB_FILENAME, self.INIT_QUERIES)

    def insert(self, profile_info):
        """inserts a row into the profiles table; profile_info = {'profile_alias':xx,'auth_data':xx,'user_id':xx}"""
        if not self._has_all_keys(profile_info,['profile_alias','auth_data','user_id','auth_flag','purpose','priority'],fail_if_extra=True):
            raise Exception('expected keys not found (or extra keys found) in profile_info')

        insert_tuple= self._make_insert_clause(profile_info)    # insert_tuple=(sql_str,param_dict)
        return self.q("""
            INSERT OR REPLACE INTO `profiles` %s 
            """ % insert_tuple[0], insert_tuple[1], 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True)

    def get_profile(self, profile_id):
        return self.q('SELECT * FROM profiles WHERE profile_id=:profile_id', {'profile_id': profile_id}, 'ONE_ROW')
    
    def get_profiles(self, auth_flag=None):
        if auth_flag is None:
            return self.q('SELECT * FROM profiles', {}, 'ALL_ROWS')
        else:
            return self.q('SELECT * FROM profiles WHERE auth_flag=:auth_flag', {'auth_flag':auth_flag}, 'ALL_ROWS')

    def delete(self, profile_alias):
        return self.q("""
            DELETE FROM profiles WHERE profile_alias=:profile_alias
            """, {'profile_alias': profile_alias}, 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True)

    def update(self, profile_alias, profile_info):
        update_tuple= self._make_set_clause(profile_info,['profile_alias','auth_data','user_id',('purpose',''),'priority','auth_flag'])
        return self.q("""
            UPDATE profiles SET %s WHERE profile_alias=:profile_alias
            """ % update_tuple[0], [{'profile_alias':profile_alias}, update_tuple[1]], 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True)


class DPeople(DObject):
    DB_FILENAME= 'tt_main.db'
    INIT_QUERIES= ['''
        CREATE TABLE IF NOT EXISTS `people`(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER,
            nick_name TEXT,
            user_name TEXT,
            user_id INTEGER,
            flags INTEGER
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

    def __init__(self, directory='../var'):
        super(DPeople,self).__init__(directory+'/'+self.DB_FILENAME, self.INIT_QUERIES)

    def insert(self, person_info):
        """inserts a new person; returns person_id"""
        if not 'nick_name' in person_info:
            person_info['nick_name']= person_info['user_name']
        
        if not self._has_all_keys(user_info, ['user_name', 'user_id', 'flags','nick_name'], fail_if_extra=True):
            raise Exception('expected keys not found (or extra keys found) in user_info')
        
        insert_tuple= self._make_insert_clause(user_info)
        # do not check for existing user; faster
        last_rowid= self.q("""
            INSERT OR REPLACE INTO `people` p1 %s
            """ % insert_tuple[0], insert_tuple[1], 'LAST_ROWID', auto_commit=False)
    
        if 0== self.q("""
            UPDATE people SET person_id=:person_id WHERE id=:id
            """, {'person_id': last_rowid, 'id': last_rowid}, 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True):
            self.get_db().rollback()
            return False

        return last_rowid

    def get_person_from_user(self, user_id=None, user_name=None):
        """
            get person_id and nick_name for given user_name(s). to retrieve multiple user_names, use [name1,name2,..].
            returns a dict {person_id:xx,nick_name:xx,user_id:xx} or a list of dicts
        """
        sql_str= ''
        params= {}
        is_single= True
        if user_id is not None:
            if isinstance(user_id,list):
                sql_param_tuple= self._make_in_clause(user_id)
                sql_str= 'SELECT person_id,nick_name,user_id FROM people WHERE user_id %s'% sql_param_tuple[0]
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
            throw ValueError('expecting either user_id or user_name to be not None')
            
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
        if not self._has_all_keys(user_info, ['user_name', 'user_id', 'flags'], fail_if_extra=True):
            raise Exception('expected keys not found (or extra keys found) in user_info')
        user_info['person_id']= person_id

        insert_tuple= self._make_insert_clause(user_info)
        return self.q("""
            INSERT OR REPLACE INTO `people` %s
            """ % insert_tuple[0], insert_tuple[1], 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True)

class DTweets(DObject):
    DB_FILENAME= 'tt_main.db'
    INIT_QUERIES= ['''
        CREATE TABLE IF NOT EXISTS `tweets`(
            tweet_id INTEGER PRIMARY KEY,
            timeline_owner INTEGER,
            profile_id INTEGER,
            created_by INTEGER,
            in_reply_to_tweet INTEGER,
            in_reply_to_user INTEGER,
            date TEXT,
            html_text TEXT,
            plain_text TEXT
        )
        ''','''
        CREATE TABLE IF NOT EXISTS `mentions`(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id INTEGER,
            user_id INTEGER
        )
        ''','''
        CREATE INDEX IF NOT EXISTS tweets__in_reply_to_tweet ON tweets(
            in_reply_to_tweet
        )
        ''','''
        CREATE INDEX IF NOT EXISTS tweets__in_reply_to_user ON tweets(
            in_reply_to_user
        )
        ''','''
        CREATE INDEX IF NOT EXISTS mentions__tweet_id ON mentions(
            tweet_id
        )
        ''']

    def __init__(self, directory='../var'):
        init_queries= self.INIT_QUERIES +DPeople.INIT_QUERIES +DProfiles.INIT_QUERIES
        super(DTweets,self).__init__(directory+'/'+ self.DB_FILENAME, init_queries)
    
    def get_timeline_of(self, person_id=None, user_id=None):
        pass

    def get_tweets_of(self, person_id=None, user_id=None):
        pass

    def get_mentions_of(self, person_id=None, user_id=None):
        pass

    def get_replies(self, tweet_id):
        pass

    def get_reply_count(self, tweet_id=None, user_id=None, person_id=None):
        if not tweet_id is None:
            return self.q("""SELECT COUNT(tweet_id) FROM tweets WHERE in_reply_to_tweet=:tweet_id""", {'tweet_id':tweet_id}, 'NUMBER')
        elif not user_id is None:
            return self.q("""SELECT COUNT(tweet_id) FROM tweets WHERE in_reply_to_user=:user_id""", {'user_id':user_id}, 'NUMBER')
        elif not person_id is None:
            return self.q("""SELECT COUNT(tweet_id) FROM tweets WHERE in_reply_to_user=:user_id""", {'user_id':user_id}, 'NUMBER')
        else:
            raise Exception('either tweet_id, user_id, person_id should be not None')

    def get_tweet(self, tweet_id):
        """ get a tweet by tweet ID"""
        return self.q("""
            SELECT t.* FROM tweets t
            OUTER JOIN profiles pr ON t.profile_id=pr.profile_id
            WHERE t.tweet_id=:tweet_id""", {'tweet_id':tweet_id}, 'ONE_ROW')
    
    def get_tweets(self, tweet_id=None, user_id=None, person_id=None, timeline_owner=None):
        """select multiple tweets. provide timeline_owner or (user_id or person_id)"""
        if timeline_owner is None:
            
            pass
        else:
            pass

    def __insert_tweet(self, tweet, profile=None):
        """add one tweet to the database; returns True if insertion was successful"""
        data_dict= {}

        # profile can be either a profile row or an integer
        if isinstance(profile,dict) and 'profile_id' in dict:
            data_dict['profile_id']= profile['profile_id']
        else:
            data_dict['profile_id']= profile
        
        # prepare tweet data
        if not isinstance(tweet,dict):
            raise TypeError('expecting tweet to be a dict')
            
        # execute query
        return 1== self.q("""
            INSERT OR REPLACE INTO `tweets`
            (tweet_id, timeline_owner, profile_id, created_by, in_reply_to_tweet, in_reply_to_user, date, html_text, plain_text)
            VALUES (:tweet_id, :timeline_owner, :profile_id, :created_by, :in_reply_to_tweet, :in_reply_to_user, :date, :html_text, :plain_text)
        """, data_dict, 'NUMBER_OF_ROWS_AFFECTED') 

    def insert_tweets(self, tweets, profile=None):
        """add one or more tweets to the database"""
        tweet_list= None
        if isinstance(tweets,dict):
            return self.__insert_tweet(self, tweets, authenticating_user)
        elif isinstance(tweets,list):
            result_list= []
            result_list.append(self.__insert_tweet(tweet, authenticating_user))
            return result_list
        else:
            raise TypeError('expecting a dict or list')


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
        return self.q("""
            INSERT OR REPLACE INTO schedule %s
            """ % insert_tuple[0], insert_tuple[1], 'LAST_ROWID'])
    
    def get_schedules(self, runnable_only=True):
        """get a list of scheduled tasks (at this moment). returns a list of dicts"""
        return self.q("""
            SELECT * FROM schedules s
            WHERE %s
            ORDER BY s.priority DESC
            """ % """
            s.last_run IS NULL OR s.last_run <= strftime('%s', 'now') - s.interval
            """ if runnable_only else '1', {}, 'ALL_ROWS')

    def set_run_just_now(self, schedule_id):
        current_timestamp= int(time.time())
        update_tuple= self._make_set_clause({'last_run': current_timestamp})
        return 1== self.q("""
            UPDATE schedules SET %s WHERE schedule_id=:schedule_id
            """% update_tuple[0], [{'schedule_id':schedule_id},update_tuple[1]], 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True)

    def delete(self, schedule_id):
        """delete scheduled task by ID"""
        return self.q("""
            DELETE FROM schedules WHERE schedule_id=:schedule_id
            """, {'schedule_id': schedule_id}, 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True)

    def update(self, schedule_id, schedule_info, reset_last_run=True):
        """modifies scheduled task info"""
        schedule_info2= schedule_info.copy()
        if reset_last_run:
            schedule_info2['last_run']= None

        update_tuple= self._make_set_clause(schedule_info2, accepted_columns_list=['interval','profile_id','action','target','priority'])
        return 1== self.q("""
            UPDATE schedules SET %s WHERE schedule_id=:schedule_id
            """ % update_tuple[0], [{'schedule_id':schedule_id}, update_tuple[1]], 'NUMBER_OF_ROWS_AFFECTED', auto_commit=True)



