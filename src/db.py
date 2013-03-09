import sqlite3
import string
import random

class DObject(object):
    def get_db(self):
        """get database instance; if not established then create"""
        if isinstance(self.__db, sqlite3.Connection):
            return self.__db
        else:
            self.__db= sqlite3.connect(self.__db_filename, self.__isolation_level)
            self.__db.row_factory= sqlite3.Row
            self.__run_init_queries()

    def __run_init_queries(self):
        """initializes DB; just have a connection before calling this method"""
        # ensure we have a DB connection
        if not isinstance(self.__db, sqlite3.Connection):
            raise Exception('database connection not established')

        query_list= []
        if not self.__init_queries is None:
            if isinstance(self.__init_queries, str) or isinstace(self.__init_queries,tuple):
                query_list= [self.__init_queries]
            elif isinstance(self.__init_queries,list):
                query_list= self.__init_queries
            else:
                raise TypeError('expecting type: list, str, tuple(sql,param_dict)')

            for item in query_list:
                if isinstace(item,str):
                    self.q(self, item)
                elif isinstace(item,tuple):
                    self.q(self, item[0], item[1])
                else:
                    raise TypeError('expecting type: str, tuple(sql,param_dict)')

    def __init__(self, db_filename_str, init_queries=None, isolation_level='DEFERRED'):
        """
            initializes a DObject
            db_filename_str: string
                database filename
            init_queries: list
                a list of "str" or "tuple(sql,param_dict)"
        """
        # ensure directory is useable; if not, throw an exception
        dir = os.path.dirname(db_filename_str)
        if not os.path.isdir(dir):
            try:
                os.makedirs(dir)
            except:
                raise Exception('unable to create directory %s' % dir)

        self.__db_filename= db_filename_str
        self.__init_queries= init_queries
        self.__isolation_level= isolation_level
        self.__db= None

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
            self.db.commit()

        return result_list

    def q(self, query, params=None, result_type='ALL_ROWS', auto_commit=False):
        """
            executes a query.
            query: string
                one single SQL statement
            params: dict|tuple
                a key-value pair of parameters; key should be like ":spam"
            result_type: string|int
                should be "ALL_ROWS", "ONE_ROW", N, "NUMBER", "CURSOR", 'LAST_ROWID','NUMBER_OF_ROWS_AFFECTED'
        """
        if instanceof(query,list):
            return self.q_multiple(query, result_type, auto_commit)

        # -- type check --
        if not params is None:
            if not isinstance(params,dict) and not isinstance(params,tuple):
                raise TypeError('params should be a dict or tuple type')
        if not isinstance(query,str):
                raise TypeError('query should be a str')

        result= False
        cursor= self.get_db().cursor()
        cursor.execute(sql_str, params)
        if 'NUMBER'== result_type:
            row= cursor.fetchone()
            result= None if row is None else row[0]
        if 'ONE_ROW'== result_type:
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
            raise Exception('unknown result_type')

        if auto_commit:
            self.db.commit()

        return result

    @classmethod
    def _make_set_clause(cls, kv_pairs_dict, table_name='', parameter_prefix_str='par'):
        """generates a 'colA=:par1, colB=:par2,...' and parameter dictionary"""
        sql_str_list= []
        params_dict= {}
        par_count= 0

        for column_name in kv_pairs_dict:
            param_name= '%s_%s' % (parameter_prefix_str, par_count)
            if table_name== '':
                sql_str_list.append('`%s`=:%s' % (column_name, param_name) 
            else
                sql_str_list.append('`%s`.`%s`=:%s' % (table_name, column_name, param_name)
            
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
            param_name= '%s_%s' % (parameter_prefix_str, par_count)
            par_names_list.append(':'+param_name)
            column_names_list.append('`%s`' % column_name)
            params_dict[param_name]= kv_pairs_dict[column_name]
            par_count+= 1

        return ('(%s) VALUES (%s)' % (','.join(column_names_list), ','.join(par_names_list)), params_dict)
    
    @classmethod
    def _has_all_keys(cls, subject_dict, search_for_list, fail_if_extra=False):
        """returns true if subject_dict contains all keys in search_for_list"""
        for key in search_for_list:
            if not key in subject_dict:
                return False
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
            tokens TEXT,
            user_id INTEGER 
        )
        ''']
    def __init__(self, directory='../var', isolation_level='DEFERRED'):
        super(DProfiles,self).__init__(directory+'/'+self.DB_FILENAME, self.INIT_QUERIES, isolation_level)

    def insert(self, profile_info):
        """inserts a row into the profiles table; profile_info = {'profile_alias':xx,'tokens':xx,'user_id':xx}"""
        assert(self._has_all_keys(profile_info,['profile_alias','tokens','user_id'],fail_if_extra=True))
        insert_tuple= self._make_insert_clause(profile_info)
        return self.q('INSERT OR REPLACE INTO `profiles` %s'% insert_tuple[0], insert_tuple[1], 'NUMBER_OF_ROWS_AFFECTED')

    def get_profile(self, profile_id):
        return self.q('SELECT * FROM profiles WHERE profile_id=:profile_id', {'profile_id': profile_id}, 'ONE_ROW')
    
    def get_profiles(self, authenticated_only=True):
        where_str= 'tokens IS NOT NULL' if authenticated_only else ''
        return self.q('SELECT * FROM profiles WHERE %s' % where_str, {}, 'ALL_ROWS')

    def delete(self, profile_alias):
        return self.q('DELETE FROM profiles WHERE profile_id=:profile_id', {'profile_id': profile_id}, 'NUMBER_OF_ROWS_AFFECTED')

    def update(self, profile_alias, profile_info):
        pass


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

    def __init__(self, directory='../var', isolation_level='DEFERRED'):
        super(DPeople,self).__init__(directory+'/'+self.DB_FILENAME, self.INIT_QUERIES, isolation_level)

    def insert(nick_name, user_id, user_name='', person_id=None):
        return self.q('''
            INSERT INTO `people`(`nick_name`,`user_name`,`user_id`)
            VALUES(:nick_name, :user_name, :user_id)
            ''',{'nick_name':nick_name, 'user_id':user_id, 'user_name':user_name},'LAST_ROWID')

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

    def update_user_names(s= [self.__init_queries]
        if not instanceof(usernames_dict,dict):
            raise TypeError('usernames_dict should be a dict')

        rows_updated= 0
        for user_id in usernames_dict.keys():
            user_name= usernames_dict[user_id]
            rows_updated += self.q('''
                UPDATE OR IGNORE people
                SET user_name=:user_name WHERE user_id=:user_id
                ''',{'user_name':user_name, 'user_id':user_id},'NUMBER_OF_ROWS_AFFECTED')

        return rows_updated


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

    def __init__(self, directory='../var', isolation_level='DEFERRED'):
        init_queries= self.INIT_QUERIES +DPeople.INIT_QUERIES +DProfiles.INIT_QUERIES
        super(DTweets,self).__init__(directory+'/'+ self.DB_FILENAME, init_queries, isolation_level)
    
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
            throw Exception('either tweet_id, user_id, person_id should be not None')

    def get_tweet(self, tweet_id)
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
        if instanceof(profile,dict) and 'profile_id' in dict:
            data_dict['profile_id']= profile['profile_id']
        else
            data_dict['profile_id']= profile
        
        # prepare tweet data
        if not isinstance(tweet,dict):
            throw TypeError('expecting tweet to be a dict')
            
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
        else
            throw TypeError('expecting a dict or list')


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

    def __init__(self, directory='../var', isolation_level='DEFERRED'):
        super(DPeople,self).__init__(directory+'/'+ self.DB_FILENAME, self.INIT_QUERIES+DTweets.INIT_QUERIES, isolation_level)

    def get_thread_tree(self, tweet_id, get_content_bool=False):
        pass

    def get_child_count(self, tweet_id):
        pass
