import sqlite3
import string
import random

class DObject(object):
    @property db
    def __get_db(self):
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
        if ! isinstance(self.__db, sqlite3.Connection):
            raise Exception('database connection not established')

        query_list= []
        if not self.__init_queries is None:
            if isinstance(self.__init_queries, str) || isinstace(self.__init_queries,tuple):
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
            if isinstance(q_list_item,tuple) || isinstance(q_list_item,list):
                result_list.append(self.q(q_list_item[0], q_list_item[1], result_type, auto_commit=False)
            elif isinstance(q_list_item,str):
                result_list.append(self.q(q_list_item, None, result_type, auto_commit=False)
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
        if !params is None:
            if !isinstance(params,dict) && !isinstance(params,tuple):
                raise TypeError('params should be a dict or tuple type')
        if !isinstance(query,str):
                raise TypeError('query should be a str')

        result= False
        cursor= self.db.cursor()
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


class DCredentails(DObject):
    DB_FILENAME= 'tt_setup.db'
    INIT_QUERIES= ['''
        CREATE TABLE IF NOT EXISTS `credentials`(
            alias TEXT PRIMARY KEY,
            state INTEGER,
            session_data TEXT,
            user_info TEXT
        )
        ''']
    def __init__(self, directory='../var', isolation_level='DEFERRED'):
        super(DCredentials,self).__init__(directory+'/'+self.DB_FILENAME, self.INIT_QUERIES, isolation_level)
    def insert(self, alias, state, session_data, user_info):
        pass
    def delete(self, alias):
        pass
    def update_user_info(self, alias, user_info):
        pass


class DPeople(DObject):
    DB_FILENAME= 'tt_main.db'
    INIT_QUERIES= ['''
        CREATE TABLE IF NOT EXISTS `people`(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER,
            nick_name TEXT,
            user_name TEXT,
            user_id INTEGER
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

    def __init__(self, directory='../var', isolation_level='DEFERRED'):
        super(DPeople,self).__init__(directory+'/'+self.DB_FILENAME, self.INIT_QUERIES, isolation_level)

    def insert(nick_name, user_id, user_name='', person_id=None):
        return self.q('''
            INSERT INTO `people`(`nick_name`,`user_name`,`user_id`)
            VALUES(:nick_name, :user_name, :user_id)
            ''',{'nick_name':nick_name, 'user_id':user_id, 'user_name':user_name},'LAST_ROWID')

    def get_users_of(self, person_id=None, nick_name=None):
        if !person_id is None:
            # search by person_id
            return self.q('''
                SELECT * FROM `people` p WHERE p.person_id=:person_id
                ''',{'person_id':person_id},'ALL_ROWS')

        elif !nick_name is None:
            # search by nick name
            return self.q('''
                SELECT * FROM `people` p WHERE p.nick_name=:nick_name
                ''',{'nick_name':nick_name},'ALL_ROWS')

        else:
            raise Exception('must either provide person_id or nick_name')

    def get_people(self):
        return self.q('SELECT DISTINCT person_id,nick_name FROM people','ALL_ROWS')

    def update_user_names(self, usernames_dict):
        """
            updates user_name in the people table
            usernames_dict: dict
                should be {'user_id':'new_user_name', ...}
        """
        if !instanceof(usernames_dict,dict):
            raise TypeError('usernames_dict should be a dict')

        rows_updated= 0
        for user_id IN usernames_dict.keys():
            user_name= usernames_dict[user_id]
            rows_updated += self.q('''
                UPDATE OR IGNORE people
                SET user_name=:user_name WHERE user_id=:user_id
                ''',{'user_name':user_name, 'user_id':user_id},'NUMBER_OF_ROWS_AFFECTED')

        return rows_updated

