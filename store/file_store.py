'''
Created on 2013-8-29

@author: sdsong
'''
import os
import functools
import sqlite3
import time
from misc import md5
# store = HbaseStore()
class FileStore:

    def __init__(self, workdir='~/', store_name='spider'):

        self.workdir = workdir
        self.datafile = workdir + '%s.db' % store_name
        self.update = functools.partial(lambda data_file, sql:
                                       (lambda conn, sql: conn.cursor().execute(sql) is not None and conn.commit())(sqlite3.connect(data_file), sql), self.datafile)

        self.select = functools.partial(
            lambda data_file, sql: sqlite3.connect(data_file).cursor().execute(sql).fetchall(), self.datafile)
        self.__createDbIfNotExists__()   
    
    # url pk text
    # content_type int
    # md5 text
    # website int
    # last_access_status text
    # last_access_time long
    def __createDbIfNotExists__(self):
        return os.path.exists(self.datafile) \
            or (self.update('''create table urls (url text primary key,content_type int ,
                md5 text,website text,last_access_status int,last_access_time long)''') is not None)
    def __today_as_string(self):
        return time.strftime('%Y-%m-%d', time.localtime(time.time()))

    def save_data(self, url, data):
        self.__write(data, self.__get_path(md5(str(url)), self.__today_as_string()))
        
    def success(self, url, md5_digest,website):
        if not self.has(url):
            self.update('''insert into urls(url,content_type,md5,
                        website,last_access_status,last_access_time) values("%s","%s","%s","%s",1,datetime())
                        ''' % (url, 1, md5_digest, 1))
        else:
 
            self.update('''update urls set md5="%s",last_access_status="%s",last_access_time=datetime()
                where url="%s"
                ''' % (md5_digest, 1, url))

    def error(self, url,website):
        if not self.has(url):
            self.update('''insert into urls(url,content_type,md5,website,last_access_status,last_access_time) 
                        values("%s","%s","%s","%s",0,datetime())
                        ''' % (url, 1, '', 1))
        else:
            self.update('''update urls set last_access_status="%s",last_access_time=datetime()
                where url="%s"
                ''' % (0, url))

    def has(self, url):
        return (lambda o:o[0] if len(o) > 0 else None)(self.select('select md5 from urls where url="%s"' % url))
    
    def last_task_id(self):
        return (lambda o :o[0] if len(o) > 0 else None) (self.select('''select * from urls 
                order by last_access_time desc limit  1'''))


    def last_error_task_id(self):
        return (lambda o :o[0] if len(o) > 0 else None) (self.select('''select * from urls where last_access_status = 0 
                order by last_access_time desc limit  1'''))

    def last_success_task_id(self):
        return (lambda o :o[0] if len(o) > 0 else None) (self.select('''select * from urls where last_access_status = 1 
                order by last_access_time desc limit  1'''))
        
    def __get_path(self, name, *dirs):
        path = self.workdir
        for pdir in dirs:
            path = path + str(pdir) + '/'
            if not os.path.exists(path):
                os.mkdir(path)

        return path + name

    def __write(self, obj, filename):
        file_object = open(filename, 'wb')
        file_object.write(obj)
        file_object.close()