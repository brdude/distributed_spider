'''
Created on 2013-8-29

@author: sdsong
'''

import happybase
import time


class HbaseStore:
    def __init__(self):
        self.__createDbIfNotExists__()
    
    def get_conn(self):
        conn = happybase.Connection(host='aliyun2', port=9090,
                                  timeout=None, autoconnect=True,
                                  table_prefix=None, table_prefix_separator='_',
                                  compat='0.92', transport='buffered') 
        return conn
    def close_conn(self,conn):
        conn.close()
        
    def __createDbIfNotExists__(self):
        conn = self.get_conn()
        tables = conn.tables()
        if not 'urls' in tables:
        
            families = {
                            'url_info':dict(max_versions=10),
                            'spider_info':dict(max_versions=10),
                            'content':dict(max_versions=1000)}
            conn.create_table('urls', families)
        self.close_conn(conn)
    def save_data(self, url, data):
        conn = self.get_conn()
        table = conn.table('urls')
        table.put(url,
                  {"content:data":data}
                  )
        
        self.close_conn(conn)
        
    
    def success(self, url, md5_digest,website):
        conn = self.get_conn()
        table = conn.table('urls')
        table.put(url,{"spider_info:md5":md5_digest,
                       "spider_info:last_access_time":str(time.time()),
                       "spider_info:last_access_status":'1',
                       'url_info:website':website
                       })
        
        self.close_conn(conn)
    def error(self, url,website):
        conn = self.get_conn()
        table = conn.table('urls')
        table.put(url,{
                       "spider_info:last_access_time":str(time.time()),
                       "spider_info:last_access_status":'1'

                       })
        
        self.close_conn(conn)
    def has(self, url):
        conn = self.get_conn()
        table = conn.table('urls')
        row = table.row(url)
        self.close_conn(conn)
        return None if len(row)==0 else [row['spider_info:md5']]
