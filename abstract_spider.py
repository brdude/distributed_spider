# -*- coding: utf-8 -*-
'''
Created on 2013-8-1

@author: sdsong
'''
from threading import Thread
import time
import os
import logging

import urllib2
from zkconfig import zookeeper

from store.file_store import FileStore
from store.misc import md5
from spider_state import SpiderDataState
from spider_state import SpiderNodeState

class AbstractSpider(Thread):

    def __init__(self,
                 name='spider',
                 workdir=None,
                 store=None):
        Thread.__init__(self)
        
        if store == None and workdir == None:
            raise Exception('set store value or workdir to create spider')
        self.store = store if store != None else  FileStore(workdir, name)
        
        self.name = name
        self.start_time = time.time()
        self.pause_seconds = 1
        self.urls = []
        self.zk = zookeeper()
        self.zk.start()
        self.zk.ensure_path("/spider/data/running/%s" % self.website())
        self.zk.ensure_path("/spider/spiders/%s" % self.website())
        #self.register()
        
    def register(self):
        # time.sleep(5)
        # 等待manager设置好watch
        while self.zk.get("/spider/spiders/%s" % self.website())[0] != 'ready' and self.zk.get("/spider/data/running/%s" % self.website())[0] == '':
            time.sleep(0.1)
            
        self.node_state = SpiderNodeState(SpiderNodeState.INIT, self.website())
        self.data_state = SpiderDataState(SpiderDataState.INIT, self.website())
        
        self.zk_spider_node = self.zk.create('/spider/spiders/%s/dn_' % self.website(),
                                             ephemeral=True,
                                             sequence=True,
                                             value=self.node_state.dumps())
        self.data_state.spider_node_path = self.zk_spider_node
        
        self.zk_data_node = self.zk.create('/spider/data/running/%s/dn_' % self.website(), 
                                           sequence=True,
                                           value=self.data_state.dumps())  
        
        
              
        self.ready = False
        def data_alloc_ok(event):
            print event
            self.node_state =  SpiderNodeState.loads(self.zk.get(self.zk_spider_node)[0])
            self.node_state.data_node_path = self.zk_data_node
            self.node_state.state = SpiderNodeState.WORKING
            self.zk.set(self.zk_spider_node, value = self.node_state.dumps())
            
            self.data_state.state = SpiderNodeState.WORKING
            self.data_state.task_no = self.node_state.task_no
            print self.data_state.task_no
            self.zk.set(self.zk_data_node,value = self.data_state.dumps())
            self.ready = True
                        # now can start the spider
            # start()
        state = SpiderNodeState.loads( self.zk.get(self.zk_spider_node, watch=data_alloc_ok)[0])
        if state.state == SpiderNodeState.READY:
            self.ready = True
        
        #print 'data' + str(self.zk.get('/spider/spiders/%s' % self.website()))

        # logging config
        logging.basicConfig(
            filename=os.path.join('./', '%s.log' % (self.name)), level=logging.INFO, filemode='w',
            format='%(asctime)s - %(levelname)s: %(message)s')
        while not self.ready:
            time.sleep(0.1)
            
    def unregister(self):
        self.data_state.state = SpiderDataState.DONE

        self.zk.ensure_path('/spider/data/completed/%s/'%self.website())
        self.zk.create('/spider/data/completed/%s/%s'%(self.website(),self.zk_data_node.split('/')[-1]),value = self.data_state.dumps())
        
        self.zk.delete(self.zk_data_node)
        self.zk.delete(self.zk_spider_node)
    def update_state(self):
        self.zk.set(self.zk_data_node, self.state.dumps())
    def info(self, msg, br=True):
        logging.info(msg)

    def pause(self):
        self.info('pause %s seconds' % self.pause_seconds, br=False)
        map(lambda i: (self.info('.', br=False) and time.sleep(1)),
            range(0, self.pause_seconds))
        self.info('resume')



    def save(self, url, data):
        self.store.save_data(url, data)

    def visit(self, url):
        return  urllib2.urlopen(url).read()
    
    def run(self):
        while True:
            self.register()
            for url in self.urls(self.data_state.task_no):
                try:
                    print url
                    data = self.visit(url)
                    print data
                    new_md5 = md5(data)
                    data_in_db = self.store.has(url)
    
                    if data_in_db:
                        self.info('has fetched before:%s,checking md5...' % url)
                        if new_md5 == data_in_db[0]:
                            self.info('md5 is the same，so skip.')
                        else:
                            self.info('md5 is change, so fetch it again:')
                            self.store.save_data(url, data)
                            self.store.success(url, new_md5, self.website())
                    else:
                        self.save(url, data)
                        self.store.success(url, md5(data), self.website())
                        
                    self.pause()
                except Exception as e:
                    self.info(str(e))
                    try:
                        self.store.error(url)
                    except Exception as e:
                        self.info(str(e))
            self.unregister()

    def website(self):
        return "unknown"
    def stop(self):
        pass


