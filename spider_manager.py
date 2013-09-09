# -*- coding: utf-8 -*-

'''
Created on 2013-8-20

@author: sdsong
'''
from zkconfig import zookeeper
from kazoo.protocol.states import  EventType
import logging
from bitarray import bitarray
import pickle
import time
from spider_state import SpiderDataState, SpiderNodeState
logging.basicConfig(level=logging.warn) 
def info(method, msg):
    
    logging.warn(str(time.time())+(' in %s | ' % method) + str(msg))
    
class SpiderManager:
    
    def __init__(self):
        self.ip = None
        self.leader = None
        self.node = None
        self.is_leader = lambda :(self.leader == self.node)
        
        self.zk = zookeeper()

        self.zk.start()

        self.ensure_path()
        self.set_watch()
    def ensure_path(self):
        self.zk.ensure_path('/spider/spiders')
        self.zk.ensure_path('/spider/managers')
        
        self.zk.ensure_path('/spider/data/')
        self.zk.ensure_path('/spider/data/running')
        self.zk.ensure_path('/spider/data/completed')
        self.zk.ensure_path('/spider/data/error')
        
    def set_watch(self):
        # 监控 如/spider/spiders/huatian/
        # 当有新节点加入时：处理数据分配
        # 当有节点断开时：处理数据错误节点的产生
        def spiders_website_watch(event):
            info('spiders_website_watch', 'spiders_website_watch was triggered:' + str(event))
            
            if event.type == EventType.DELETED:
                info('spiders_website_watch', 'spider website was deleted')
                return
            elif event.type == EventType.CHILD:
                childs = self.zk.get_children(event.path, watch=spiders_website_watch)
                for child in childs:
                    child_path = event.path + '/' + child
                    state = self.zk.get(child_path)
                    node_state = SpiderNodeState.loads(state[0])
                    if node_state.is_new_node():
                        info('spiders_website_watch', 'new spider joined:' + child_path)
                        # alloc data node for the new spider
                        self.alloc_task_no(child_path)
                
                info('spiders_website_watch', 'check if some spider exit')
                website = event.path.split('/')[-1]
                childs = self.zk.get_children('/spider/data/running/%s' % website)
                
                for child in childs:
                    data_node_state = pickle.loads(self.zk.get('/spider/data/running/%s/%s' % (website, child))[0])
                    if (not self.zk.exists(data_node_state.spider_node_path)) \
                            and (data_node_state.state != SpiderDataState.DONE):
                        info('spiders_website_watch', 'spider node %s is dead' % (data_node_state.spider_node_path))
                        
                        self.zk.create('/spider/data/error/%s/en_'%website,
                                       sequence=True,
                                       value = data_node_state.dumps())
                        self.zk.delete('/spider/data/running/%s/%s' % (website, child))

        # 监控 如/spider/data/running/huatian    
        # 监控children数目变化
        def data_running_website_watch(event):
            info('data_running_website_watch', 'data_running_website_watch was triggered:' + str(event))

            if event.type == EventType.DELETED:
                info('data_running_website_watch', 'data running website was deleted')
                return
            elif event.type == EventType.CHILD:
                info('data_running_website_watch', 'data_running_website_watch child num change')
                self.zk.get_children(event.path, watch=data_running_website_watch)
              
        # 监控 如/spider/data/completed/huatian  
        # 监控children数目变化      
        def data_completed_website_watch(event):
            info('data_completed_website_watch', 'data_completed_website_watch was triggered:' + str(event))

            if event.type == EventType.DELETED:
                info('data_completed_website_watch', 'data completed website was deleted')
                return
            elif event.type == EventType.CHILD:
                self.zk.get_children(event.path, watch=data_completed_website_watch)
                info('data_completed_website_watch', 'data_completed_website_watch child num change')         
                 
        # 监控 如/spider/data/error/huatian   
        # 监控children数目变化     
        def data_error_website_watch(event):
            info('data_error_website_watch', 'data_error_website_watch was triggered:' + str(event))

            if event.type == EventType.DELETED:
                info('data_error_website_watch', 'data error website was deleted ')
            elif event.type == EventType.CHILD:
                self.zk.get_children(event.path, watch=data_error_website_watch)
                info('data_error_website_watch', 'data_error_website_watch child num change')             

        # 监控 /spider/spiders
        # 当子节点数目变化时，为每一个子节点重新设置watch
        def cws_spiders_watch(event):
            info('cws_spiders_watch', 'cws_spiders_watch triggered:' + str(event))
            for child in self.zk.get_children("/spider/spiders", watch=cws_spiders_watch):
                info('cws_spiders_watch','set childs watch for /spider/spiders/%s' %child)
                self.zk.get_children('/spider/spiders/%s' % child, watch=spiders_website_watch)
                self.zk.set('/spider/spiders/%s' % child, 'ready')
                
                self.zk.ensure_path('/spider/data/error/%s/'%child)

        # 监控 /spider/data/running
        # 当子节点数目变化时，为每一个子节点重新设置watch
        def cws_datarunning_watch(event):
            info('cws_datarunning_watch', 'cws_datarunning_watch triggered:' + str(event))

            for child in self.zk.get_children("/spider/data/running", watch=cws_datarunning_watch):
                if self.zk.get('/spider/data/running/%s' % child)[0] == '':
                    info('cws_datarunning_watch', 'init bitmap for ' + event.path)
                    bitmap = bitarray('0' * 10240)
                    self.zk.set('/spider/data/running/%s' % child, pickle.dumps(bitmap))  
                self.zk.get_children('/spider/data/running/%s' % child, watch=data_running_website_watch)

        # 监控 /spider/data/completed
        # 当子节点数目变化时，为每一个子节点重新设置watch
        def cws_datacompleted_watch(event):
            info('cws_datacompleted_watch', 'cws_datacompleted_watch triggered:' + str(event))

            for child in self.zk.get_children("/spider/data/completed", watch=cws_datacompleted_watch):
                self.zk.get_children('/spider/data/completed/%s' % child, watch=data_completed_website_watch)
        # 监控 /spider/data/completed
        # 当子节点数目变化时，为每一个子节点重新设置watch
        def cws_dataerror_watch(event):
            info('cws_dataerror_watch', 'cws_dataerror_watch triggered:' + str(event))

            for child in self.zk.get_children("/spider/data/error", watch=cws_dataerror_watch):
                self.zk.get_children('/spider/data/error/%s' % child, watch=data_error_website_watch)
        # 监控 /spider/managers/
        # 执行managers中的leader选举
        # 并为新leader设置leader watch
        def select_leader_managers_watch(event):
            info('select_leader_managers_watch', 'select_leader_managers_watch triggered:' + str(event))

            childs = self.zk.get_children('/spider/managers', watch=select_leader_managers_watch)
            childs.sort()
            self.leader = '/spider/managers/%s' % childs[0]
            
            if self.is_leader():
                info('select_leader_managers_watch', 'i am the leader now')
                
                info('select_leader_managers_watch', 'set watch for /spider/spiders')
                
                for spider_category in self.zk.get_children('/spider/spiders', watch=cws_spiders_watch):
                    info('select_leader_managers_watch', 'set watch for ' + '/spider/spiders/%s' % spider_category)

                    self.zk.get_children('/spider/spiders/%s' % spider_category, watch=spiders_website_watch)

                for rdata in self.zk.get_children('/spider/data/running', watch=cws_datarunning_watch):
                    info('select_leader_managers_watch', 'set watch for ' + '/spider/data/running/%s' % rdata)

                    self.zk.get_children('/spider/data/running/%s' % rdata, watch=data_running_website_watch)
                    
                for cdata in self.zk.get_children('/spider/data/completed', watch=cws_datacompleted_watch):
                    info('select_leader_managers_watch', 'set watch for ' + '/spider/data/completed/%s' % cdata)
                    self.zk.get_children('/spider/data/completed/%s' % cdata, watch=data_completed_website_watch)
                     
                for edata in self.zk.get_children('/spider/data/error', watch=cws_dataerror_watch):
                    info('select_leader_managers_watch', 'set watch for ' + '/spider/data/error/%s' % edata)
                    self.zk.get_children('/spider/data/error/%s' % edata, watch=data_error_website_watch)
                     

        # self.zk.get_children("/spider/spiders", watch=cws_spiders_watch)
        self.zk.get_children("/spider/managers", watch=select_leader_managers_watch)
        
        self.node = self.zk.create('/spider/managers/node_', ephemeral=True, sequence=True)      
    #为website申请任务节点
    def alloc_task_no(self, child_path):
        info('alloc_task_no', 'try to alloc task no for %s'%child_path)
        
        tx = self.zk.transaction()
        node_version =  self.zk.get(child_path)[1].version
        
        node_state = SpiderNodeState.loads(self.zk.get(child_path)[0])
        node_state.state = SpiderNodeState.READY
        node_state.task_no = -1
        
        website = child_path.split('/')[-2]
        
        #首先检查该网站下是否有错误节点
        childs = self.zk.get_children("/spider/data/error/%s" % website)
        if len(childs)>0:
            info('alloc_task_no','alloc error data node')
            error_node_state = SpiderDataState.loads(self.zk.get("/spider/data/error/%s/%s"%(website,childs[0]))[0])
            tx.delete("/spider/data/error/%s/%s"%(website,childs[0]))
            
            node_state.task_no =  error_node_state.task_no
        else:
            path = "/spider/data/running/%s" % website
            data = self.zk.get(path)
            bitmap = pickle.loads(data[0])
            
            for i in xrange(len(bitmap)):

                if not bitmap[i]:
                    bitmap[i] = 1
                    self.zk.set(path, pickle.dumps(bitmap))
                    node_state.task_no =  i;
                    break
        info('alloc_task_no', 'alloc task no %s for %s'%(node_state.task_no,child_path))
        
        tx.set_data(child_path, value=node_state.dumps())
        tx.commit()
    def success_task(self, task_no):
        return True
    
    def error_task(self, task_no):
        return True
    
    def schedule(self):
        while True:
            time.sleep(1000)        

SpiderManager().schedule()



