'''
Created on 2013-8-30

@author: sdsong
'''
import pickle

class SpiderNodeState:
    INIT='INIT'
    READY='READY'
    WORKING='WORKING'
    DONE = 'DONE'
    def __init__(self,state,website):
        self.state = state
        self.website = website
        self.data_node_path = ""
    
    def set_task_no(self,task_no):
        self.task_no =task_no
        
    def is_new_node(self):
        return self.state == SpiderNodeState.INIT
    def dumps(self):
        return pickle.dumps(self)
    @staticmethod
    def loads(data):
        return pickle.loads(data)       
        
class SpiderDataState:
    INIT='INIT'
    READY='READY'
    WORKING='WORKING'
    DONE = 'DONE'
    def __init__(self,state,website):
        self.state = state
        self.website = website
        
        self.task_no = 0
        self.spider_node_path = ''

        self.data = 'abc'*100
        
        self.start_time = 0
        self.last_active_time = 0
        
        self.success_nr =0
        self.error_nu = 0
    
    def dumps(self):
        return pickle.dumps(self)
    @staticmethod
    def loads(data):
        return pickle.loads(data)
    #SpiderState.loads = 
# state = SpiderState()
# 
# data =  state.dumps()
# 
# s = SpiderState.loads(data)
# print s.data