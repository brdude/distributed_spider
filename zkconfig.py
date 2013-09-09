'''
Created on 2013-8-29

@author: sdsong
'''
import logging

from kazoo.client import KazooClient

logging.basicConfig(level=logging.WARN) 
#from kazoo.protocol.states import KazooState

def zookeeper():
    return  KazooClient(hosts='10.232.3.164:2181')
