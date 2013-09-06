'''
Created on 2013-8-29

@author: sdsong
'''
import logging

from kazoo.client import KazooClient

logging.basicConfig(level=logging.WARN) 
#from kazoo.protocol.states import KazooState

def zookeeper():
    return  KazooClient(hosts='zkzerver:2181')
