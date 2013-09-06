'''
Created on 2013-8-29

@author: sdsong
'''
import hashlib

def md5(data):
    md5 = hashlib.md5()
    md5.update(data)
    return md5.hexdigest()