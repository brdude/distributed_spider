'''
Created on 2013-8-22

@author: sdsong
'''

import urllib2
from abstract_spider import AbstractSpider
class TestSpider(AbstractSpider):
    def __init__(self, name='default_name', 
                 store=None,
                 workdir=None, 
                 url_generators=None):
        super(TestSpider, self).__init__(
            name=name, store=store,workdir=workdir)
        self.urls = lambda taskno:['http://www.163.com','http://love.163.com']
    def visit(self, url):
        return  urllib2.urlopen(url).read()
    
    def website(self):
        return 'huatian'
        
spider = TestSpider(name='testspider',workdir='./')
spider.start()