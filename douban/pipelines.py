# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import json

import pymongo
from douban.settings import mongo_host, mongo_port, mongo_db_name, mongo_db_collection

class DoubanPipeline(object):
    def __init__(self):
        host = mongo_host
        port = mongo_port
        dbname = mongo_db_name
        sheetname = mongo_db_collection
        client = pymongo.MongoClient(host=host, port=port)
        mydb = client[dbname]
        self.post = mydb[sheetname]
    def process_item(self, item, spider):
        data = dict(item)
        self.post.insert(data)
        return item

class JsonWithEncoding(object):
    '''
    自定义导出json文件
    '''
    def __init__(self):
        # 使用codecs模块的打开方式，可以指定编码打开，避免很多编码问题
        self.file = codecs.open('douban_movie.json', 'w', encoding='utf-8')

    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False)+'\n'
        self.file.write(lines)
        # 注意别忘返回Item给下一个管道
        return item

    def spider_closed(self, spider):
        self.file.close()