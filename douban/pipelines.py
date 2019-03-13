# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import json
import pymongo
import pymysql.cursors

from scrapy.http import Request
from scrapy.pipelines.images import ImagesPipeline
from douban.settings import mongo_host, mongo_port, mongo_db_name, mongo_db_collection

# 保存到MongoDB
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


# 保存到MySQL
class MySQLPipeline(object):
    def __init__(self):
        info = dict(
            host='127.0.0.1',  # 数据库地址
            port=3306,  # 数据库端口
            db='douban',  # 数据库名
            user='root',  # 数据库用户名
            passwd='123456',  # 数据库密码
            charset='utf8',  # 编码方式
            use_unicode=True
        )
        self.conn = pymysql.Connect(**info)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        sql = """insert into douban_movie(serial_number, movie_name, movie_img, introduce, star, evaluate, describ)
        value (%s, %s, %s, %s, %s, %s, %s)"""
        self.cursor.execute(sql, (item.get('serial_number'), item.get('movie_name'), item.get('movie_img'), item.get('introduce'), item.get('star'), item.get('evaluate'), item.get('describe')))
        self.conn.commit()
        return item


# 生成json文件
class JsonWithEncoding(object):
    '''
    自定义导出json文件
    '''
    def __init__(self):
        # 使用codecs模块的打开方式，可以指定编码打开，避免很多编码问题
        self.file = codecs.open('douban_movie.json', 'w', encoding='utf-8')

    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False)+',\n'
        self.file.write(lines)
        # 注意别忘返回Item给下一个管道
        return item

    def spider_closed(self, spider):
        self.file.close()


# 下载图片
class ImagespiderPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        yield Request(item.get('movie_img', ''))