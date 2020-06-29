# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.images import ImagesPipeline
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exporters import JsonItemExporter
import codecs
import json
from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors

class ArticlespiderPipeline(object):
    def process_item(self, item, spider):
        return item


class JsonExporterPipleline(object):
    #调用scrapy提供的json export导出json文件
    def __init__(self):
        self.file = open('articleexport.json', 'wb')
        self.exporter = JsonItemExporter(self.file, encoding="utf-8", ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class JsonWithEncodingPipline(object):
    def __init__(self):
        self.file = codecs.open("article.json", "a", encoding="utf-8")

    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False)+"\n"
        self.file.write(lines)
        return item

    def spider_closed(self,spider):
        self.file.close()

class MysqlPipeline(object):
    # def __init__(self):
    #     self.conn = MySQLdb.connect('127.0.0.1', 'root', 'Zhou950330', 'test', charset="utf8", use_unicode=True)
    #     self.cursor = self.conn.cursor()
    #
    # def process_item(self, item, spider):
    #     insert_sql = """
    #             insert into test(title, url, create_date, fav_nums)
    #             VALUES (%s, %s, %s, %s)
    #         """
    #     self.cursor.execute(insert_sql, (item["title"], item["url"], item["create_date"], item["fav_nums"]))
    #     self.cursor.execute(insert_sql, (item.get("title", ""), item["url"], item["create_date"], item["fav_nums"]))
    #     self.conn.commit()
    # 采用同步的机制写入mysql
    def __init__(self):
        self.conn = MySQLdb.connect('127.0.0.1', 'root', 'Zhou950330', 'jobbole_article', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
            insert into jobble_article(title,url,url_object_id,front_image_path,front_image_url,parise_nums,comment_nums,fav_nums,tags,content,create_date)
            VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s) ON DUPLICATE KEY UPDATE parise_nums = VALUES(parise_nums)
        """
        params=list()
        params.append(item.get("title", ""))
        params.append(item.get("url", ""))
        params.append(item.get("url_object_id", ""))
        front_image = ",".join(item.get("front_image_path", []))
        params.append(front_image)
        params.append(item.get("front_image_url", ""))
        params.append(item.get("parise_nums", 0))
        params.append(item.get("comment_nums", 0))
        params.append(item.get("fav_nums", 0))
        params.append(item.get("tags", ""))
        params.append(item.get("content", ""))
        params.append(item.get("create_date", "1970-07-01"))
        self.cursor.execute(insert_sql, tuple(params))
        self.conn.commit()

        return item

class MysqlTwistedPipeline(object):
    def __init__(self,dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls,settings):
        dbparms = dict(
        host = settings["MYSQL_HOST"],
        db = settings["MYSQL_DBNAME"],
        user = settings["MYSQL_USER"],
        passwd = settings["MYSQL_PASSWORD"],
        charset='utf8',
        cursorclass=MySQLdb.cursors.DictCursor,
        use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool("MySQLdb",**dbparms)
        return cls(dbpool)

    def process_item(self, item, spider):
        #使用twisted将mysql插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider) #处理异常
        return item

    def handle_error(self, failure, item, spider):
        print(failure)

    def do_insert(self, cursor, item):
        insert_sql = """
            insert into jobble_article(title,url,url_object_id,front_image_path,front_image_url,parise_nums,comment_nums,fav_nums,tags,content,create_date)
            VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s) ON DUPLICATE KEY UPDATE parise_nums = VALUES(parise_nums)
        """
        params=list()
        params.append(item.get("title", ""))
        params.append(item.get("url", ""))
        params.append(item.get("url_object_id", ""))
        front_image = ",".join(item.get("front_image_path", []))
        params.append(front_image)
        params.append(item.get("front_image_url", ""))
        params.append(item.get("parise_nums", 0))
        params.append(item.get("comment_nums", 0))
        params.append(item.get("fav_nums", 0))
        params.append(item.get("tags", ""))
        params.append(item.get("content", ""))
        params.append(item.get("create_date", "1970-07-01"))
        cursor.execute(insert_sql, tuple(params))


class ArticleImagePipeline(ImagesPipeline):
    def item_completed(self, results, item, info):
        if "front_image_url" in item:
            image_file_path = ""
            for ok, value in results:
                image_file_path = value["path"]
            item["front_image_path"] = image_file_path
        return item