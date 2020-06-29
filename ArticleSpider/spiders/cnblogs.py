# -*- coding: utf-8 -*-
import re
from typing import Optional, Match
from  urllib import  parse
import scrapy
from scrapy import Request
from urllib import parse
import requests
import re
import json
from ArticleSpider.utils import common
from ArticleSpider.items import JobBoleArticleItem
from scrapy.loader import ItemLoader





class CnblogsSpider(scrapy.Spider):
    name = 'cnblogs'
    allowed_domains = ['news.cnblogs.com']
    start_urls = ['http://news.cnblogs.com/']

    def parse(self, response):
        post_nodes = response.css('#news_list .news_block')[:1]
        for post_node in post_nodes:
            image_url = post_node.css('.entry_summary a img::attr(src)').extract_first("")
            post_url = post_node.css('h2 a::attr(href)').extract_first("")
            yield Request(url=parse.urljoin(response.url,post_url),meta={"front_image_url":image_url},callback=self.parse_detail)
        # next_url = response.css("div.pager a:last-child::text").extract_first("")
        # next_url = response.xpath("//a[contains(text(),'Next >')]/@href").extract_first("")
        # yield Request(url=parse.urljoin(response.url,next_url),callback=self.parse)
        # if next_url == "Next >":
        #     next_url = response.css("div.pager a:last-child::attr(href)").extract_first("")
        #     yield Request(url=parse.urljoin(response.url,next_url),callback=self.parse)

    def parse_detail(self,response):
        match_re = re.match(".*?(\d+)",response.url)
        if match_re:
            post_id = match_re.group(1)
            article_item = JobBoleArticleItem()
            title = response.css("#news_title a::text").extract_first("")
            # title = response.xpath("//*[@id='new_title']//a/text()")
            create_date = response.css("#news_info .time::text").extract_first("")
            # create_date = response.xpath("//*[@id='new_info']//*[@class='time']/text()")
            match_re = re.match(".*?(\d+.*)", create_date)
            if match_re:
                create_date = match_re.group(1)
            content = response.css("#news_content").extract()[0]
            # content = response.xpath("//*[@id='new_content']").extract()[0]
            tag_list = response.css(".news_tags a::text").extract()
            # tag_list = response.xpath("//*[@id='new_tags']//a/text()").extract()[0]
            tags = ",".join(tag_list)

            # html = requests.get(parse.urljoin(response.url,"/NewsAjax/GetAjaxNewsInfo?contentId={}".format(post_id)))
            # j_data=json.loads(html.text)
            # post_id = match_re.group(1)
            article_item["title"] = title
            article_item["create_date"] = create_date
            article_item["content"] = content
            article_item["tags"] = tags
            article_item["url"] = response.url
            if response.meta.get("front_image_url", ""):
                a1 = response.meta.get("front_image_url", "")
                a1 = "https:"+a1
                article_item["front_image_url"] = [a1]
            else:
                article_item["front_image_url"] = []



            yield Request(url=parse.urljoin(response.url, "/NewsAjax/GetAjaxNewsInfo?contentId={}".format(post_id)),
                          meta={"article_item": article_item}, callback=self.parse_nums)
            # praise_nums=j_data['DiggCount']
            # fav_nums=j_data['TotalView']
            # comment_nums=j_data['CommentCount']


    def parse_nums(self,response):
        j_data=json.loads(response.text)
        article_item = response.meta.get("article_item", "")

        praise_nums = j_data["DiggCount"]
        fav_nums = j_data["TotalView"]
        comment_nums = j_data["CommentCount"]

        article_item["praise_nums"] = praise_nums
        article_item["fav_nums"] = fav_nums
        article_item["comment_nums"] = comment_nums
        article_item["url_object_id"] = common.get_md5(article_item["url"])


        yield article_item