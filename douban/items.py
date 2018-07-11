# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DoubanItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()          #影片标题
    directors = scrapy.Field()      #导演
    scriptwriters = scrapy.Field()  #编剧
    actors = scrapy.Field()         #演员
    types = scrapy.Field()          #剧情
    release_region = scrapy.Field() #制片地区
    release_date = scrapy.Field()   #上映日期／首播
    alias = scrapy.Field()          #影片别名
    languages = scrapy.Field()      #语言
    duration = scrapy.Field()       #影片时长／电视剧单集时长
    score = scrapy.Field()          #评分
    description = scrapy.Field()    #剧情简介
    media_episoders = scrapy.Field()    #集数（电视剧）电影等为1
    other_link = scrapy.Field()     #其它站点URL
    movie_type = scrapy.Field()     #影片类型
    youku_url = scrapy.Field()
    tencent_url = scrapy.Field()
    iqiyi_url = scrapy.Field()
    letv_url = scrapy.Field()
    huashu_url = scrapy.Field()
    souhu_url = scrapy.Field()
    cntv_url = scrapy.Field()
    pptv_url = scrapy.Field()
    mgtv_url = scrapy.Field()