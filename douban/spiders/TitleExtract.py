#!/usr/bin/env python
# coding=utf-8
import time

import psycopg2
import requests
from scrapy import Selector

from Entity import url_list
from douban.settings import DATABASE


class TaskParser(object):
    """docstring for ClassName"""
    def __init__(self):
        self.db = psycopg2.connect(database=DATABASE['database'], user=DATABASE['user'], password=DATABASE['password'], host=DATABASE['ip'], port=DATABASE['port'])
        self.cur = self.db.cursor()
        #从url_list中获得URL去辨别是哪个网站然后拿到影片title和导演，演员
    def _get_movie_name(self):    
        for url in url_list:
            times = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            MovieList = []
            uid = ''
            type1 = ''
            MovieTitle = []
            MovieDirector = []
            MovieActor = []
            premiere = []
            #爱奇艺影片详情URL方案
            if url.startswith('http://www.iqiyi.com'):
                print u'iqiyi url：{}'.format(url)
                page_html = requests.get(url).content
                uid = ''
                premiere = ''
                response = Selector(text=page_html)
                type1 = response.xpath('//div[@class="topDot"]/a/h2/text()').extract()[0]
                #影片Title
                MovieTitle = response.xpath('//a[@id="widget-videotitle"]/text()').extract() \
                or response.xpath('//span[@id="widget-videotitle"]/text()').extract()
                MovieList.extend(MovieTitle)
                #影片导演
                MovieDirector = response.xpath('//a[@itemprop="director"]/text()').extract()
                MovieList.extend(MovieDirector)
                #影片主演
                MovieActor = response.xpath('//p[@class="progInfo_rtp"]/span/a[@itemprop="actor"]/text()').extract()
                MovieList.extend(MovieActor)
            #腾讯影片详情URL方案
            elif url.startswith('https://v.qq.com'):
                print u'腾讯URL：{}'.format(url)
                page_html = requests.get(url,verify=False).content
                firstresponse = Selector(text=page_html)
                titleurl = firstresponse.xpath('//h2[@class="player_title"]/a/@href').extract()
                if titleurl:
                    fullurl = 'https://v.qq.com'+titleurl[0]
                    print fullurl
                    detail_page = requests.get(fullurl,verify=False).content
                    #print detail_page
                    secondresponse = Selector(text=detail_page)
                    #影片类型
                    vediotypelist = secondresponse.xpath('//h1[@class="video_title_cn"]/span[@class="type"]/text()').extract()
                    if vediotypelist:
                        type1 = vediotypelist[0]
                        print type1
                        if type1 == u'电视剧' or type1 == u'电影':
                            #影片名称
                            MovieTitle = secondresponse.xpath('//h1[@class="video_title_cn"]/a/text()').extract()
                            MovieList.extend(MovieTitle)
                            #print Movietitle[0]
                            #上映时间 注意区分电影和电视
                            if type1 == u'电视剧':
                                t = u"出品时间:"
                                premiere = secondresponse.xpath('//span[text()="%s"]/following-sibling::span/text()'%(t)).extract()
                            else:#电影
                                t = u"上映时间:"
                                premiere = secondresponse.xpath('//span[text()="%s"]/following-sibling::span/text()'%(t)).extract()
                            #判断是否存在上映时间或者出品时间
                            if not premiere:
                                premiere = []
                            #导演和演员列表
                            DirectorActor = firstresponse.xpath('//div[@class="director"]/a/text()').extract()
                            #直接将导演和演员的列表去重后加入
                            MovieList.extend(list(set(DirectorActor)))
                            #判断演员和导演
                            lag = firstresponse.xpath('//div[@class="director"]/text()').extract()
                            count = 0
                            for i in lag:
                                if not i.endswith(u'演员: ') :
                                    if i == u'/':
                                        count =+1
                                else:
                                    break
                            #导演
                            MovieDirector = DirectorActor[:count+1]
                            #演员
                            MovieActor = DirectorActor[count+1:]
            print MovieTitle[0]
            #插入一整条的信息，这里没有去重。
            self.url1_task_insert(uid,url,type1,'/'.join(MovieTitle),'/'.join(MovieDirector),'/'.join(MovieActor),'/'.join(premiere),times) #将uid，URL1，type1,Title,Director,premiere，times插入库中
            # #土豆影片URL方案
            # elif url.startswith('http://video.tudou.com'):
            #     print u'土豆影片详情URL：{}'.format(url)
            #     page_html = requests.get(url).text
            #     response = Selector(text=page_html)
            #     MovieTitle = response.xpath("//div[@class='td-listbox__title']/text()").extract()[0]
            #     MovieList.append(MovieTitle)
            # #优酷影片URL方案
            # elif url.startswith('http://v.youku.com'):
            #     print u'优酷影片详情URL：{}'.format(url)
            #     page_html = requests.get(url).text
            #     response = Selector(text=page_html)
            #     MovieTitle = response.xpath('//h1[@class="title"]/span/text()').extract()[0]
            #     MovieList.append(MovieTitle)
            #elif len(url) < 30:
                #print 'This is Movie Title：{}'.format(url)
                #MovieList.append(url.decode('utf-8'))
            # else:
            #     return "*"*100
        #URL1产生关键字单条分开入库，提供豆瓣采集搜索关键字
            for key in list(set(MovieList)):
                check_sql = "select keyword from public.movie_keyword_task where keyword ='%s'"%key
                self.cur.execute(check_sql)
                check_data = self.cur.fetchall()
                if check_data:  # 关键字存在的话就不插入
                    print 'keyword break......' 
                else:
                    self.cur.execute(u"insert into public.movie_keyword_task(keyword, flag, insert_times) values('{}', {}, '{}')".format(key,0,times))
                self.db.commit()
                print 'keyword insert ok!!!'

    def url1_task_insert(self,uid,url,type1,title,director, actor, premiere,create_time):
        print 'url1_task_insert'
        self.cur.execute(u"insert into public.movie_url1_task(uid, url, type1, title, director, actor, premiere, create_time) \
        values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(uid,url,type1,title,director,actor,premiere,create_time))
        print 'cur.execute'
        self.db.commit()

    def __del__(self):
        self.db.close()

#if __name__ == '__main__':
    #s = TaskParser()
    #s._get_movie_name()