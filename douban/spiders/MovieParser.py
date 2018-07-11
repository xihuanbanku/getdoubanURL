#!/usr/bin/env python
# coding=utf-8

import re
import urllib2
from collections import OrderedDict

from bs4 import BeautifulSoup


class MovieParser(object):

    def __init__(self,Htmlpage):
        self.lagelse = [u"真人秀",u"纪录片",u"脱口秀",u"音乐",u"歌舞",u"运动",u"短片"]
        self.soup = BeautifulSoup(Htmlpage, 'html.parser')
        self.domain_rules = {
            'v.qq.com':'tencent_url',
            'iqiyi.com':'iqiyi_url',
            'youku.com':'youku_url',
            'le.com':'letv_url',
            'mgtv.com':'mgtv_url',
            'pptv.com':'pptv_url',
            'souhu.com':'souhu_url',
            'huashu.com':'huashu_url',
            'cntv.com':'cntv_url',
        }
    def get_attr_by_func(self):
        pass

    @property    
    def is_404_page(self):
        if self.soup.find(u'页面不存在') != -1:
            return True
        else:
            return False
    #电影名称
    @property
    def title(self):
        title_info = self.soup.find('span', {'property': 'v:itemreviewed'})
        return title_info.get_text() if title_info else ''

    #导演
    @property
    def directors(self):
        directors_ele = self.soup.find_all('a', {'rel': 'v:directedBy'})
        if len(directors_ele)>1:
            directors_list = [ele.get_text() for ele in directors_ele]
            return '/'.join(directors_list)
        else:
            Directors = self.soup.find('a', {'rel': 'v:directedBy'})
            return Directors.get_text() if Directors else ''

    #编剧
    @property
    def scriptwriters(self):
        scriptwriters_info = self.soup.find_all('span', {'class': 'attrs'})
        return scriptwriters_info[1].get_text().replace(' / ', '/') if scriptwriters_info else ''

    #演员
    @property
    def actors(self):
        actors_list = self.soup.find_all('a', {'rel': 'v:starring'})
        if len(actors_list)>1:
            actors_list = [ele.get_text() for ele in actors_list]
            return '/'.join(actors_list)
        else:
            return actors_list.get_text()

    #影片类型
    @property
    def types(self):
        movie_type = self.soup.find_all('span', {'property': 'v:genre'})
        if len(movie_type) >1:
            movie_type_list = [ele.get_text() for ele in movie_type]
            return '/'.join(movie_type_list)
        else:
            movie_type = self.soup.find('span', {'property': 'v:genre'})
            return movie_type.get_text() if movie_type else ''

    #上映日期
    @property
    def release_date(self):
        release_date = self.soup.find_all('span', {'property': 'v:initialReleaseDate'})
        if len(release_date) >1:
            release_date_list = [ele.get_text() for ele in release_date]
            return '/'.join(release_date_list)
        else:
            release_date = self.soup.find('span', {'property': 'v:initialReleaseDate'})
            return release_date.get_text() if release_date else ''

    #电影别名
    @property
    def alias(self):
        other_info = self.soup.find('div', id='info')
        info = other_info.contents
        for i in range(0, len(info)):
            if len(str(info[i].encode('utf-8'))) < 10:
                continue
            if str(info[i].encode('utf-8')).find('又名') != -1:
                return info[i + 1].replace(' / ', '/').strip()
        return ''

    #影片时长与电视剧集数
    #修改为电影片长或者电视剧单集时常
    @property
    def duration(self):
        duration = self.soup.find('span', {'property': 'v:runtime'})#是电影定位电影片长
        if duration:
            return duration.get_text()
        
        else:#是电视剧定位单集时长
            other_info = self.soup.find('div', id='info')
            info = other_info.contents
            for i in range(0, len(info)):
                if len(str(info[i].encode('utf-8'))) < 10:
                    continue
                if str(info[i].encode('utf-8')).find('单集片长') != -1:
                    return info[i + 1].replace(' / ', '/').strip()
            return '' 

    @property#判断是电视剧 获取电视剧集数,电影等资源没有默认为1
    def media_episoders(self):
        other_info = self.soup.find('div', id='info')
        info = other_info.contents
        for i in range(0, len(info)):
            if str(info[i].encode('utf-8')).find('集数') != -1:
                return info[i + 1].replace(' / ', '/').strip()
        return ''#没有集数标签就返回空

    #剧情简介
    @property
    def description(self):
        description_info = self.soup.find('span', {'property': 'v:summary'})
        if description_info:
            description = re.sub(' |\n|\t','',description_info.get_text())
            return description.strip()
        else:
            return ''

    #语言
    @property
    def languages(self):
        other_info = self.soup.find('div', id='info')
        info = other_info.contents
        for i in range(0, len(info)):
            if len(str(info[i].encode('utf-8'))) < 10:
                continue
            if str(info[i].encode('utf-8')).find('语言') != -1:
                return info[i+ 1].replace(' / ', '/').strip()
        return ''

    @property
    def release_region(self):
        other_info = self.soup.find('div', id='info')
        info = other_info.contents
        for i in range(0, len(info)):
            if len(str(info[i].encode('utf-8'))) < 10:
                continue
            if str(info[i].encode('utf-8')).find('制片国家') != -1:
                return info[i + 1].replace(' / ', '/').strip()
        return ''

    #每集影片时长（限电视剧）
    @property
    def single_time(self):
        other_info = self.soup.find('div', id='info')
        info = other_info.contents
        for i in range(0, len(info)):
            if len(str(info[i].encode('utf-8'))) < 10:
                continue
            if str(info[i].encode('utf-8')).find('单集片长') != -1:
                return info[i + 1].replace(' / ', '/').strip()
        return ''

    @property
    def score(self):
        duration = self.soup.find('strong', {'property': 'v:average'})
        return duration.get_text() if duration else ''

    #电视剧推荐站点URL
    @property
    def teleplay_link(self):
        pass

    #movie teplay_link link parser
    def get_source_link(self, movie_item):
        r = re.compile('url=(.*)&subtype')
        teleplay_pa = re.compile("https://www.douban.com/link2/\?url=(.*);subtype=")
        #teleplay_pa = re.compile("http://www.douban.com/(.*)")
        movie_source_urls = OrderedDict()#定义一个有序字典用来盛放此资源对应的各个源播放网站的连接
        #判断视频标签是否在特定标签内
        isfilm = self.iselsejudge(movie_item["types"])
        if movie_item['media_episoders'] == ''and not isfilm: #判断电影的条件，集数标签是空，且遍历media_type2的类型，关键字不含有特定字段的
            print u'电影'
            movie_item['movie_type'] = 1
            movie_item['media_episoders'] = '1' #如果是电影，重置为1
            movie_urls = [link.attrs["href"] for link in self.soup.find_all('a',{"class":"playBtn"})]#拿到了豆瓣中各个网站播放此资源的URL
            for key,value in self.domain_rules.items():
                if movie_urls != []:
                    for link in movie_urls:
                        if not movie_source_urls.get(value):#如果是空
                            movie_source_urls[value] = [urllib2.unquote(r.findall(link)[0])] if key in link else []
                else:
                     movie_source_urls[value] = []
        elif movie_item['media_episoders'] != ''and not isfilm:#判断是电视剧，集数标签返回不是‘’，且判断不是其他类型标签
            print u"电视剧"
            movie_item['movie_type'] = 2 # 电视剧
            movie_urls = teleplay_pa.findall(self.soup.prettify())
            print movie_urls
            for key,value in self.domain_rules.items():
                if movie_urls!=[]:
                    for link in movie_urls:
                        if not movie_source_urls.get(value):
                            movie_source_urls[value] = [urllib2.unquote(link) for link in movie_urls if key in link]
                else:
                    movie_source_urls[value] = []
        else:
             movie_item['movie_type'] = 99
        #print movie_source_urls
        movie_item.update(movie_source_urls)#把该资源对应各个网站，例如，搜狐，优酷的播放地址更新到 movie_item
        #print movie_source_urls
        #print '@@@@@@@@@@@'
        return movie_item

    def iselsejudge(self,movietype):
        for i in movietype.split('/'):
            if i in self.lagelse:
                return True
