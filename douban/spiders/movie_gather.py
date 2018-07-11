#!/usr/bin/env python
# coding=utf-8
import random
import time
from urllib2 import quote

import psycopg2
import scrapy
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

from douban.settings import DATABASE


class MovieGatherSpider(scrapy.Spider):
    name = 'movie_gather'
    start_urls = ['https://www.douban.com/']

    def __init__(self):
        self.cookie={}
        cookies = 'gr_user_id=b0d9a237-aa82-4b1f-9def-c87653259ea4; ll="108288"; viewed="5337254_26993157_10590856_25779298"; bid=MbIatP8Uy9k; __yadk_uid=pzXMbRXdx2O9a6H11yW0Ov8dwQY35iF7; ps=y; dbcl2="174643900:UD/7+sfcADQ"; ct=y; ck=f-sV; ap=1; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1520404252%2C%22https%3A%2F%2Fwww.douban.com%2F%22%5D; as="https://sec.douban.com/b?r=https%3A%2F%2Fmovie.douban.com%2F"; _pk_id.100001.4cf6=84f120e21ffdaba9.1519269036.32.1520405069.1520397782.; _pk_ses.100001.4cf6=*; __utma=30149280.1789096787.1478770268.1520397757.1520404253.59; __utmb=30149280.0.10.1520404253; __utmc=30149280; __utmz=30149280.1520404253.59.31.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utma=223695111.1772094035.1519269036.1520397782.1520404253.32; __utmb=223695111.0.10.1520404253; __utmc=223695111; __utmz=223695111.1520404253.32.12.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; push_noty_num=0; push_doumail_num=0; _vwo_uuid_v2=39E6F0B28328A4287CEDDE656A81EE0D|651218b2e691a30aa8f71f79fc9e9382'
        for line in cookies.split(';'):
            key,value = line.strip().split('=', 1) #1代表只分一次，得到两个数据
            self.cookie[key] = value.replace('"','')
        self.db = psycopg2.connect(database=DATABASE['database'], user=DATABASE['user'], password=DATABASE['password'], host=DATABASE['ip'], port=DATABASE['port'])
        self.cur = self.db.cursor()
        # m_path="F:\py_workspace\getdoubanURL\douban\spiders\driver\geckodriver.exe"
        m_path="/home/hadoop/deploy/spider/getdoubanURL/douban/spiders/driver/geckodriver_linux"
        options = Options()
        options.add_argument("--headless")
        firefox_profile = webdriver.FirefoxProfile()
        firefox_profile.set_preference('permissions.default.image', 2)
        self.driver = webdriver.Firefox(firefox_options=options, firefox_profile=firefox_profile, executable_path=m_path)
        self.driver.implicitly_wait(20)
        self.__get_task()
        self.driver.quit()
        self.db.close()      

    def __get_task(self):
        # display = Display(visible=0, size=(1024, 768))
        # display.start()
        self.loggerWithTime( u'Read KeyWord Task!!!')
        read_sql = 'select keyword from public.tb_movie_keyword_task where flag=0'
        self.cur.execute(read_sql)
        keyword_task = self.cur.fetchall()
        self.loggerWithTime(u'requests done!!!')
        for keyword, in keyword_task:
            self.loggerWithTime(keyword)
            search_link = "https://movie.douban.com/subject_search?search_text=%s&cat=1002" % quote(keyword)
            self.driver.get(search_link)
            time.sleep(random.randrange(3,5))
            #self.driver.find_element_by_xpath('//input[@name="search_text"]').send_keys(keyword.decode('utf-8'))
            #self.loggerWithTime 'find_element_by_xpath'
            #self.driver.find_element_by_xpath('//input[@type="submit"]').click()
            #time.sleep(random.randrange(3,8))
            self.parse_search_list()
            self.cur.execute(u"UPDATE public.tb_movie_keyword_task set flag=1 WHERE keyword='{}'".format(keyword.decode('utf-8')))
            self.db.commit()
            #break 

    #分页抽取每部影片详情URL
    def parse_search_list(self):
        movie_link_elements = self.driver.find_elements_by_xpath('//a[contains(@href,"movie.douban.com/subject")]')
        movie_links = [link.get_attribute('href') for link in movie_link_elements]
        #影片URL过滤入库
        self.insert_data(list(set(movie_links)))
        if u'检测到有异常' in self.driver.page_source:
            self.loggerWithTime(u'该IP已被限制访问........')
            return False
        next_elements = self.driver.find_elements_by_xpath('//a[@class="next"]')
        if next_elements:
            next_elements[0].click()
            time.sleep(random.randrange(2,5))
            return self.parse_search_list()
        else:
            return True

    #影片详情URL去重入库
    def insert_data(self,movie_links):
        for link in movie_links:
            check_sql = "select url from public.tb_movie_url_task where url ='%s'"%link
            self.cur.execute(check_sql)
            check_data = self.cur.fetchall()
            if check_data:
                self.loggerWithTime("URL 已存在 break......")
            else:
                self.cur.execute("insert into public.tb_movie_url_task(url) values('{}')".format(link))
                self.loggerWithTime("URL Task Insert OK!!!")
            self.db.commit()
    def parse(self,response):
        pass

    def __del__(self):
        # try:
        #     if hasattr(self,'display'):
        #         self.display.stop()
        #         self.loggerWithTime("===========display stop==========")
        # except:
        #     pass
        self.driver.quit()
        self.db.close()
        self.loggerWithTime ("===========driver close==========")

    #打印当前时间的消息
    def loggerWithTime(self, message):
        now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print("[%s][%s]"%(now, message))
