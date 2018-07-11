# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from codecs import open
import json
import time
import psycopg2
from douban.items import DoubanItem
from douban.settings import DATABASE

class DoubanPipeline(object):

    red = "\033[31;1m  %s  \033[0m"
    blue = "\033[34;1m  %s  \033[0m"
    green = "\033[1;32;40m  %s  \033[0m"
    yellow = "\033[33;1m  %s  \033[0m"
    
    def __init__(self):
        try:
            self.db = psycopg2.connect(database=DATABASE['database'], user=DATABASE['user'], password=DATABASE['password'], host=DATABASE['ip'], port=DATABASE['port'])
            self.cursor = self.db.cursor()
            print self.green % (u"==> connect db sucessfully<==")
        except Exception as e:
            print self.red % (u"==> fail to connect db!!!<==")

    def gen_sql(self, item):
        keys = item.fields.keys()
        

    def process_item(self, item, spider):
        # self.cursor.execute("Select * FROM public.movie_data limit 0")
        # colnames = [desc[0] for desc in self.cursor.description]
        # for i in colnames:
        #     print i
        print self.blue %"in pipeline........."
        times = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        try:
            if isinstance(item,DoubanItem):
                print json.dumps(dict(item),ensure_ascii=False, indent=4)
                check_sql = "select media_url from public.movie_data_bak where media_url='%s'"
                self.cursor.execute(check_sql%item['url'])   
                result = self.cursor.fetchall()
                if result:
                    print self.yellow % 'Movie Data Exist'
                else:
                    self.cursor.execute("""insert into public.movie_data_bak(\
                     media_url,
                     media_type1,
                     media_title,
                     media_director,
                     media_screenwriter,
                     media_starring,
                     media_type2,
                     media_language,
                     media_premiere,
                     media_region,
                     media_length,
                     media_alias, 
                     media_score,
                     media_episodes,
                     media_summary,
                     youku_url,
                     tencent_url,
                     iqiyi_url,
                     letv_url,
                     huashu_url,
                     souhu_url,
                     cntv_url,
                     pptv_url,
                     mgtv_url,
                     create_time
                     ) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", \
                                    (item['url'],
                                    item['movie_type'],
                                    item['title'],
                                    item['directors'],
                                    item['scriptwriters'],
                                    item['actors'],
                                    item['types'],
                                    item['languages'],
                                    item['release_date'],
                                    item['release_region'],
                                    item['duration'],
                                    item['alias'],
                                    item['score'],
                                    item['media_episoders'],
                                    item['description'],
                                    item['youku_url'],
                                    item['tencent_url'],
                                    item['iqiyi_url'],
                                    item['letv_url'],
                                    item['huashu_url'],
                                    item['souhu_url'],
                                    item['cntv_url'],
                                    item['pptv_url'],
                                    item['mgtv_url'],
                                    times
                                    ))
                    print self.green % ("==> sucess to insert into table DOUBAN!")
                    self.cursor.execute("UPDATE unrepeatable_url_movie set flag=1 WHERE url='{}'".format(item['url'].decode('utf-8')))
                    print self.green % ("==> sucess to update into table DOUBAN!")
        except Exception as e:
            print  self.red % "DoubanPipeline ERROR %s" % e
            self.db.rollback()

    def _del__(self):
        self.db.close()
        print self.green % ("==>database closed sucess<==")