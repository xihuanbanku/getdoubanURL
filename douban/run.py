#coding:utf-8
import subprocess
import time

from scrapy import cmdline

while True:
    # cmdline.execute('scrapy crawl videowebset'.split())
    subprocess.call('scrapy crawl movie_gather --nolog', shell=True)
    time.sleep(5)
