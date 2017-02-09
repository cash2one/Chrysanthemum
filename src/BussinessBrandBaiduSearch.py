#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# Desc: 拿着4w+的品牌去百度搜索整理品类信息
# File: BussinessBrandBaiduSearch.py
# Date: 08/Feb/2017
import urllib
import urllib2
import re
import random
import time

# 载入品牌
Brands = []
with open('../data/bussiness4wbrands.txt', 'r') as F:
    for line in F:
        if len(line.strip()) == 0:
            continue
        Brands.append(line.strip())

# 定义品类关键词
pinLei = ['服装', '鞋帽', '箱包', '珠宝首饰', '美装', '餐饮', '娱乐', '教育', '超市', '瘦身纤体', '保健品']

Res = []

print 'Starting Loop ...'
for brand in Brands:
    baseUrl = 'http://www.baidu.com/s'
    page = 1 #第几页
    word = brand  #搜索关键词

    data = {'wd':word,'pn':str(page-1)+'0','tn':'baidurt','ie':'utf-8','bsst':'1'}
    data = urllib.urlencode(data)
    url = baseUrl+'?'+data

    try:
        request = urllib2.Request(url)
        response = urllib2.urlopen(request)
    except urllib2.HttpError,e:
        print e.code
        exit(0)
    except urllib2.URLError,e:
        print e.reason
        exit(0)

    html = response.read()
    print 'Result: '

    for p in pinLei:
        if p in html:
            print brand, p
            Res.append([brand, p])
            break

    time.sleep(random.randrange(1, 10))

with open('../tmp/BussinessBrandBaiduSearch.txt', 'w') as F:
    for row in Res:
        F.write(row[0] + '\t' + row[1] + '\n')
print 'Done'
