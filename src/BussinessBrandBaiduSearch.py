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

# 载入已经处理的
Processed = []
with open('../tmp/log.txt') as F:
    for line in F:
        if len(line.strip()) == 0:
            continue
        Processed.append(line.strip())

# 定义品类关键词
pinLei = ['服装', '鞋帽', '箱包', '珠宝首饰', '美装', '餐饮', '娱乐', '教育', '超市', '瘦身纤体', '保健品']

Res = []

print 'Starting Loop ...'
for brand in Brands[11545:]:
    if brand in Processed:
        continue
    if Brands.index(brand) == len(Brands) - 1:
        continue

    baseUrl = 'http://www.baidu.com/s'
    page = 1 #第几页
    word = brand  #搜索关键词

    data = {
        'ie':'utf-8',
        'f': '8',
        'rsv_bp': '1',
        'rsv_idx': '2',
        'tn': 'baiduhome_pg',
        'rsv_spt': '1',
        'oq': 'csrgxtu',
        'rsv_pq': '1',
        'rsv_t': '2',
        'rqlang': 'cn',
        'rsv_enter': '0',
        'wd': word,
    }
    data = urllib.urlencode(data)
    url = baseUrl+'?'+data

    try:
        request = urllib2.Request(url)
        request.add_header('User-Agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36')
        response = urllib2.urlopen(request)
    except urllib2.HTTPError,e:
        print e.code
        with open('../tmp/BussinessBrandBaiduSearch.txt', 'a') as F:
            for row in Res:
                F.write(row[0] + '\t' + row[1] + '\n')
                print 'Done'
        exit(0)
    except urllib2.URLError,e:
        print e.reason
        with open('../tmp/BussinessBrandBaiduSearch.txt', 'a') as F:
            for row in Res:
                F.write(row[0] + '\t' + row[1] + '\n')
                print 'Done'
        exit(0)

    html = response.read()
    print 'Result:[' + brand + '] ', response.getcode()
    with open('../tmp/log.txt', 'a') as F:
        F.write(brand)

    for p in pinLei:
        if p in html:
            print brand, p
            Res.append([brand, p])
            break

    time.sleep(random.randrange(1, 5))
    # time.sleep(0.5)
