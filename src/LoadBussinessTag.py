#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# File: LoadBussinessTag.py
# Date: 11/Jan/2017
# Desc: 载入翁同学给的市场部重新定义的商场标签数据。i dont like structure of the data
#       changing and didt give time to adjust the code.
import psycopg2
import psycopg2.extras

tags = []
with open('../data/bussinesstagsuniq.txt') as F:
    for line in F:
        tags.append(line.strip())

# connect to the psql and insert the upper tags into tbl_tag_def
connStr = "host='127.0.0.1' dbname='jmtool20161229' user='postgres' password='postgres'"
conn = psycopg2.connect(connStr)
tagDefCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

idx = 1
for tag in tags:
    id = idx
    name = tag
    descriptions = tag
    sql = "insert into tbl_tag_def (id, name, descriptions, flag) values (" + str(id) + ", '" + name + "', '" + descriptions + "', 'BUSSINESS')"
    print sql
    tagDefCursor.execute(sql)
    conn.commit()

    idx = idx + 1
