#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# File: LoadTagAndAssociates.py
# Date: 06/Jan/2017
# Desc: 载入翁同学定义的医院科室和相关关键词的数据到标签定义表格
import psycopg2
import psycopg2.extras

mat = []
with open('../data/hospitaltags.csv') as F:
    for line in F:
        # print set(line.strip().split(','))
        # for x in set(line.strip().split(',')): print x
        mat.append(list(set(line.strip().split(','))))

# connect to the psql and insert the upper tags into tbl_tag_def
connStr = "host='127.0.0.1' dbname='jmtool20161229' user='postgres' password='postgres'"
conn = psycopg2.connect(connStr)
tagDefCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

idx = 1
for row in mat:
    id = idx
    name = row[0]
    descriptions = ','.join(row)
    sql = "insert into tbl_tag_def (id, name, descriptions, flag) values (" + str(id) + ", '" + name + "', '" + descriptions + "', 'HOSPITAL')"
    print sql
    tagDefCursor.execute(sql)
    conn.commit()

    idx = idx + 1
