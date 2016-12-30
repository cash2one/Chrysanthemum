#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# Date: 30/Dec/2016
# Desc: 第二阶段给商场类型的采集点打标签
#       根据品牌对照表
# File: TagBussinessNoMapV2.py
import psycopg2
import psycopg2.extras
import pprint
import sys
import csv

def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]

def main():
    conn_string = "host='127.0.0.1' dbname='jmtool20161229'"\
    + " user='postgres' password='postgres'"
    conn = psycopg2.connect(conn_string)
    cpPropCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    count = 0

    reader = unicode_csv_reader(open('../data/商场品牌表.csv'))
    for filed1, filed2, filed3, filed4, filed5 in reader:
        print filed1, filed2, filed3, filed4, filed5
        cpPropSql = "select * from tbl_cp_prop where point_name like '%" + filed1 + "%'"
        cpPropCursor.execute(cpPropSql)
        for cpPropRow in cpPropCursor:
            count = count + 1


    print "Done", count


if __name__ == "__main__":
	main()
