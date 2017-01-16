#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# Date: 15/Jan/2017
# Desc: 第二阶段按照point_name字段给家电行业标注，在第一阶段的基础上
#       在阶段一清洗的基础上，根据采集点point_name和标签系统匹配情况来处理。
# File: TagJiaDianV2.py
import psycopg2
import psycopg2.extras
import sys

# insert tbl_cp_tag
# conn connection to the database
# cursor a db table cursor
# data tuple
def insertCPTag(conn, cursor, data):
	cpTagSQL = "INSERT INTO tbl_cp_tag_relation(ref_cp_code, ref_tag_definition_id, ref_area_code, ref_brand_code"\
		", flag) VALUES(%(ref_cp_code)s, "\
		"%(ref_tag_definition_id)s, %(ref_area_code)s, %(ref_brand_code)s, %(flag)s)"

	cursor.executemany(cpTagSQL, data)
	conn.commit() # commit the operation, or it wont take effect
	print len(data), cursor.statusmessage

	return True


def main():
    connStr = "host='127.0.0.1' dbname='jmtool20161229' user='postgres' password='postgres'"
    conn = psycopg2.connect(connStr)

    cpPropCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpExPropCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    tagDefCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    brandCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpTagCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpTagResCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # load tags
    TagDefs = []
    with open('../data/jiadianpointname.csv') as F:
        for line in F:
            TagDefs.append(line.strip().split(','))

    cpTagLst = []
    count = 0

    # point_name, CP-JIADIAN-SELL
    cpPropSql = "select ref_cp_code, point_name from tbl_cp_prop where ref_cptype_code = 'CP-JIADIAN-SELL' and ref_cp_code not in (select ref_cp_code from tbl_cp_tag_relation where flag = 'JIADIANV1')"
    cpPropCursor.execute(cpPropSql)

    for cpPropRow in cpPropCursor:
        if len(cpTagLst) >= 100:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            print "Inserted", len(cpTagLst)
            count = count + len(cpTagLst)
            cpTagLst = []

        for tagDef in TagDefs:
            if tagDef[0] in cpPropRow['point_name']:
                tmpDict = {
                    'ref_cp_code': cpPropRow['ref_cp_code'],
                    'ref_tag_definition_id': 1,
                    'ref_area_code': None,
                    'ref_brand_code': None,
                    'flag': 'JIADIANV2'
                }
                cpTagLst.append(tmpDict)
                break
            else:
                if cpPropRow['point_name'] in tagDef[0]:
                    tmpDict = {
                        'ref_cp_code': cpPropRow['ref_cp_code'],
                        'ref_tag_definition_id': 1,
                        'ref_area_code': None,
                        'ref_brand_code': None,
                        'flag': 'JIADIANV2'
                    }
                    cpTagLst.append(tmpDict)
                    break

    if len(cpTagLst) > 0:
        insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
        print "Inserted", len(cpTagLst)
        count = count + len(cpTagLst)
        cpTagLst = []

    print count

if __name__ == "__main__":
	main()
