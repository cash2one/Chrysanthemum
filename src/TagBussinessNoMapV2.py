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

# getBrandCode
# 根据传入的关键词在tbl_brand里面获取品牌code
# conn connection to the database
# cursor a db table cursor
# keyword like 格力空调
# industry like AT-JIADIAN
def getBrandCode(conn, cursor, keyword, industry):
	sql = "select brand_code from tbl_brand where ref_area_type_code = %s and chinese_name != '' and %s ~ chinese_name or english_name != '' and %s ~ english_name limit 1"
	# sql = "SELECT brand_code FROM tbl_brand WHERE ref_area_type_code = 'AT-"\
	# 	+ industry + "' AND chinese_name != '' AND '" + keyword + "' ~ chinese_name"\
	# 	+ " OR english_name != '' AND '" + keyword + "' ~ english_name LIMIT 1"
	cursor.execute(sql, ('AT-' + industry, keyword, keyword))
	rtv = cursor.fetchone()
	# return rtv['brand_code']
	if rtv:
		return rtv['brand_code']
	else:
		return None

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
	# print len(data), cursor.statusmessage

	return True

def isInLst(Lst, query):
    if len(query) == 0:
        return False

    for l in Lst:
        if len(l) == 0:
            continue

        if query in l:
            print 'isInLst', query, l
            return True

    return False

def isInLstReverse(Lst, query):
    if len(query) == 0:
        return False

    for l in Lst:
        if len(l) == 0:
            continue

        if l in query:
            print 'isInLstReverse', query, l
            return True

    return False


def main():
    conn_string = "host='127.0.0.1' dbname='jmtool20161229'"\
    + " user='postgres' password='postgres'"
    conn = psycopg2.connect(conn_string)
    cpPropCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    brandCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    tagDefCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpTagCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpTagResCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    count = 0
    cpTagLst = []

    # 载入对照表
    BrandTags = []
    with open('../data/商场品牌表.csv') as F:
        for line in F:
            BrandTags.append(line.strip().split(','))


    cpPropSql = "select cp.point_code, cp.ref_area_code, p.point_name from tbl_cp cp inner join tbl_cp_prop p on cp.point_code = p.ref_cp_code where (p.ref_cptype_code = 'CP-BUSSINESS-NONMAP-INDOOR' or p.ref_cptype_code = 'CP-BUSSINESS-NONMAP-OUTDOOR')"
    cpPropCursor.execute(cpPropSql)
    for cpPropRow in cpPropCursor:
        if len(cpTagLst) >= 100:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            print "Inserted", len(cpTagLst)
            cpTagLst = []

        for brandTag in BrandTags:
            tmpLst = brandTag[0:1]

            rtvReverse = isInLstReverse(tmpLst, cpPropRow['point_name']) # point_name中包含品牌
            if rtvReverse:
                tmpDict = {
                    'ref_cp_code': cpPropRow['point_code'],
                    'ref_tag_definition_id': 1,
                    'ref_area_code': cpPropRow['ref_area_code'],
                    'ref_brand_code': None,
                    'flag': 'BV2'
                }
                cpTagLst.append(tmpDict)
                break
            else:
                rtv = isInLst(tmpLst, cpPropRow['point_name']) # 品牌中包含point_name
                if rtv:
                    tmpDict = {
                        'ref_cp_code': cpPropRow['point_code'],
                        'ref_tag_definition_id': 1,
                        'ref_area_code': cpPropRow['ref_area_code'],
                        'ref_brand_code': None,
                        'flag': 'BV2'
                    }
                    cpTagLst.append(tmpDict)
                    break

    if len(cpTagLst) > 0:
        insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
        print "Inserted", len(cpTagLst)
        cpTagLst = []

if __name__ == "__main__":
	main()
