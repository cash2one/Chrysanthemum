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
	cpTagSQL = "INSERT INTO tbl_cp_tag(ref_cp_code, ref_tag_definition_id, ref_area_code, ref_brand_code"\
		") VALUES(%(ref_cp_code)s, "\
		"%(ref_tag_definition_id)s, %(ref_area_code)s, %(ref_brand_code)s)"

	cursor.executemany(cpTagSQL, data)
	conn.commit() # commit the operation, or it wont take effect
	# print len(data), cursor.statusmessage

	return True

# insert tbl_cp_tag_result
# conn connection to the database
# cursor a db table cursor
# data tuple
def insertCPTagResult(conn, cursor, data):
	cpTagResSQL = "INSERT INTO tbl_cp_tag_result(ref_cp_code, "\
		"ref_tag_type_code, tag_name, tag_value, ref_area_code) "\
		"VALUES(%(ref_cp_code)s, %(ref_tag_type_code)s, %(tag_name)s, "\
		"%(tag_value)s, %(ref_area_code)s)"

	cursor.executemany(cpTagResSQL, data)
	conn.commit()
	# print len(data), cursor.statusmessage

	return True

# get tag name
# 根据pid获取父亲节点名称
# conn connection to the database
# cursor a db table cursor
# pid
def getTagName(conn, cursor, pid):
	tag_name = None
	sql = "select name from tbl_tag_definition where id = " + str(pid) + " limit 1"
	cursor.execute(sql)
	rtv = cursor.fetchone()

	if rtv is not None:
		tag_name = rtv['name']

	return tag_name

def getTag(conn, cursor, name):
        sql = "select id, pid, ref_tag_type_code from tbl_tag_definition where name = '" + name + "'"
        cursor.execute(sql)
	rtv = cursor.fetchone()
	if rtv:
		return (rtv['id'], rtv['pid'], rtv['ref_tag_type_code'])
	else:
		return False

def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]

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
    cpTagResLst = []

    reader = unicode_csv_reader(open('../data/商场品牌表.csv'))
    for row in reader:
        if len(cpTagLst) >= 100:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            insertCPTagResult(conn, cpTagResCursor, tuple(cpTagResLst))
            print "Inserted", len(cpTagLst)
            cpTagLst = []
            cpTagResLst = []

        cpPropSql = "select cp.point_code, cp.ref_area_code, p.point_name from tbl_cp cp inner join tbl_cp_prop p on cp.point_code = p.ref_cp_code where p.ref_cptype_code like 'CP-BUSSINESS-NONMAP%' and point_name like '%" + row[0] + "%'"
        cpPropCursor.execute(cpPropSql)
        for cpPropRow in cpPropCursor:
            for tag in row[1:]:
                tagRtv = getTag(conn, tagDefCursor, tag)
                if tagRtv:
                    tmpDict = {
                        'ref_cp_code': cpPropRow['point_code'],
                        'ref_tag_definition_id': tagRtv[0],
                        'ref_area_code': cpPropRow['ref_area_code'],
                        'ref_brand_code': getBrandCode(conn, brandCursor, row[0], 'BUSSINESS-NONMAP')
                    }
                    cpTagLst.append(tmpDict)

                    tmpCPTagResDict = {
                        'ref_cp_code': cpPropRow['point_code'],
                        'ref_tag_type_code': tagRtv[2],
                        'tag_name': getTagName(conn, tagDefCursor, tagRtv[1]),
                        'tag_value': tag,
                        'ref_area_code': cpPropRow['ref_area_code']
                    }
                    cpTagResLst.append(tmpCPTagResDict)

            count = count + 1


    print "Done", count


if __name__ == "__main__":
	main()
