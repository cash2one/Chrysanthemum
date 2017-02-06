#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# Date: 30/Dec/2016
# Desc: 第一阶段给医院类型的采集点打标签
#       根据品牌对照表
# File: Tag4SV1.py
import psycopg2
import psycopg2.extras
import sys

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

def getTagId(conn, cursor, name):
    tagId = None

    sql = "select id from tbl_tag_def where name = '" + name + "' limit 1"
    cursor.execute(sql)
    rtv = cursor.fetchone()

    if rtv is not None:
        tagId = rtv['id']

    return tagId

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
	cpTagSQL = "INSERT INTO tbl_cp_tag_relation(ref_cp_code, ref_tag_definition_id, ref_area_code, ref_brand_code,"\
		"flag) VALUES(%(ref_cp_code)s, "\
		"%(ref_tag_definition_id)s, %(ref_area_code)s, %(ref_brand_code)s, %(flag)s)"

	cursor.executemany(cpTagSQL, data)
	conn.commit() # commit the operation, or it wont take effect
	print len(data), cursor.statusmessage

	return True

def isInLst(Lst, query):
    if len(query) == 0:
        return False

    for l in Lst:
        if len(l) == 0:
            continue

        if query in l:
            return True

    return False

def isInLstReverse(Lst, query):
    if len(query) == 0:
        return False

    for l in Lst:
        if len(l) == 0:
            continue

        if l in query:
            return True

    return False

def main():
    connStr = "host='127.0.0.1' dbname='jmtool20161229' user='postgres' password='postgres'"
    conn = psycopg2.connect(connStr)

    cpPropCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpExPropCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    tagDefCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    brandCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpTagCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpTagResCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cpTagLst = []

    Brands = []
    with open('../data/4stags.csv') as F:
        for line in F:
            tmpLst = line.strip().split(' ')
            # 车类，车型
            Brands.append([tmpLst[0], ' '.join(tmpLst[1:])])
            # Brands.append(line.strip())

    # point_name
    cpPropSql = "select cp.point_code, cp.ref_area_code, p.point_name from tbl_cp cp inner join tbl_cp_prop p on cp.point_code = p.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = cp.ref_area_code where p.ref_cptype_code like 'CP-4S-SHOWROOM%' and (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067)"
    cpPropCursor.execute(cpPropSql)
    for cpPropRow in cpPropCursor:
        if len(cpTagLst) >= 100:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            print "Inserted", len(cpTagLst)
            cpTagLst = []

        # 循环Brands，拿着point_name和其匹配
        for brand in Brands:
            if brand[1] in cpPropRow['point_name']: # point_name中包含车型的
            # tmpLst = brand
            #
            # rtvReverse = isInLstReverse(tmpLst, cpPropRow['point_name']) # point_name中包含品牌或者型号
            #
            # if rtvReverse:
                print brand[1], 'POINT_NAME', cpPropRow['point_name']
                tmpDict = {
                    'ref_cp_code': cpPropRow['point_code'],
                    'ref_tag_definition_id': 1,
                    'ref_area_code': cpPropRow['ref_area_code'],
                    'ref_brand_code': None,
                    'flag': '4SV1.1'
                }
                cpTagLst.append(tmpDict)
                break
            else:
                if cpPropRow['point_name'] in brand[1]:
                # rtv = isInLst(tmpLst, cpPropRow['point_name']) # 品牌中包含point_name
                # if rtv:
                    print brand[1], 'POINT_NAME', cpPropRow['point_name']
                    tmpDict = {
                        'ref_cp_code': cpPropRow['point_code'],
                        'ref_tag_definition_id': 1,
                        'ref_area_code': cpPropRow['ref_area_code'],
                        'ref_brand_code': None,
                        'flag': '4SV1.1'
                    }
                    cpTagLst.append(tmpDict)
                    break

    if len(cpTagLst) > 0:
        insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
        print "Inserted", len(cpTagLst)
        cpTagLst = []

if __name__ == "__main__":
	main()
