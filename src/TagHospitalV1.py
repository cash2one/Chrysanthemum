#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# Date: 21/Jan/2017
# Desc: 第一阶段给医院类型的采集点打标签
#       根据定义的科室关联词 ../data/hospitaltags.csv
# File: TagHospitalV1.py
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

def isInCpTagRelation(conn, cursor, ref_cp_code, tagId):
    sql = "select * from tbl_cp_tag_relation where ref_cp_code = '" + ref_cp_code + "' and ref_tag_definition_id = " + str(tagId) + " limit 1"
    cursor.execute(sql)
    rtv = cursor.fetchone()

    if rtv is not None:
        return True
    else:
        return False

def isInLst(Lst, query):
    if len(query) == 0:
        return False

    for l in Lst:
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

    TagDefs = []
    tagDefCursor.execute("select id, name, descriptions from tbl_tag_def where flag = 'HOSPITAL'")
    for tagDefRtv in tagDefCursor:
        TagDefs.append([tagDefRtv['id'], tagDefRtv['name'], tagDefRtv['descriptions']])


    # point_name, ROOM, 审核通过的
    cpPropSql = "select cp.point_code, cp.ref_area_code, p.point_name from tbl_cp_prop p inner join tbl_cp cp on cp.point_code = p.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = cp.ref_area_code where (p.ref_cptype_code = 'CP-HOSPITAL-ROOM' or p.ref_cptype_code = 'CP-PRIVATE-HOSPITAL-ROOM') and (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107 or b.status_result = 1067)"
    cpPropCursor.execute(cpPropSql)
    print '查询完毕'
    for cpPropRow in cpPropCursor:
        if len(cpTagLst) >= 100:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            print "Inserted", len(cpTagLst)
            cpTagLst = []

        # 循环TagDefs，拿着point_name和里面的descriptions匹配
        for tagDef in TagDefs:
            tmpLst = tagDef[2].split(',')

            rtvReverse = isInLstReverse(tmpLst, cpPropRow['point_name'].upper()) # point_name中包含科室

            if rtvReverse:
                tmpDict = {
                    'ref_cp_code': cpPropRow['point_code'],
                    'ref_tag_definition_id': tagDef[0],
                    'ref_area_code': cpPropRow['ref_area_code'],
                    'ref_brand_code': getBrandCode(conn, brandCursor, cpPropRow['point_name'], 'HOSPITAL'),
                    'flag': 'HV1'
                }
                cpTagLst.append(tmpDict)
                break
            else:
                rtv = isInLst(tmpLst, cpPropRow['point_name'].upper()) # 科室中包含point_name
                if rtv:
                    tmpDict = {
                        'ref_cp_code': cpPropRow['point_code'],
                        'ref_tag_definition_id': tagDef[0],
                        'ref_area_code': cpPropRow['ref_area_code'],
                        'ref_brand_code': getBrandCode(conn, brandCursor, cpPropRow['point_name'], 'HOSPITAL'),
                        'flag': 'HV1'
                    }
                    cpTagLst.append(tmpDict)
                    break

    if len(cpTagLst) > 0:
        insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
        print "Inserted", len(cpTagLst)
        cpTagLst = []


if __name__ == "__main__":
	main()
