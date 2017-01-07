#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# Date: 30/Dec/2016
# Desc: 第一阶段给专科医院类型的采集点打标签
#       根据品牌对照表
# File: TagPrivateHospitalV1.py
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
	cpTagSQL = "INSERT INTO tbl_cp_tag_relation(ref_cp_code, ref_tag_definition_id, ref_area_code, ref_brand_code, flag"\
		") VALUES(%(ref_cp_code)s, "\
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

    # point_name
    # cpPropSql = "select * from tbl_cp_prop where ref_cptype_code like 'CP-BUSSINESS-NONMAP%'"
    cpPropSql = "select cp.point_code, cp.ref_area_code, p.point_name from tbl_cp cp inner join tbl_cp_prop p on cp.point_code = p.ref_cp_code where p.ref_cptype_code like 'CP-PRIVATE-HOSPITAL-ROOM%'"
    cpPropCursor.execute(cpPropSql)

    for cpPropRow in cpPropCursor:
        if len(cpTagLst) >= 100:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            print "Inserted", len(cpTagLst)
            cpTagLst = []

        tagDefSql = "select * from tbl_tag_definition where node_type = 'PROP_VALUE' and '" +cpPropRow['point_name'].replace("'", " ") + "' like '%' || name || '%' order by id desc"
        tagDefCursor.execute(tagDefSql)
        for tagDefRtv in tagDefCursor:
            tmpDict = {
                'ref_cp_code': cpPropRow['point_code'],
                'ref_tag_definition_id': tagDefRtv['id'],
                'ref_area_code': cpPropRow['ref_area_code'],
                'ref_brand_code': getBrandCode(conn, brandCursor, cpPropRow['point_name'], 'PRIVATE-HOSPITAL'),
                'flag': 'PHV1'
            }
            cpTagLst.append(tmpDict)

        tagDefSql = "select * from tbl_tag_definition where node_type = 'PROP_VALUE' and name like '%" + cpPropRow['point_name'].replace("'", " ") + "%' order by id desc"
        tagDefCursor.execute(tagDefSql)
        for tagDefRtv in tagDefCursor:
            if not isInCpTagRelation(conn, cpTagResCursor, cpPropRow['point_code'], tagDefRtv['id']):
                tmpDict = {
                    'ref_cp_code': cpPropRow['point_code'],
                    'ref_tag_definition_id': tagDefRtv['id'],
                    'ref_area_code': cpPropRow['ref_area_code'],
                    'ref_brand_code': getBrandCode(conn, brandCursor, cpPropRow['point_name'], 'PRIVATE-HOSPITAL'),
                    'flag': 'PHV1'
                }
                cpTagLst.append(tmpDict)

    if len(cpTagLst) > 0:
        insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
        print "Inserted", len(cpTagLst)
        cpTagLst = []

    # prop_value, the folloing sql will be fuck slow, 商场在tbl_cp_exprop里面没有数据，所以下面的逻辑可以不执行
    # cpExPropSql = "select cp.point_code, cp.ref_area_code, ex.prop_value from tbl_cp cp inner join tbl_cp_prop p on cp.point_code = p.ref_cp_code inner join tbl_cp_exprop ex on cp.point_code = ex.ref_cp_code where p.ref_cptype_code like 'CP-PRIVATE-HOSPITAL-ROOM%' and cp.point_code not in (select ref_cp_code from tbl_cp_tag)"
    # cpExPropCursor.execute(cpExPropSql)
    # cpTagLst = []
    # cpTagResLst = []
    # for cpExPropRow in cpExPropCursor:
    #     if len(cpTagLst) >= 100:
    #         insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
    #         insertCPTagResult(conn, cpTagResCursor, tuple(cpTagResLst))
    #         print "Inserted", len(cpTagLst)
    #         cpTagLst = []
    #         cpTagResLst = []
    #
    #     tagDefSql = "select * from tbl_tag_definition where node_type = 'PROP_VALUE' and '" +cpExPropRow['prop_value'].replace("'", " ") + "' like '%' || name || '%' order by id desc limit 1"
    #     tagDefCursor.execute(tagDefSql)
    #     tagDefRtv = tagDefCursor.fetchone()
    #     if tagDefRtv is not None:
    #         tmpDict = {
    #             'ref_cp_code': cpExPropRow['point_code'],
    #             'ref_tag_definition_id': tagDefRtv['id'],
    #             'ref_area_code': cpExPropRow['ref_area_code'],
    #             'ref_brand_code': getBrandCode(conn, brandCursor, cpExPropRow['prop_value'], 'PRIVATE-HOSPITAL')
    #         }
    #         cpTagLst.append(tmpDict)
    #
    #         tmpCPTagResDict = {
    #             'ref_cp_code': cpPropRow['point_code'],
    #             'ref_tag_type_code': tagDefRtv['ref_tag_type_code'],
    #             'tag_name': getTagName(conn, tagDefCursor, tagDefRtv['pid']),
    #             'tag_value': tagDefRtv['name'],
    #             'ref_area_code': cpPropRow['ref_area_code']
    #         }
    #         cpTagResLst.append(tmpCPTagResDict)
    #
    # if len(cpTagLst) > 0:
    #     insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
    #     insertCPTagResult(conn, cpTagResCursor, tuple(cpTagResLst))
    #     print "Inserted", len(cpTagLst)
    #     cpTagLst = []
    #     cpTagResLst = []

if __name__ == "__main__":
	main()
