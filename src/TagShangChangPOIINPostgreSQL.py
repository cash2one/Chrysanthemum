#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# File: TagShangChangPOIInPostgreSQL.py
# Desc: 为数据库中的采集点数据打标签，原始采集点数据在tbl_cp, tbl_cp_prop,
#       tbl_cp_exprop, 标签数据在tbl_tag_definition, 打标签的结果存放
#       在关系表格tbl_cp_tag中。
#       先从tbl_tag_definition里面获取一条，然后去tbl_cp里面过滤包含当前tag名称
#       的记录，对找到的记录又根据tag id和记录id存入tbl_cp_tag表格中。
#
#	    打标签就是向tbl_cp_tag表格里面存入标签信息ref_tag_definition_id和
#       采集点信息ref_cp_code，以将其关联起来。
#
#		还需要顺便插入tbl_cp_tag_result表格， 上面的表格只是一个中间表格
#
#       上面的方法可以根据tbl_cp_prop里面的point_name给医院相关的采集点数据打标签
#       但是因为point_name是采集员手动输入，point_name参差不齐，下来还需要根据表格
#       tbl_cp_exprop中prop_value来打标签，但是并不是每个在tbl_cp_prop里面的点
#       都有对应tbl_cp_exprop里面的记录。
#
# Date: 14/Nov/2016
import psycopg2
import psycopg2.extras
import pprint
import sys

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

# get tag name
# 根据pid获取父亲节点名称
# conn connection to the database
# cursor a db table cursor
# pid
def getTagName(conn, cursor, pid):
	tag_name = None
	cursor.execute("SELECT * FROM tbl_tag_definition WHERE "\
		"pid < " + str(pid) + " ORDER BY pid DESC LIMIT 1")

	rtv = cursor.fetchone()
	if rtv is not None:
		tag_name = rtv['name']

	return tag_name

# getRefAreaCode
# 根据ref_cp_code获取ref_area_code
# conn connection to the database
# cursor a db table cursor
# ref_cp_code
def getRefAreaCode(conn, cursor, ref_cp_code):
	sql = "SELECT ref_area_code FROM tbl_cp WHERE point_code = '" + ref_cp_code + "' LIMIT 1"
	cursor.execute(sql)
	rtv = cursor.fetchone()
	return rtv['ref_area_code']

# getBrandCode
# 根据传入的关键词在tbl_brand里面获取品牌code
# conn connection to the database
# cursor a db table cursor
# keyword like 格力空调
# industry like AT-JIADIAN
def getBrandCode(conn, cursor, keyword, industry):
	sql = "SELECT brand_code FROM tbl_brand WHERE ref_area_type_code = 'AT-"\
		+ industry + "' AND chinese_name != '' AND '" + keyword + "' ~ chinese_name"\
		+ " OR english_name != '' AND '" + keyword + "' ~ english_name LIMIT 1"
	cursor.execute(sql)
	rtv = cursor.fetchone()
	# return rtv['brand_code']
	if rtv:
		return rtv['brand_code']
	else:
		return None

# isCPType
# 查看采集点是否属于某一种类型
# conn connection to the database
# cursor a db table cursor
# ref_cp_code
# type
def isCPType(conn, cursor, ref_cp_code, type):
	sql = "SELECT count(*) FROM tbl_cp_prop WHERE ref_cp_code = '" + ref_cp_code + "' AND ref_cptype_code LIKE 'CP-" + type + "%' LIMIT 1"
	cursor.execute(sql)
	rtv = cursor.fetchone()
	if rtv:
		return True
	else:
		return False

def main():
	# 记录标记了多少采集点
	counter = 0

	#Define our connection string
	conn_string = "host='192.168.0.21' dbname='jmtool3_1206_i'"\
		+ " user='postgres' password='postgres'"

	# print the connection string we will use to connect
	print "Connecting to database\n	->%s" % (conn_string)

	# get a connection, if a connect cannot be made an exception
	# will be raised here
	conn = psycopg2.connect(conn_string)
	print "Connected!\n"

	# conn.cursor will return a cursor object, you can use this
	# cursor to perform queries
	tagCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	tagDefCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cpCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cpPropCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cpProp1Cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cpTagCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cpTagResCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	brandCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

	# id, name, pid, node_type, ref_tag_type_code
	tagCursor.execute("SELECT * FROM tbl_tag_definition WHERE node_type = 'PROP_VALUE' OR node_type = 'EX_PROP_BRAND'")
	for tagRow in tagCursor:
		# 根据tag 的name在tbl_cp_prop, tbl_cp里面搜索
		# id, ref_cp_code, ref_cptype_code, point_name, floor_number
		cpPropSQL = "SELECT cp.ref_area_code, cp.point_code, prop.point_name FROM "\
		"tbl_cp_prop prop INNER JOIN tbl_cp cp "\
		"ON prop.ref_cp_code = cp.point_code WHERE prop.point_name LIKE "\
		" '%" + tagRow['name'] + "%' AND prop.ref_cptype_code LIKE 'CP-BUSSINESS-NONMAP%'"
		cpPropCursor.execute(cpPropSQL)
		cpTagDict = []
		cpTagResDict = []
		for cpPropRow in cpPropCursor:
			tmpDict = {
				'ref_cp_code': cpPropRow['point_code'],
				'ref_tag_definition_id': tagRow['id'],
				'ref_area_code': cpPropRow['ref_area_code'],
				'ref_brand_code': getBrandCode(conn, brandCursor, cpPropRow['point_name'], 'BUSSINESS-NONMAP')
			}
			cpTagDict.append(tmpDict)

			# 根据tagRow里面的pid选出父亲结点作为tag_name
			tmpCPTagResDict = {
				'ref_cp_code': cpPropRow['point_code'],
				'ref_tag_type_code': tagRow['ref_tag_type_code'],
				'tag_name': getTagName(conn, tagDefCursor, tagRow['pid']),
				'tag_value': tagRow['name'],
				'ref_area_code': cpPropRow['ref_area_code']
			}
			cpTagResDict.append(tmpCPTagResDict)

		# do the real insertion here, comment out for testing
		if len(cpTagDict) != 0:
			counter = counter + len(cpTagDict)
			insertCPTag(conn, cpTagCursor, tuple(cpTagDict))
			insertCPTagResult(conn, cpTagResCursor, tuple(cpTagResDict))


		# 根据tag的name字段在tbl_cp_exprop里面搜索
		cpExPropSQL = "SELECT ref_cp_code, prop_value FROM tbl_cp_exprop WHERE ref_cp_code "\
			" NOT IN (SELECT ref_cp_code FROM tbl_cp_tag) AND prop_value LIKE "\
			" '%" + tagRow['name'] + "%'"

		cpPropCursor.execute(cpExPropSQL)
		cpTagDict = []
		cpTagResDict = []
		for cpPropRow in cpPropCursor:
			if isCPType(conn, cpProp1Cursor, cpPropRow['ref_cp_code'], 'BUSSINESS-NONMAP'):
				tmpDict = {
					'ref_cp_code': cpPropRow['ref_cp_code'],
					'ref_tag_definition_id': tagRow['id'],
					'ref_area_code': getRefAreaCode(conn, cpCursor, cpPropRow['ref_cp_code']),
					'ref_brand_code': getBrandCode(conn, brandCursor, cpPropRow['prop_value'], 'BUSSINESS-NONMAP')
				}

				cpTagDict.append(tmpDict)

				# 根据tagRow里面的pid选出f父节点作为tag_name
				tmpCPTagResDict = {
					'ref_cp_code': cpPropRow['ref_cp_code'],
					'ref_tag_type_code': tagRow['ref_tag_type_code'],
					'tag_name': getTagName(conn, tagDefCursor, tagRow['pid']),
					'tag_value': tagRow['name'],
					'ref_area_code': getRefAreaCode(conn, cpCursor, cpPropRow['ref_cp_code'])
				}
				cpTagResDict.append(tmpCPTagResDict)
			else:
				continue

		if len(cpTagDict) != 0:
			counter = counter + len(cpTagDict)
			insertCPTag(conn, cpTagCursor, tuple(cpTagDict))
			insertCPTagResult(conn, cpTagResCursor, tuple(cpTagResDict))


	conn.close()
	print 'Done', counter

if __name__ == "__main__":
	main()
