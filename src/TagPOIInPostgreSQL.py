#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# File: TagPOIInPostgreSQL.py
# Desc: 为数据库中的采集点数据打标签，原始采集点数据在tbl_cp, tbl_cp_prop,
#       tbl_cp_exprop, 标签数据在tbl_tag_definition, 打标签的结果存放
#       在关系表格tbl_cp_tag中。
#       先从tbl_tag_definition里面获取一条，然后去tbl_cp里面过滤包含当前tag名称
#       的记录，对找到的记录又根据tag id和记录id存入tbl_cp_tag表格中。
#
#	    打标签就是向tbl_cp_tag表格里面存入标签信息ref_tag_definition_id和
#       采集点信息ref_cp_code，以将其关联起来。
#
# Date: 08/Nov/2016
import psycopg2
import psycopg2.extras
import pprint

# insert tbl_cp_tag
# conn connection to the database
# cursor a db table cursor
# data tuple
def insertCPTag(conn, cursor, data):
	cpTagSQL = "INSERT INTO tbl_cp_tag(ref_cp_code, ref_tag_definition_id,"\
		" ref_area_code) VALUES(%(ref_cp_code)s, "\
		"%(ref_tag_definition_id)s, %(ref_area_code)s)"

	cursor.executemany(cpTagSQL, data)
	conn.commit() # commit the operation, or it wont take effect
	print len(data), cursor.statusmessage

	return True

def main():
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
	cpPropCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cpTagCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

	# id, name, pid, node_type, ref_tag_type_code
	tagCursor.execute("SELECT * FROM tbl_tag_definition")
	for tagRow in tagCursor:
		# 根据tag 的name在tbl_cp_prop, tbl_cp里面搜索
		# id, ref_cp_code, ref_cptype_code, point_name, floor_number
		cpPropSQL = "SELECT cp.point_code, cp.ref_area_code FROM "\
			"tbl_cp_prop prop INNER JOIN tbl_cp cp "\
			"ON prop.ref_cp_code = cp.point_code WHERE prop.point_name LIKE "\
			" '%" + tagRow['name'] + "%'"
		cpPropCursor.execute(cpPropSQL)
		cpTagDict = []
		for cpPropRow in cpPropCursor:
			tmpDict = {
				'ref_cp_code': cpPropRow['point_code'],
				'ref_tag_definition_id': tagRow['id'],
				'ref_area_code': cpPropRow['ref_area_code']
			}
			cpTagDict.append(tmpDict)

		insertCPTag(conn, cpTagCursor, tuple(cpTagDict))
		# cpTagSQL = "INSERT INTO tbl_cp_tag(ref_cp_code, ref_tag_definition_id,"\
		# 	" ref_area_code) VALUES(%(ref_cp_code)s, "\
		# 	"%(ref_tag_definition_id)s, %(ref_area_code)s)"
		# cpTagCursor.executemany(cpTagSQL, tuple(cpTagDict))
		# conn.commit() # commit the operation, or it wont take effect
		# print len(cpTagDict), cpTagCursor.statusmessage

	conn.close()
	print 'Done'

if __name__ == "__main__":
	main()
