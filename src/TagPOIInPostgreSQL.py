#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# File: TagPOIInPostgreSQL.py
# Desc: 为数据库中的采集点数据打标签，原始采集点数据在tbl_cp, tbl_cp_prop,
#       tbl_cp_exprop, 标签数据在tbl_tag_definition, 打标签的结果存放
#       在关系表格tbl_cp_tag中。
# Date: 08/Nov/2016
import psycopg2
import psycopg2.extras
import pprint

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
	cursor = conn.cursor('cursor_unique_name', \
		cursor_factory=psycopg2.extras.DictCursor)

	cursor.execute("SELECT * FROM tbl_cp")

	# id, ref_area_code, ref_floor_code, point_index, point_code,
	# create_time, submit_times, valid_flag, insert_time
	for row in cursor:
		print row['id'], row['ref_area_code'], row['ref_floor_code'],\
			row['point_index'], row['point_index'], row['create_time'],\
			row['submit_times'], row['valid_flag'], row['insert_time']

if __name__ == "__main__":
	main()
