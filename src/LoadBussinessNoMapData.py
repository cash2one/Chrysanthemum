#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer Reilly
# File: LoadBussinessNoMapData.py
# Date: 19/Dec/2016
# Desc: 将定义的json格式的商场标签数据载入到tbl_tag_definition表格里面去，
#       注意标签之间的层级关系.
#
import psycopg2
import psycopg2.extras
import sys
import json

# getRootId
# 获取标签系统根标签的id
# cursor
#
# id or False
def getRootId(cursor):
    sql = "select id from tbl_tag_definition order by id limit 1"
    cursor.execute(sql)
    rtv = cursor.fetchone()
    if rtv:
        return rtv['id']
    else:
        return False

# insertTagDefinition
# 插入一条tag到表格中
# conn
# cursor
#
# id
def insertTagDefinition(conn, cursor, data):
    sql = "insert into tbl_tag_definition (name, pid, node_type) values(%s, %s, %s) RETURNING id"
    cursor.execute(sql, data)
    conn.commit()
    id = cursor.fetchone()[0]

    return id

# delTagDefinition
# 根据id删除一个标签定义
# conn
# cursor
#
# boolean
def delTagDefinition(conn, cursor, id):
    sql = "delete from tbl_tag_definition where id = " + str(id)
    cursor.execute(sql)
    conn.commit()

    return True

def main():
    with open('../data/TagBussinessNoMap.json') as jsonData:
        TagDefinition = json.load(jsonData)

	#Define our connection string
	conn_string = "host='192.168.0.21' dbname='jmtool3_1206_i'"\
		+ " user='postgres' password='postgres'"

	# print the connection string we will use to connect
	print "Connecting to database\n	->%s" % (conn_string)

	# get a connection, if a connect cannot be made an exception
	# will be raised here
	conn = psycopg2.connect(conn_string)
	print "Connected!\n"

    tagDefCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # first, get root tag id
    rootId = getRootId(tagDefCursor)
    if not rootId:
        print 'Cant get root tag id'
        sys.exit(1)

    # 针对树形结构的json数据，逐步载入数据到tbl_tag_definition
    # 1st, for industry
    data = (TagDefinition['name'], rootId, TagDefinition['type'])
    id = insertTagDefinition(conn, tagDefCursor, data)
    if id:
        print TagDefinition['name']
        # print 'inserted', id
        # delTagDefinition(conn, tagDefCursor, id)
        # print 'deleted', id
    else:
        print 'not inserted', data
        sys.exit(1)

    # 2nd, 品类
    data = (TagDefinition['data']['name'], id, TagDefinition['data']['type'])
    id = insertTagDefinition(conn, tagDefCursor, data)
    if id:
        print '\t' + TagDefinition['data']['name']
        # print 'inserted', id
        # delTagDefinition(conn, tagDefCursor, id)
        # print 'deleted', id
    else:
        print 'not inserted', data
        sys.exit(1)

    # 3rd, 对与每一个品类
    for pinlei in TagDefinition['data']['data']:
        data = (pinlei['name'], id, pinlei['type'])
        pinleiId = insertTagDefinition(conn, tagDefCursor, data)
        if pinleiId:
            print '\t\t' + pinlei['name']
            # print 'inserted', pinleiId
            # delTagDefinition(conn, tagDefCursor, pinleiId)
            # print 'deleted', pinleiId
        else:
            print 'not inserted', data
            sys.exit(1)

        # 4th, 对每个品类下的类别风格档次等
        for category in pinlei['data']:
            data = (category['name'], pinleiId, category['type'])
            categoryId = insertTagDefinition(conn, tagDefCursor, data)
            if categoryId:
                print '\t\t\t' + category['name']
                # print 'inserted', categoryId
                # delTagDefinition(conn, tagDefCursor, categoryId)
                # print 'deleted', categoryId

            else:
                print 'not inserted', data
                sys.exit(1)

            # 5th, 对于每一个类别的具体分类
            for row in category['data']:
                data = (row['name'], categoryId, row['type'])
                rowId = insertTagDefinition(conn, tagDefCursor, data)
                if rowId:
                    print '\t\t\t\t' + row['name']
                    # print 'inserted', rowId
                    # delTagDefinition(conn, tagDefCursor, rowId)
                    # print 'deleted', rowId
                else:
                    print 'not inserted', data
                    sys.exit(1)



if __name__ == "__main__":
	main()
