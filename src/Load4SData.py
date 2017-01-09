#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer Reilly
# File: Load4SData.py
# Date: 09/Jan/2017
# Desc: 将定义的json格式的商场标签数据载入到tbl_tag_def表格里面去，
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
    sql = "insert into tbl_tag_def (id, name, descriptions, flag) values(%s, %s, %s, %s)"
    cursor.execute(sql, data)
    conn.commit()
    return

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
    Tags = []
    with open('../data/4stags.csv') as F:
        for line in F:
            Tags.append(line.strip())


	#Define our connection string
    connStr = "host='127.0.0.1' dbname='jmtool20161229' user='postgres' password='postgres'"
    conn = psycopg2.connect(connStr)

    tagDefCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    idx = 1
    for tag in Tags:
        data = (idx, tag, tag, '4S')
        insertTagDefinition(conn, tagDefCursor, data)
        idx = idx + 1



    # # 针对树形结构的json数据，逐步载入数据到tbl_tag_def
    # # 1st, for industry
    # data = (TagDefinition['name'], rootId, TagDefinition['type'], 'TTC_CLASS', '4S')
    # id = insertTagDefinition(conn, tagDefCursor, data)
    # if id:
    #     print TagDefinition['name']
    #     # print 'inserted', id
    #     # delTagDefinition(conn, tagDefCursor, id)
    #     # print 'deleted', id
    # else:
    #     print 'not inserted', data
    #     sys.exit(1)
    #
    # # 2nd, 品类
    # data = (TagDefinition['data']['name'], id, TagDefinition['data']['type'], 'TTC_CLASS')
    # id = insertTagDefinition(conn, tagDefCursor, data)
    # if id:
    #     print '\t' + TagDefinition['data']['name']
    #     # print 'inserted', id
    #     # delTagDefinition(conn, tagDefCursor, id)
    #     # print 'deleted', id
    # else:
    #     print 'not inserted', data
    #     sys.exit(1)
    #
    # # 3rd, 对与每一个品类
    # for pinlei in TagDefinition['data']['data']:
    #     data = (pinlei['name'], id, pinlei['type'], 'TTC_CLASS')
    #     pinleiId = insertTagDefinition(conn, tagDefCursor, data)
    #     if pinleiId:
    #         print '\t\t' + pinlei['name']
    #         # print 'inserted', pinleiId
    #         # delTagDefinition(conn, tagDefCursor, pinleiId)
    #         # print 'deleted', pinleiId
    #     else:
    #         print 'not inserted', data
    #         sys.exit(1)
    #
    #     # 4th, 对每个品类下的类别风格档次等
    #     for category in pinlei['data']:
    #         data = (category['name'], pinleiId, category['type'], 'TTC_CLASS')
    #         categoryId = insertTagDefinition(conn, tagDefCursor, data)
    #         if categoryId:
    #             print '\t\t\t' + category['name']
    #             # print 'inserted', categoryId
    #             # delTagDefinition(conn, tagDefCursor, categoryId)
    #             # print 'deleted', categoryId
    #
    #         else:
    #             print 'not inserted', data
    #             sys.exit(1)
    #
    #         # 5th, 对于每一个类别的具体分类
    #         for row in category['data']:
    #             data = (row['name'], categoryId, row['type'], 'TTC_CLASS')
    #             rowId = insertTagDefinition(conn, tagDefCursor, data)
    #             if rowId:
    #                 print '\t\t\t\t' + row['name']
    #                 # print 'inserted', rowId
    #                 # delTagDefinition(conn, tagDefCursor, rowId)
    #                 # print 'deleted', rowId
    #             else:
    #                 print 'not inserted', data
    #                 sys.exit(1)



if __name__ == "__main__":
	main()
