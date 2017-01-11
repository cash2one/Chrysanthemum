#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# Date: 11/Jan/2017
# Desc: 第一阶段给家居建材的CP-JIAJU-JIANCAI-JIADIAN-SELL CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT
#       根据prop_value app 标签对照表清洗 ../data/jiajujiancaipropvalue.csv
# File: TagJiaJuJianCaiV1.py
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

def getTagId(TagIds, name):
    for tagId in TagIds:
        if tagId[0] == name:
            return tagId[1]

    return None

def isInCpTagRelation(conn, cursor, ref_cp_code, tagId):
    sql = "select * from tbl_cp_tag_relation where ref_cp_code = '" + ref_cp_code + "' and ref_tag_definition_id = " + str(tagId) + " limit 1"
    cursor.execute(sql)
    rtv = cursor.fetchone()

    if rtv is not None:
        return True
    else:
        return False

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
	print len(data), cursor.statusmessage

	return True

def isInLst(Lst, query):
    for l in Lst:
        if query in l:
            return (True, Lst[0])

    return (False, False)

def isInLstReverse(Lst, query):
    for l in Lst:
        if l in query:
            return (True, Lst[0])

    return (False, False)


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

    # 载入对照表
    PropValueTags = []
    with open('../data/jiajujiancaipropvalue.csv') as F:
        for line in F:
            PropValueTags.append(line.strip().split(','))

    # 载入id，tag对照表
    TagIds = []
    tagIdSql = "select id, name from tbl_tag_def where flag = 'JIAJU-PROP-VALUE'"
    tagDefCursor.execute(tagIdSql)
    for tagIdRow in tagDefCursor:
        TagIds.append([tagIdRow['name'], tagIdRow['id']])

    # point_name, CP-JIAJU-JIANCAI-JIADIAN-SELL CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT
    cpPropSql = "select cp.point_code, cp.ref_area_code, p.point_name from tbl_cp cp inner join tbl_cp_prop p on cp.point_code = p.ref_cp_code where (p.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL' or p.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT')"
    cpPropSql = "select p.ref_cp_code, "
    cpPropCursor.execute(cpPropSql)

    for cpPropRow in cpPropCursor:
        if len(cpTagLst) >= 100:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            print "Inserted", len(cpTagLst)
            cpTagLst = []

        for brandTag in BrandTags:
            tmpLst = brandTag[0:1]

            rtvReverse = isInLstReverse(tmpLst, cpPropRow['point_name']) # point_name中包含品牌
            if rtvReverse[0]:
                for tag in brandTag[2:]:
                    if len(tag) == 0:
                        continue

                    tmpDict = {
                        'ref_cp_code': cpPropRow['point_code'],
                        'ref_tag_definition_id': getTagId(TagIds, tag),
                        'ref_area_code': cpPropRow['ref_area_code'],
                        'ref_brand_code': getBrandCode(conn, brandCursor, cpPropRow['point_name'], 'HOSPITAL'),
                        'flag': 'BV3'
                    }
                    cpTagLst.append(tmpDict)
            else:
                rtv = isInLst(tmpLst, cpPropRow['point_name']) # 品牌中包含point_name
                if rtv[0]:
                    for tag in brandTag[2:]:
                        if len(tag) == 0:
                            continue

                        tmpDict = {
                            'ref_cp_code': cpPropRow['point_code'],
                            'ref_tag_definition_id': getTagId(TagIds, tag),
                            'ref_area_code': cpPropRow['ref_area_code'],
                            'ref_brand_code': getBrandCode(conn, brandCursor, cpPropRow['point_name'], 'HOSPITAL'),
                            'flag': 'BV3'
                        }
                        cpTagLst.append(tmpDict)

    if len(cpTagLst) > 0:
        insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
        print "Inserted", len(cpTagLst)
        cpTagLst = []

if __name__ == "__main__":
	main()
