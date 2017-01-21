#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# Date: 12/Jan/2017
# Desc: 第二阶段给家居建材的CP-JIAJU-JIANCAI-JIADIAN-SELL CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT
#       在阶段一二清洗的基础上，根据采集点point_name和标签系统匹配情况来处理。
# File: TagJiaJuJianCaiV3.py
import psycopg2
import psycopg2.extras
import sys

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
    if len(query) == 0:
        return False

    for l in Lst:
        if len(l) == 0:
            continue

        if query in l:
            # print 'isInLst', query, l
            return True

    return False

def isInLstReverse(Lst, query):
    if len(query) == 0:
        return False

    for l in Lst:
        if len(l) == 0:
            continue

        if l in query:
            # print 'isInLstReverse', query, l
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

    # load tags
    TagDefs = []
    sql = "select id, descriptions from tbl_tag_def where flag = 'JIAJU-POINT-NAME'"
    tagDefCursor.execute(sql)
    for tag in tagDefCursor:
        TagDefs.append(tag['descriptions'].split(','))

    cpTagLst = []
    count = 0

    # point_name, CP-JIAJU-JIANCAI-JIADIAN-SELL CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT, 且审核通过的
    cpPropSql = "select cp.ref_cp_code, cp.point_name from tbl_cp p inner join tbl_cp_prop cp on p.point_code = cp.ref_cp_code inner join tbl_user_task_brief b on b.ref_area_code = p.ref_area_code where (cp.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL' or cp.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT') and cp.ref_cp_code not in (select ref_cp_code from tbl_cp_tag_relation where flag = 'JIAJUV1' or flag = 'JIAJUV2') and (b.status_result = 1045 or b.status_result = 106 or b.status_result = 107)"
    cpPropCursor.execute(cpPropSql)

    for cpPropRow in cpPropCursor:
        if len(cpTagLst) >= 100:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            print "Inserted", len(cpTagLst)
            count = count + len(cpTagLst)
            cpTagLst = []

        for tagDef in TagDefs:
            if isInLstReverse(tagDef, cpPropRow['point_name']):
                tmpDict = {
                    'ref_cp_code': cpPropRow['ref_cp_code'],
                    'ref_tag_definition_id': 1,
                    'ref_area_code': None,
                    'ref_brand_code': None,
                    'flag': 'JIAJUV3'
                }
                cpTagLst.append(tmpDict)
                break

    if len(cpTagLst) > 0:
        insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
        print "Inserted", len(cpTagLst)
        count = count + len(cpTagLst)
        cpTagLst = []

    print count

if __name__ == "__main__":
	main()
