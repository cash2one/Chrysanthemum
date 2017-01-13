#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# Date: 11/Jan/2017
# Desc: 第一阶段给家电CP-JIADIAN-SELL taging
#       根据prop_value app 标签对照表清洗 ../data/jiadian.csv
# File: TagJiaDianV1.py
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

    # prop_value, CP-JIAJU-JIANCAI-JIADIAN-SELL CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT
    cpPropSql = "select e.prop_value, e.ref_cp_code from tbl_cp_prop p inner join tbl_cp_exprop e on p.ref_cp_code = e.ref_cp_code where (p.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL' or p.ref_cptype_code = 'CP-JIAJU-JIANCAI-JIADIAN-SELL-OUT') and e.ref_exprop_code = 'EXP-CATEGORY'"
    cpPropCursor.execute(cpPropSql)

    for cpPropRow in cpPropCursor:
        if len(cpTagLst) >= 100:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            print "Inserted", len(cpTagLst)
            cpTagLst = []

        for propValueTag in PropValueTags:
            if cpPropRow['prop_value'].replace(',', ':') == propValueTag[0]:
                tmpDict = {
                    'ref_cp_code': cpPropRow['ref_cp_code'],
                    'ref_tag_definition_id': 1,
                    'ref_area_code': None,
                    'ref_brand_code': None,
                    'flag': 'JIAJUV1'
                }
                cpTagLst.append(tmpDict)

    if len(cpTagLst) > 0:
        insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
        print "Inserted", len(cpTagLst)
        cpTagLst = []

if __name__ == "__main__":
	main()
