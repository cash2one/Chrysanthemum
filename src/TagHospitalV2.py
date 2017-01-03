#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# Date: 30/Dec/2016
# Desc: 第一阶段给医院类型的采集点打标签
#       根据品牌对照表
# File: TagHospitalV1.py
import psycopg2
import psycopg2.extras
import sys

# getTagDef
# 根据名称获取tag定义信息
# conn connection to the database
# cursor a db table cursor
# name 标签名称
def getTagDef(conn, cursor, name):
    sql = "select * from tbl_tag_definition where name = '" + name + "' limit 1"
    cursor.execute(sql)
    rtv = cursor.fetchone()

    if rtv:
        return rtv
    else:
        return None

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

# findAndInsert
# find the matched records and insert it into tbl_cp_tag
# conn connection to the database
# cursor a db table cursor
# point_name
# prop_value
def findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor, brandCursor, tagName, point_name, prop_value = ''):
    cpTagLst = []
    cpTagResLst = []
    tag = getTagDef(conn, tagDefCursor, tagName)  # cursor 混用了
    if len(prop_value) > 0:
        cpPropSql = "select cp.point_code, cp.ref_area_code, p.point_name from tbl_cp cp inner join tbl_cp_prop p on cp.point_code = p.ref_cp_code left join tbl_cp_exprop ex on cp.point_code = ex.ref_cp_code where p.ref_cptype_code like 'CP-HOSPITAL%' and p.point_name like  '%" + point_name + "%' or ex.prop_value like '%" + prop_value + "%'"
        cpPropCursor.execute(cpPropSql)

        for cpPropRow in cpPropCursor:
            if len(cpTagLst) >= 100:
                insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
                insertCPTagResult(conn, cpTagResCursor, tuple(cpTagResLst))
                print tagName, point_name, prop_value, "Inserted", len(cpTagLst)
                cpTagLst = []
                cpTagResLst = []

            tmpDict = {
                'ref_cp_code': cpPropRow['point_code'],
                'ref_tag_definition_id': tag['id'],
                'ref_area_code': cpPropRow['ref_area_code'],
                'ref_brand_code': getBrandCode(conn, brandCursor, cpPropRow['point_name'], 'HOSPITAL')
            }
            cpTagLst.append(tmpDict)
            tmpCPTagResDict = {
                'ref_cp_code': cpPropRow['point_code'],
                'ref_tag_type_code': tag['ref_tag_type_code'],
                'tag_name': getTagName(conn, tagDefCursor, tag['pid']),
                'tag_value': tag['name'],
                'ref_area_code': cpPropRow['ref_area_code']
            }
            cpTagResLst.append(tmpCPTagResDict)
        if len(cpTagLst) > 0:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            insertCPTagResult(conn, cpTagResCursor, tuple(cpTagResLst))
            print tagName, point_name, prop_value, "Inserted", len(cpTagLst)
            cpTagLst = []
            cpTagResLst = []
    else:
        cpPropSql = "select cp.point_code, cp.ref_area_code, p.point_name from tbl_cp cp inner join tbl_cp_prop p on cp.point_code = p.ref_cp_code left join tbl_cp_exprop ex on cp.point_code = ex.ref_cp_code where p.ref_cptype_code like 'CP-HOSPITAL%' and p.point_name like  '%" + point_name + "%'"
        cpPropCursor.execute(cpPropSql)

        for cpPropRow in cpPropCursor:
            if len(cpTagLst) >= 100:
                insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
                insertCPTagResult(conn, cpTagResCursor, tuple(cpTagResLst))
                print tagName, point_name, prop_value, "Inserted", len(cpTagLst)
                cpTagLst = []
                cpTagResLst = []

            tmpDict = {
                'ref_cp_code': cpPropRow['point_code'],
                'ref_tag_definition_id': tag['id'],
                'ref_area_code': cpPropRow['ref_area_code'],
                'ref_brand_code': getBrandCode(conn, brandCursor, cpPropRow['point_name'], 'HOSPITAL')
            }
            cpTagLst.append(tmpDict)
            tmpCPTagResDict = {
                'ref_cp_code': cpPropRow['point_code'],
                'ref_tag_type_code': tag['ref_tag_type_code'],
                'tag_name': getTagName(conn, tagDefCursor, tag['pid']),
                'tag_value': tag['name'],
                'ref_area_code': cpPropRow['ref_area_code']
            }
            cpTagResLst.append(tmpCPTagResDict)
        if len(cpTagLst) > 0:
            insertCPTag(conn, cpTagCursor, tuple(cpTagLst))
            insertCPTagResult(conn, cpTagResCursor, tuple(cpTagResLst))
            print tagName, point_name, prop_value, "Inserted", len(cpTagLst)
            cpTagLst = []
            cpTagResLst = []

def main():
    connStr = "host='127.0.0.1' dbname='jmtool20161229' user='postgres' password='postgres'"
    conn = psycopg2.connect(connStr)

    cpPropCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpExPropCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    tagDefCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    brandCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpTagCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cpTagResCursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # 这里根据翁给出的细化列表，一个一个的处理

    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor, brandCursor, tagName='检验室', point_name='放射科', prop_value='放射科')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor, brandCursor, tagName='检验室', point_name='心电图', prop_value='心电图室')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor, brandCursor, tagName='检验室', point_name='超声科', prop_value='超声科')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor, brandCursor, tagName='检验室', point_name='超生')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor, brandCursor, tagName='检验室', point_name='检验')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='检验室', point_name='检查室')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='检验室', point_name='心超')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='检验室', point_name='彩超')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='检验室', point_name='临检')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='检查化验类', point_name='放疗')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='检查化验类', point_name='化验')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='骨科', point_name='骨')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='骨科', point_name='关节')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='外科', point_name='注射')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='外科', point_name='接种')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='外科', point_name='配液室')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='外科', point_name='输液')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='外科', point_name='侯种')
    # # findAndInsert(conn, cpPropCursor, tagName='外科', point_name='外治科', prop_value='外治科')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='泌尿外科', point_name='膀胱镜')
    # findAndInsert(conn, cpPropCursor, tagDefCursor, brandCursor, tagName='泌尿科', point_name='尿')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='内科', point_name='风湿')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='消化内科', point_name='胃肠')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='消化内科', point_name='消化')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='内分泌科', point_name='治胃病')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='心内科', point_name='心脏')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='呼吸内科', point_name='肺')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='呼吸内科', point_name='呼吸')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='神经内科', point_name='神内')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='妇科', point_name='乳腺')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='妇科', point_name='妇女')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='妇产科', point_name='孕')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='皮肤科', point_name='皮科')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='皮肤科', point_name='皮肤')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='口腔科', point_name='口腔')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='口腔科', point_name='牙片')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='感染科', point_name='院感')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='感染科', point_name='感染')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='传染科', point_name='慢病')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='肝病科', point_name='肝病')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='针灸推拿科', point_name='针灸')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='针灸推拿科', point_name='针')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='针灸推拿科', point_name='推拿')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='中医科', point_name='中医')
    # findAndInsert(conn, cpPropCursor, tagDefCursor, brandCursor, tagName='心理咨询室', point_name='临床心理科')
    # findAndInsert(conn, cpPropCursor, tagDefCursor, brandCursor, tagName='心理咨询室', point_name='精防')
    # findAndInsert(conn, cpPropCursor, tagDefCursor, brandCursor, tagName='心理咨询室', point_name='心理', prop_value='心理')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='血液科', point_name='抽血')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='血液科', point_name='采血')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='美容', point_name='御颜')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='美容', point_name='抗衰老')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='美容', point_name='整容')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='手术室', point_name='手术')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='B超室', point_name='B超')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='血透室', point_name='血液透析')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='重症监护室', point_name='重症')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='CT室', point_name='CT')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='磁共振室', point_name='磁共振')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='药房', point_name='药')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='药房', point_name='药剂科')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='收费窗口', point_name='挂号')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='收费窗口', point_name='收费')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='办公室', point_name='病案室', prop_value='病案室')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='办公室', point_name='职工')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='办公室', point_name='库房')
    findAndInsert(conn, cpPropCursor, tagDefCursor, cpTagCursor, cpTagResCursor,  brandCursor, tagName='办公室', point_name='会议')



if __name__ == "__main__":
	main()
