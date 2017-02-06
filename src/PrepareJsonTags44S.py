#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# File: PrepareJsonTags44S.py
# Desc: 4s店的标签对照表有700行，不能手动整理，必须自动整理出json格式的层级关系
# Date: 06/Feb/2017
import sys
import json
import pprint

pp = pprint.PrettyPrinter(indent=2)

# 载入原始的4s的标签对照表
OriginalData = []
with open('../data/original4s.txt') as F:
    for line in F:
        OriginalData.append(line.strip().split(','))

# 五层循环构建层级嵌套的json
jsonDict = {
    "type": "PROP_VALUE",
    "name": "4S"
}

# 车类
jsonDict['data'] = {
    "type": "PROP_NAME",
    "name": "车类",
    "data": []
}
for row in OriginalData[1:]:
    foundFlag = False
    for data in jsonDict['data']['data']:
        if data['name'] == row[0]:
            foundFlag = True
            break

    if not foundFlag:
        tmpDict = {
            "type": "PROP_VALUE",
            "name": row[0],
            "data": []
        }
        jsonDict['data']['data'].append(tmpDict)

#车型
for row in OriginalData[1:]:
    for data in jsonDict['data']['data']:
        if data['name'] == row[0]:
            cheXi = {
                "type": "PROP_VALUE",
                "name": row[3]
            }
            dangCi = {
                "type": "PROP_VALUE",
                "name": row[4]
            }
            chanDi = {
                "type": "PROP_VALUE",
                "name": row[5]
            }
            tmpDict = {
                "type": "PROP_VALUE",
                "name": row[1],
                "data": [cheXi, dangCi, chanDi]
            }
            data['data'].append(tmpDict)
            break

# with open('tmp.txt', 'w') as F:
#     F.write(json.dumps(jsonDict, ensure_ascii=False))
print json.dumps(jsonDict, ensure_ascii=False)
# print jsonDict
