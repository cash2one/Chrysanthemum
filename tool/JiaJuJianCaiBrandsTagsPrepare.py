#!/usr/bin/env python
# coding=utf-8
#
# Author: Archer
# File: JiaJuJianCaiBrandsTagsPrepare.py
# Desc: 根据网站上的品牌整理品牌到标签的对应关系， 需要你提供网站上抓下来的品牌名字
#       可能包含empty line。
# Date: 13/Feb/2017
import sys

if len(sys.argv) != 4:
    print 'Usage: python JiaJuJianCaiBrandsTagsPrepare.py 品类 类别 input.txt'
    sys.exit(1)

PinLei = sys.argv[1]
Leibie = sys.argv[2]
Input = sys.argv[3]

Brands = []
with open(Input, 'r') as F:
    for line in F:
        if len(line.strip()) == 0:
            continue
        else:
            Brands.append(line.strip())

# for brand in Brands:
#     print brand

print PinLei + '#' + Leibie + ':' + ','.join(Brands)
