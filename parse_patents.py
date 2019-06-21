#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import sys
import argparse
import sqlite3
from copy import copy
from collections import OrderedDict

# parse input arguments
parser = argparse.ArgumentParser(description='China patent parser.')
parser.add_argument('path', type=str, help='path of file to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
parser.add_argument('--clobber', action='store_true', help='delete database and restart')
parser.add_argument('--output', type=int, default=0, help='print out patents per')
parser.add_argument('--limit', type=int, default=0, help='only parse n patents')
parser.add_argument('--chunk', type=int, default=1000, help='chunk insert size')
args = parser.parse_args()

# announce
print(args.path)

# for later
write = args.db is not None
pper = args.output
limit = args.limit
chunk = args.chunk

# database schema
schema = {
    'patnum': '公开（公告）号', # Patent number
    'pubdate': '公开（公告）日', # Publication date
    'appnum': '申请号', # Application number
    'appdate': '申请日', # Application date
    'title': '名称', # Title
    'ipc1': '主分类号', # IPC code 1
    'ipc2': '分类号', # IPC code 2
    'appname': '申请（专利权）人', # Application name
    'invname': '发明（设计）人', # Inventor name
    'abstract': '摘要', # Abstract
    'claims': '主权项', # Independent claim
    'province': '国省代码', # Province code
    'address': '地址', # Address
    'agency': '专利代理机构', # Patent Agency
    'agent': '代理人', # Patent Agent
    'path': '发布路径', # Data Path
    'pages': '页数', # No. of Pages
    'country': '申请国代码', # Application Country
    'type': '专利类型', # Type of Patent
    'source': '申请来源', # Source
    'sipoclass': '范畴分类' # Classification by SIPO
}
rschema = {v: k for k, v in schema.items()}

# default values
default = OrderedDict([(k, None) for k in schema])

# database setup
if write:
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    if args.clobber:
        cur.execute('drop table if exists patent')
        cur.execute('drop index if exists idx_patnum')
    sig = ', '.join([f'{k} text' for k in schema])
    cur.execute(f'create table if not exists patent ({sig})')
    cur.execute('create unique index if not exists idx_patnum on patent (patnum)')

# storage
pats = []
cmd = 'insert or replace into patent values (%s)' % ','.join(['?' for _ in schema])
def commit_patents():
    cur.executemany(cmd, pats)
    con.commit()
    pats.clear()

# chunking express
n = 0
def add_patent(p):
    global n
    n += 1

    # storage
    if write:
        pats.append(list(p.values()))
        if len(pats) >= chunk:
            commit_patents()

    # output
    if pper > 0:
        if n % pper == 0:
            print(f'pat = {n}')
            for k, v in p.items():
                print(f'{k} = {v}')
            print()

    # break
    if limit > 0:
        if n >= limit:
            return False

    return True

# parse file
n = 0
pat = None
for i, line in enumerate(open(args.path, encoding='gb18030', errors='ignore')):
    # skip empty lines
    line = line.strip()
    if len(line) == 0:
        continue

    # start patent
    if line == '<REC>':
        # store current
        if pat is not None:
            if not add_patent(pat):
                break

        # set defaults
        pat = copy(default)

        # clear buffer
        tag = None
        buf = None

        continue

    # start tag
    ret = re.match('<([^\x00-\x7F][^>]*)>=(.*)', line)
    if ret:
        # store old
        if tag in rschema:
            k = rschema[tag]
            pat[k] = buf

        # start new
        tag, buf = ret.groups()
    else:
        # continue existing
        buf += line

if write:
    # close database
    commit_patents()
    cur.close()
    con.close()
