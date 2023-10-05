#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import sys
import argparse
import pandas as pd
from collections import defaultdict
from itertools import islice

# parse input arguments
parser = argparse.ArgumentParser(description='China patent parser.')
parser.add_argument('inpath', type=str, help='TRS file to parse')
parser.add_argument('--outdir', type=str, default=None, help='directory to store to')
parser.add_argument('--clobber', action='store_true', help='delete database and restart')
parser.add_argument('--output', action='store_true', help='print out patents per')
parser.add_argument('--chunk', type=int, default=100_000, help='chunk size')
parser.add_argument('--limit', type=int, default=None, help='only parse n patents')
args = parser.parse_args()

# announce
if not args.clobber and os.path.exists(args.outpath):
    print(f'Skipping: {args.inpath}')
    sys.exit(0)
else:
    print(f'Parsing: {args.inpath}')

# construct output path
if args.outdir is not None:
    filename = os.path.basename(args.inpath)
    basename, _ = os.path.splitext(filename)
    outpath = os.path.join(args.outdir, f'{basename}.csv')
else:
    outpath = None

# database schema
trans = {
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
    'sipoclass': '范畴分类', # Classification by SIPO
}
rtrans = {v: k for k, v in trans.items()}

# parse file
def patent_generator(fid):
    pat = None
    for line in fid:
        # skip empty lines
        line = line.strip()
        if len(line) == 0:
            continue

        # start patent
        if line == '<REC>':
            # store current
            if pat is not None:
                yield pat

            # set defaults
            pat = defaultdict(None)

            # clear buffer
            tag = None
            buf = None

            continue

        # start tag
        ret = re.match('<([^\x00-\x7F][^>]*)>=(.*)', line)
        if ret:
            # store old
            if tag in rtrans:
                k = rtrans[tag]
                pat[k] = buf

            # start new
            tag, buf = ret.groups()
        else:
            # continue existing
            buf += line

# open and parse
with open(args.inpath, encoding='gb18030', errors='ignore') as fid:
    # initial state
    tot = 0
    gen = patent_generator(fid)

    while True:
        # get up to chunk
        batch = islice(gen, args.chunk)
        frame = pd.DataFrame(batch, dtype=str, columns=trans)

        # break if empty
        if len(frame) == 0:
            break

        # save to csv
        if outpath is not None:
            if tot == 0:
                frame.to_csv(outpath, index=False, header=True)
            else:
                frame.to_csv(outpath, index=False, mode='a', header=False)

        # update counter
        tot += len(frame)

        # output stats
        if args.output:
            print(f'tot = {tot}')

        # break if limit
        if args.limit is not None and tot >= args.limit:
            break
