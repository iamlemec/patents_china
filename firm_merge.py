import argparse
import sqlite3
import pandas as pd
from itertools import chain

# arguments
parser = argparse.ArgumentParser(description='Match firm and patent data.')
parser.add_argument('--input', type=str, help='input firm data')
parser.add_argument('--output', type=str, help='output filename')
parser.add_argument('--idcol', type=str, default='id', help='id column name')
parser.add_argument('--namecol', type=str, default='name', help='name column name')
args = parser.parse_args()

# load tax data
tax_df = pd.read_csv(args.input, usecols=[args.idcol, args.namecol])
tax_df = tax_df.rename(columns={args.idcol: 'id', args.namecol: 'name'})
tax_df = tax_df.drop_duplicates(subset='id')

# load patent data
with sqlite3.connect('/media/Liquid/work/cleancomp/code/store/patents_cn.db') as con:
    pat_df = pd.read_sql('select patnum,appname,appdate from patent', con)

# split names
pat1_df = pd.DataFrame(list(chain(*[[(s['patnum'], s['appdate'], x) for x in s['appname'].split(';')] for _, s in pat_df.iterrows()])), columns=['patnum', 'appdate', 'appname'])

# merge datasets
merged = pd.merge(pat1_df[['patnum', 'appdate', 'appname']], tax_df[['id', 'name']], left_on='appname', right_on='name', how='left')[['patnum', 'appdate', 'id']]

merged.to_csv(args.output, index=False)
