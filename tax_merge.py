import json
import numpy as np
import pandas as pd
from glob import glob

cols_basic0 = {
	'year': 'year',
	'id': 'firmid',
	'UBI_1': 'taxid',
	'UBI_3': 'loccode',
	'UBI_5_Y': 'startyear',
	'UBI_5_M': 'startmonth',
	'UBI_7_b': 'industry_b',
	'UBI_8': 'regtype'
}

cols_basic1 = {
	'year': 'year',
	'id': 'firmid',
	'UBI_1': 'taxid',
	'UBI_3': 'loccode',
	'UBI_5_Y': 'startyear',
	'UBI_5_M': 'startmonth',
	'UBI_7_a': 'industry_a',
	'UBI_8': 'regtype'
}

cols_goods = {
	'year': 'year',
	'id': 'firmid',
	'UGS_0_1': 'code',
	'UGS_42': 'sales_net',
	'UGS_43': 'cost_oper',
	'UGS_8': 'sales_va',
	'UGS_9': 'sales_nova',
	'UGS_10': 'sales_exp'
}

cols_taxes0 = {
	'year': 'year',
	'id': 'firmid',
	'UTF_M_14': 'ee',
	'UTF_M_25': 'profit_total_1',
	'UTF_L_13': 'profit_total_2',
	'UTF_M_27': 'profit_net',
	'UTF_M_2': 'income_main',
	'UTF_M_3': 'income_other',
	'UTF_L_1': 'income_oper',
	'UTF_Q_13': 'employees',
	'UTF_Q_14': 'output_value',
	'UTF_N_1': 'asset_start',
	'UTF_N_9': 'asset_end'
}

cols_taxes1 = {
	'year': 'year',
	'id': 'firmid',
	'UTF_M_14': 'ee',
	'UTF_M_25': 'profit_total_1',
	'UTF_L_13': 'profit_total_2',
	'UTF_M_27': 'profit_net',
	'UTF_M_2': 'income_main',
	'UTF_M_3': 'income_other',
	'UTF_L_1': 'income_oper',
	'UTF_Q_11': 'employees_start',
	'UTF_Q_12': 'employees_end',
	'UTF_Q_14': 'output_value',
	'UTF_N_1': 'asset_start',
	'UTF_N_9': 'asset_end'
}

industry = json.load(open('industry_map.json'))
location = json.load(open('location_map.json'))

##
## load data
##

dtype = {'id': str, 'loccode': np.float}
sconv = lambda s: int(s) if s.isdigit() else np.nan
def load_year(fn, cols):
	df = pd.read_csv(fn, dtype=dtype, usecols=cols).rename(cols, axis=1)
	df['year'] = df['year'].apply(lambda s: s[:4]).apply(sconv)
	df = df.dropna(subset=['firmid', 'year'])
	df['year'] = df['year'].astype(np.int)
	if 'loccode' in df:
		df = df.dropna(subset=['loccode'])
		df['loccode'] = df['loccode'].astype(np.int).astype(str)
	return df

# basic info
print('loading basic info')
basic0 = pd.concat([load_year(f'original/Basic_Information-{yr}.txt', cols_basic0) for yr in [2007, 2008, 2009, 2010]], sort=True)
basic1 = pd.concat([load_year(f'original/Basic_Information-{yr}.txt', cols_basic1) for yr in [2011, 2012, 2013, 2014, 2015]], sort=True)
basic = pd.concat([basic0, basic1], sort=True)

# goods info
print('loading goods and services info')
goods = pd.concat([load_year(f'original/Goods_Service-{yr}.txt', cols_goods) for yr in [2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015]], sort=True)
goods = goods[goods['code']==0]

# tax info
print('loading tax and finance info')
taxes0 = pd.concat([load_year(f'original/Taxation_Finance-{yr}.txt', cols_taxes0) for yr in [2007, 2008, 2009]], sort=True)
taxes1 = pd.concat([load_year(f'original/Taxation_Finance-{yr}.txt', cols_taxes1) for yr in [2010, 2011, 2012, 2013, 2014, 2015]], sort=True)
taxes = pd.concat([taxes0, taxes1], sort=True)

##
## fixes
##

# location fix
basic['loccode'] = basic['loccode'].where(basic['year']>2011, basic['loccode'].replace(location))

# industry fix
ind0 = lambda s: s[1:] if type(s) is str else np.nan
basic['industry_a'] = basic['industry_a'].apply(ind0)
basic['industry_b'] = basic['industry_b'].apply(ind0)
basic['industry'] = basic['industry_a'].fillna(basic['industry_b'].replace(industry))

# employee fix
taxes['employees'].fillna(0.5*(taxes['employees_start']+taxes['employees_end']))

##
## merge
##

print('merging datasets')

index = ['firmid', 'year']
conform = lambda df: df.dropna(subset=index).drop_duplicates(subset=index).set_index()
firms = pd.concat([conform(df) for df in [basic, goods, taxes]], axis=1).reset_index()

##
## columns
##

# total sales
firms['sales'] = firms['sales_va'] + firms['sales_nova'] + firms['sales_exp']

##
## selections
##

firms = firms[firms['loccode']!='0']
firms = firms.dropna(subset=['ee', 'sales', 'sales_net', 'income_main'])
firms = firms[firms['employees']>0]
for col in ['ee', 'sales_net', 'income_main', 'cost_oper', 'asset_start', 'asset_end']:
	firms = firms[firms[col]>=0]
