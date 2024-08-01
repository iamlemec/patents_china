import json
import pandas as pd
from glob import glob

# globals
columns_utility = ['app_no', 'app_date', 'title', 'claims', 'abstract']
columns_invention = ['appnum', 'appdate', 'title', 'claims', 'abstract']
columns_output = ['appnum', 'appdate', 'title', 'claims', 'abstract']

rename_utility = {'app_no': 'appnum', 'app_date': 'appdate'}
rename_invention = {}

# combine utility CSV files
def merge_patents(indir, outpath, mode):
    if mode == 'utility':
        columns = columns_utility
        rename = rename_utility
    elif mode == 'invention':
        columns = columns_invention
        rename = rename_invention

    with open(outpath, 'w') as fid:
        fid.write(','.join(columns_output)+'\n')
        for path in sorted(glob(f'{indir}/*.csv')):
            print(path)
            data = pd.read_csv(path, usecols=columns, encoding_errors='ignore')
            data = data.rename(rename, axis=1)[columns_output]
            data['appdate'] = pd.to_datetime(data['appdate'])
            data.to_csv(fid, header=False, index=False)

# write to json
def make_jsonl(inpath, outpath, chunk=8192, limit=None):
    with open(outpath, 'w') as fid:
        for batch in pd.read_csv(inpath, chunksize=chunk, nrows=limit):
            for _, row in batch.iterrows():
                dat = {
                    'appnum': row['appnum'],
                    'text': f"{row['title']}\n{row['claims']}\n{row['abstract']}",
                }
                json.dump(dat, fid, ensure_ascii=False)
                fid.write('\n')

# load with ziggy (4 hours on A6000)
# emb = ziggy.LlamaCppEmbedding('bge-small-zh-v1.5-f16.gguf')
# db = ziggy.DocumentDatabase.from_jsonl('patents.jsonl', embed=emb, name_col='patnum', qspec=ziggy.quant.Half)
# data = db.dindex.save()
# torch.save(data, 'patents.torch')

