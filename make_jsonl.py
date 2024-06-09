import json
import argparse
import pandas as pd

# parse input arguments
parser = argparse.ArgumentParser(description='China patent parser.')
parser.add_argument('inpath', type=str, help='csv file to parse')
parser.add_argument('outpath', type=str, help='jsonl file to store to')
parser.add_argument('--chunk', type=int, default=8192, help='chunk size')
parser.add_argument('--limit', type=int, default=None, help='only parse n patents')
args = parser.parse_args()

# write to jsonl
with open(args.outpath, 'w') as fid:
    for batch in pd.read_csv(
        args.inpath, chunksize=args.chunk, nrows=args.limit,
        usecols=['patnum', 'title', 'claims', 'abstract']
    ):
        for _, row in batch.iterrows():
            dat = {
                'patnum': row['patnum'],
                'text': f"{row['title']}\n{row['claims']}\n{row['abstract']}",
            }
            json.dump(dat, fid, ensure_ascii=False)
            fid.write('\n')

# load with ziggy (4 hours on A6000)
# emb = ziggy.LlamaCppEmbedding('bge-small-zh-v1.5-f16.gguf')
# db = ziggy.DocumentDatabase.from_jsonl('patents.jsonl', embed=emb, name_col='patnum', qspec=ziggy.quant.Half)
# data = db.dindex.save()
# torch.save(data, 'patents.torch')

