# generate similarity metrics

import torch
import pandas as pd
from ziggy import TorchVectorIndex
from ziggy.utils import batch_indices

# demean and renormalize vectors
def demean_inplace(x):
    x -= x.mean(dim=0)[None,:]
    x /= x.square().sum(dim=1)[:,None]

# load ziggy TorchVectorIndex directly or from TextDatabase
def load_database(path):
    data = torch.load(path)
    if 'index' in data:
        data = data['index']
    return TorchVectorIndex.load(data)

# merge patent metadata with ziggy database
def merge_patents(
    path_vecs, # ziggy database
    path_meta, # metadata csv
    path_pats, # output csv
):
    # load vector index
    print('Loading vector index')
    index = load_database(path_vecs)

    # load metadata csv
    print('Loading patent metadata')
    meta = pd.read_csv(path_meta, usecols=['patnum', 'appdate'])
    meta = meta.drop_duplicates('patnum').set_index('patnum')
    meta['appdate'] = pd.to_datetime(meta['appdate'])

    # merge with index (due to dups)
    pats = pd.DataFrame({'patnum': index.labels})
    pats = pats.join(meta, on='patnum')

    # save ordered patent data
    pats.to_csv(path_pats, index=False)

def similarity_topk(
    path_vecs, # ziggy database
    path_pats, # patent metadata csv (for comparison!)
    path_sims, # output torch file
    path_vecs1=None, # comparison ziggy database
    topk=100, batch_size=256, max_rows=None, demean=False
):
    # load vector index
    print('Loading vector index')
    index = load_database(path_vecs)
    n_pats = len(index)

    # load comparison vector index
    if path_vecs1 is not None:
        print('Loading comparison vector index')
        index1 = load_database(path_vecs1)

    # demean vectors is requested
    if demean:
        demean_inplace(index.values.data)
        if index1 is not index:
            demean_inplace(index1.values.data)

    # limit rows if requested
    if max_rows is not None:
        n_pats = min(n_pats, max_rows)

    # load merged patent data
    pats = pd.read_csv(path_pats, nrows=max_rows)

    # convert date to days since unix epoch
    epoch = pd.to_datetime('1970-01-01')
    dates = pats.set_index('patnum')['appdate']
    days = torch.tensor((dates-epoch).dt.days.to_numpy(), device='cuda')

    # create output tensors
    idxt = torch.zeros((n_pats, topk), dtype=torch.int32, device='cuda')
    simt = torch.zeros((n_pats, topk), dtype=torch.float16, device='cuda')

    # generate similarity metrics
    for i1, i2 in batch_indices(n_pats, batch_size):
        print(f'{i1} → {i2}')

        # compute similarities for batch
        vecs = index.values.data[i1:i2] # [B, D]
        sims = index.similarity(vecs) # [B, N1]

        # compute top sims for before
        before = days[None, :] < days[i1:i2, None]
        simb = torch.where(before, sims, -torch.inf)
        topb = simb.topk(topk, dim=1)

        # store in output tensors
        idxt[i1:i2] = topb.indices
        simt[i1:i2] = topb.values

    # save to disk
    torch.save({
        'top_idx': idxt,
        'top_sim': simt,
    }, path_sims)

def similarity_mean(
    path_vecs, # ziggy database
    path_pats, # patent metadata csv (for comparison!)
    path_sims, # output torch file
    path_vecs1=None, # comparison ziggy database
    batch_size=64, max_rows=None, demean=False,
):
    # load vector index
    print('Loading base vector index')
    index = load_database(path_vecs)
    n_pats = len(index)

    # load comparison vector index
    if path_vecs1 is not None:
        print('Loading comparison vector index')
        index1 = load_database(path_vecs1)
    else:
        index1 = index

    # demean vectors is requested
    if demean:
        demean_inplace(index.values.data)
        if index1 is not index:
            demean_inplace(index1.values.data)

    # limit rows if requested
    if max_rows is not None:
        n_pats = min(n_pats, max_rows)

    # load merged patent data
    pats = pd.read_csv(path_pats)
    pats['appdate'] = pd.to_datetime(pats['appdate'])

    # get application year for patents
    app_year = torch.tensor(pats['appdate'].dt.year, dtype=torch.int32, device='cuda')
    year_min, year_max = app_year.min(), app_year.max()
    year_idx = app_year - year_min

    # get application year statistics
    n_years = year_max - year_min + 1
    c_years = torch.bincount(year_idx, minlength=n_years)

    # create output tensors
    avgt = torch.zeros((n_pats, n_years), dtype=torch.float16, device='cuda')

    # generate similarity metrics
    for i1, i2 in batch_indices(n_pats, batch_size):
        print(f'{i1} → {i2}')
        n_batch = i2 - i1

        # compute similarities for batch
        vecs = index.values.data[i1:i2] # [B, D]
        sims = index1.similarity(vecs) # [B, N1]

        # generate offsets
        batch_vec = torch.arange(n_batch, device='cuda')
        offsets = batch_vec[:,None] * n_years + year_idx[None,:]

        # group sum by application year
        sums = torch.bincount(offsets.ravel(), weights=sims.ravel(), minlength=n_batch*n_years)
        avgt[i1:i2] = sums.reshape(n_batch, n_years) / c_years[None,:]

    # save to disk
    torch.save({
        'year_sim'  : avgt    ,
        'year_count': c_years ,
        'year_min'  : year_min,
        'year_max'  : year_max,
    }, path_sims)
