# generate similarity metrics

import torch
import pandas as pd
from ziggy import TorchVectorIndex
from ziggy.utils import batch_indices

def merge_patents(
    path_vecs='data/tables/patents.torch',
    path_meta='data/tables/patents.csv',
    path_pats='data/simil/pats.csv'
):
    # load vector index
    print('Loading vector index')
    data = torch.load(path_vecs)
    index = TorchVectorIndex.load(data)

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
    path_vecs='data/tables/patents.torch',
    path_pats='data/simil/pats.csv',
    path_sims='data/simil/topk.torch',
    topk=100, batch_size=256, max_rows=None, demean=False
):
    # load vector index
    print('Loading vector index')
    data = torch.load(path_vecs)
    index = TorchVectorIndex.load(data)
    n_pats = len(index)

    # demean vectors is requested
    if demean:
        index.values.data -= index.values.data.mean(dim=0)[None,:]
        index.values.data /= index.values.data.square().sum(dim=1)[:,None]

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
        vecs = index.values.data[i1:i2]
        sims = index.similarity(vecs)

        # compute top sims for before
        before = days[None, :] < days[i1:i2, None]
        simb = torch.where(before, sims, -torch.inf)
        topb = simb.topk(topk, dim=1)

        # store in output tensors
        idxt[i1:i2] = topb.indices
        simt[i1:i2] = topb.values

    # save to disk
    torch.save({'top_idx': idxt, 'top_sim': simt}, path_sims)

def similarity_mean(
    path_vecs='data/tables/patents.torch',
    path_pats='data/simil/pats.csv',
    path_sims='data/simil/mean.torch',
    batch_size=64, max_rows=None, demean=False,
):
    # load vector index
    print('Loading vector index')
    data = torch.load(path_vecs)
    index = TorchVectorIndex.load(data)
    n_pats = len(index)

    # demean vectors is requested
    if demean:
        index.values.data -= index.values.data.mean(dim=0)[None,:]
        index.values.data /= index.values.data.square().sum(dim=1)[:,None]

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
        vecs = index.values.data[i1:i2]
        sims = index.similarity(vecs)

        # generate offsets
        batch_vec = torch.arange(n_batch, device='cuda')
        offsets = batch_vec[:,None] * n_years + year_idx[None,:]

        # group sum by application year
        sums = torch.bincount(offsets.ravel(), weights=sims.ravel(), minlength=n_batch*n_years)
        avgt[i1:i2] = sums.reshape(n_batch, n_years) / c_years[None,:]

    # save to disk
    torch.save({
        'year_sim': avgt, 'year_count': c_years,
        'year_min': year_min, 'year_max': year_max,
    }, path_sims)
