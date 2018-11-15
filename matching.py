from time import time
from simhash import Cluster
from distance.cdistance import levenshtein

default_dist = lambda s1, s2: levenshtein(s1, s2, normalized=True)

# k-shingles: pairs of adjacent k-length substrings (in order)
def shingle(s, k=2):
    k = min(len(s), k)
    for i in range(len(s) - k + 1):
        yield s[i:i+k]

# k = 8, thresh = 4 works well
def close_pairs(name_dict, preproc=None, nshingle=2, output=1000, **kwargs):
    c = Cluster(**kwargs)

    if preproc is None:
        preproc = lambda s: list(shingle(s, k=nshingle))

    t0 = time()
    for i, (nid, name) in enumerate(name_dict.items()):
        features = preproc(name)
        c.add(features, nid)
        if i % output == 0:
            print(f'{i},{time()-t0}')

    # return results
    ipairs = c.unions
    npairs = [(name_dict[i1], name_dict[i2]) for i1, i2 in ipairs]
    return (ipairs, npairs)

def filter_pairs(pairs, thresh=0.1, dist=default_dist):
    return [(s1, s2) for s1, s2 in pairs if dist(s1, s2) <= thresh]
