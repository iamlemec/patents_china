# Patents China

To parse everything:

```
ls -1 data/raw/*.trs | xargs -n 1 python3 parse_patents.py --outdir data/parsed
```

To combine into one file:

```
ls -1 data/parsed/*.csv | head -n 1 | xargs -I {} sh -c 'head -n 1 {} > data/tables/patents.csv'
ls -1 data/parsed/*.csv | xargs -I {} sh -c 'tail -n +2 {} >> data/tables/patents.csv'
```
