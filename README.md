# Patents China

To parse everything:

```
ls -1 data/*.trs | xargs -n 1 python3 parse_patents.py --db store/patents.db --output 100000
```
