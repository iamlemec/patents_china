# general tools

import os
import numpy as np
import pandas as pd

# just str and int for now
def astype(data, dtype):
    if dtype == 'str':
        return pd.Series(data, dtype='str')
    elif dtype == 'int':
        return pd.to_numeric(pd.Series(data), errors='coerce').astype('Int64')
    else:
        raise Exception(f'Unsupported type: {dtype}')

# insert in chunks
class ChunkWriter:
    def __init__(self, path, schema, chunk_size=1000, output=False):
        self.path = path
        self.schema = schema
        self.chunk_size = chunk_size
        self.output = output
        self.items = []
        self.i = 0
        self.j = 0

        self.file = open(self.path, 'w+')
        header = ','.join(schema)
        self.file.write(f'{header}\n')

    def __del__(self):
        self.file.close()

    def insert(self, *args):
        self.items.append(args)
        if len(self.items) >= self.chunk_size:
            self.commit()
            return True
        else:
            return False

    def insertmany(self, args):
        self.items += args
        if len(self.items) >= self.chunk_size:
            self.commit()
            return True
        else:
            return False

    def commit(self):
        self.i += 1
        self.j += len(self.items)

        if len(self.items) == 0:
            return

        if self.output:
            print(f'Committing chunk {self.i} to {self.table} ({len(self.items)})')

        data = [x for x in zip(*self.items)]
        frame = pd.DataFrame({
            k: astype(d, v) for (k, v), d in zip(self.schema.items(), data)
        })
        frame.to_csv(self.file, index=False, header=False)

        self.items.clear()

    def delete(self):
        self.file.close()
        os.remove(self.path)
