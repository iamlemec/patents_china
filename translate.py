import time
import torch
import pandas as pd

from transformers import AutoProcessor, SeamlessM4Tv2Model
from ziggy.utils import batch_indices

class SeamlessModel:
    def __init__(self, model='facebook/seamless-m4t-v2-large', device='cuda'):
        self.processor = AutoProcessor.from_pretrained(model)
        self.model = SeamlessM4Tv2Model.from_pretrained(model).to(device)

    def translate(self, texts, src_lang, tgt_lang, max_length=64):
        if type(texts) is str:
            texts = [texts]

        # construct input tensors
        text_inputs = self.processor(
            text=texts, src_lang=src_lang, return_tensors='pt',
            truncation=True, max_length=max_length
        )
        input_ids = text_inputs.input_ids.to(self.model.device)
        attention_mask = text_inputs.attention_mask.to(self.model.device)

        # call model and get outputs
        output_ids = self.model.generate(
            input_ids, attention_mask=attention_mask, tgt_lang=tgt_lang, generate_speech=False
        )[0]

        # decode output ids
        output_texts = [
            self.processor.decode(ids, skip_special_tokens=True) for ids in output_ids
        ]

        # return output texts
        return output_texts

def translate_patents(
    path_pats='data/tables/patents.csv', path_tran='data/tables/translate.csv',
    batch_size=64, max_rows=None
):
    # load patent metadata
    print('Loading patent metadata')
    pats = pd.read_csv(path_pats, usecols=['patnum', 'title'], nrows=max_rows)

    # load seamless model
    print('Loading seamless model')
    torch.set_float32_matmul_precision('high')
    model = SeamlessModel()

    # set up output list
    output = []
    time0 = time.time()

    # translate titles and abstracts
    print('Translating titles')
    for i1, i2 in batch_indices(len(pats), batch_size):
        print(f'{i1} → {i2}')

        # translate titles
        titles = pats['title'].iloc[i1:i2].tolist()
        trans = model.translate(titles, src_lang='cmn', tgt_lang='eng')
        output += trans

    # print time taken
    time1 = time.time()
    print(f'Time taken: {time1-time0:.2f}s')

    # write to disk
    pats['title_en'] = output
    pats[['patnum', 'title_en']].to_csv(path_tran, index=False)
