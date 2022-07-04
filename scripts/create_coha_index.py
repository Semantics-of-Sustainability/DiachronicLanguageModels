"""
Creates an index based on documents from COHA.
A freely available sample can be downloaded here: https://www.corpusdata.org/coha/samples/text.zip
"""
from whoosh.index import create_in
from whoosh.fields import *
import os
import tqdm
import sys

schema = Schema(content=TEXT(stored=True),
               path=ID(stored=True),
               id=ID(stored=True),
               genre=KEYWORD(stored=True),
               year=NUMERIC(stored=True))

def extract_article(filename, path):
    genre, year, i = filename.split('_')
    idx = i.split('.')[0]
    with open(os.path.join(path,filename)) as fin:
        _ = fin.readline()
        content = fin.read()
    return {
        'content': content,
        'path': filename,
        'id': idx,
        'genre': genre,
        'year': year
    }

if __name__ == "__main__":
    data_path = sys.argv[1]
    index_dir = sys.argv[2]
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)
    ix = create_in(index_dir, schema)
    
    writer = ix.writer()

    for fname in tqdm.tqdm(os.listdir(data_path)):
        writer.add_document(**extract_article(fname, data_path))
    writer.commit()