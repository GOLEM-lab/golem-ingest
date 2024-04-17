#!/usr/bin/env python
# coding: utf-8


import logging
import pickle
import sqlite3

from common import CERT_FINGERPRINT, ELASTIC_PASSWORD, HOST, story_generator
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk
from tqdm.auto import tqdm

INDEX = "stories_ingest"


BATCH_SIZE = 50
N_ROWS = 9622381
# N_ROWS = None


MAPPING = {
    "properties": {
        "author": {"type": "keyword"},
        "author_url": {"type": "keyword"},
        "category": {"type": "keyword"},
        "chapters": {"type": "long"},
        "collections": {"type": "keyword"},
        "comments": {"type": "long"},
        "genre": {"type": "keyword"},
        "header": {"type": "text"},
        "kudos": {"type": "long"},
        "language": {"type": "keyword"},
        "packaged": {"type": "date"},
        "path": {"type": "keyword"},
        "published": {"type": "date"},
        "rating": {"type": "keyword"},
        "relationships": {"type": "keyword"},
        "series": {"type": "keyword"},
        "series_url": {"type": "keyword"},
        "status": {"type": "keyword"},
        "story_content": {"type": "text"},
        "story_id": {"type": "long"},
        "story_url": {"type": "keyword"},
        "summary": {"type": "keyword"},
        "title": {"type": "keyword"},
        "updated": {"type": "date"},
        "warnings": {"type": "keyword"},
        "words": {"type": "long"},
    }
}

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

    if N_ROWS is None:
        con = sqlite3.connect("ao3_current.sqlite3")
        cur = con.cursor()
        logger.info("Counting records")
        n_rows = cur.execute("SELECT COUNT(*) FROM metadata").fetchone()[0]
        cur.close()
        con.close()
    else:
        logger.info("Using predefined number of records")
        n_rows = N_ROWS
    logger.info(f"{n_rows} records in SQLite database")

    es = Elasticsearch(
        "http://localhost:9200",
        basic_auth=("elastic", ELASTIC_PASSWORD),
    )

    es.options(ignore_status=[400, 404]).indices.delete(index=INDEX)
    es.indices.create(index=INDEX, mappings=MAPPING)

    query = {"query": {"match_all": {}}, "source": False, "fields": []}
    logger.info("Retrieving paths of stories already indexed")
    total_docs = es.count(index=INDEX)["count"]
    paths = [hit["_id"] for hit in tqdm(scan(es, query, index=INDEX), total=total_docs)]

    with open("processed_paths.pkl", "wb")  as f:
        pickle.dump(paths, f)

    with open("processed_paths.pkl", "rb") as f:
        paths = pickle.load(f)

    logger.info("Indexing remaining stories")
    progress = tqdm(total=n_rows - len(paths))

    for ok, action in streaming_bulk(
        client=es,
        actions=story_generator(frozenset(paths)),
        index=INDEX,
        chunk_size=BATCH_SIZE,
        raise_on_error=False,
    ):
        progress.update(1)
