#!/usr/bin/env python
# coding: utf-8

import logging
import re
from multiprocessing import Pool
from functools import partial
import sqlite3

from common import DATADIR, N_PROCESSES, ELASTIC_PASSWORD, add_epub_content, row_to_record
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from tqdm.auto import tqdm

INDEX = "stories_ingest"


def story_generator(path_dict):
    with Pool(N_PROCESSES) as pool:
        for record in pool.imap_unordered(
            partial(add_epub_content, key="local_path"),
            records_from_sqlite(path_dict),
        ):
            if record:
                record["_id"] = record["path"]
                yield record


def records_from_sqlite(path_dict):
    con = sqlite3.connect("../ao3_current.sqlite3")
    cur = con.cursor()
    cur.execute("SELECT * FROM metadata")
    for row in cur:
        record = row_to_record(row)
        if record["path"].startswith("queue16"):
            record["local_path"] = path_dict[record["story_id"]]
            yield record


BATCH_SIZE = 50


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

    es = Elasticsearch(
        "http://localhost:9200",
        basic_auth=("elastic", ELASTIC_PASSWORD),
        request_timeout=300,
    )

    logger.info("Constructing lookup dictionary for correct local filenames")
    local_paths = list(DATADIR.joinpath("queue16").glob("*.epub"))
    regex = re.compile(r"(\d+)\.epub")
    path_dict = {
        int(regex.search(p.name).groups()[0]): str(p.relative_to(DATADIR))
        for p in local_paths
    }

    logger.info('Updating "queue16" stories')
    progress = tqdm()

    for ok, action in streaming_bulk(
        client=es,
        actions=story_generator(path_dict),
        index=INDEX,
        chunk_size=BATCH_SIZE,
        raise_on_error=False,
    ):
        progress.update(1)
