import re
import sqlite3
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

import ebooklib
import justext
from bs4 import BeautifulSoup
from ebooklib import epub

import os

DATADIR = Path("/mnt/D/p266261/cratch-tmp")
N_PROCESSES = 10

ELASTIC_PASSWORD = os.environ["GOLEM_ES_PASSWORD"]

COLUMNS = [
    "Path",
    "Title",
    "Author",
    "Category",
    "Genre",
    "Language",
    "Status",
    "Published",
    "Updated",
    "Packaged",
    "Rating",
    "Warnings",
    "Chapters",
    "Words",
    "Publisher",
    "StoryURL",
    "AuthorURL",
    "Summary",
    "Relationships",
    "Series",
    "SeriesURL",
    "Comments",
    "Kudos",
    "Collections",
]


FIELDS = [
    "path",
    "title",
    "author",
    "category",
    "genre",
    "language",
    "status",
    "published",
    "updated",
    "packaged",
    "rating",
    "warnings",
    "chapters",
    "words",
    "publisher",
    "story_url",
    "author_url",
    "summary",
    "relationships",
    "series",
    "series_url",
    "comments",
    "kudos",
    "collections",
]

CATEGORY_SPLIT = re.compile(r"[|,]")


def parse_long_from_text(text):
    value = None
    try:
        value = int(text.replace(",", ""))
    except ValueError:
        pass
    return value


def row_to_record(row):
    record = {key: val for key, val in zip(FIELDS, row)}
    for key in ["genre", "warnings"]:
        if record[key]:
            record[key] = [g.strip() for g in record[key].split(",")]
    if record["category"]:
        record["category"] = [
            c.strip() for c in CATEGORY_SPLIT.split(record["category"]) if c
        ]
    if record["published"]:
        record["published"] = datetime.strptime(record["published"], "%Y-%m-%d").date()
    if record["updated"]:
        record["updated"] = datetime.strptime(record["updated"], "%Y-%m-%d").date()
    if record["packaged"]:
        record["packaged"] = datetime.strptime(record["packaged"], "%Y-%m-%d %H:%M:%S")
    for key in ["comments", "words"]:
        record[key] = (
            parse_long_from_text(record[key]) if record[key] is not None else None
        )
    record["story_id"] = (
        int(record["story_url"].split("/")[-1]) if record["story_url"] else None
    )
    return record


def get_epub_story_text(path):
    book = epub.read_epub(path)
    parts = [p for p in book.get_items_of_type(ebooklib.ITEM_DOCUMENT)]
    header = (
        BeautifulSoup(parts[0].get_content(), features="xml").find(name="body").text
    )

    # we only use justext for paragraphs splitting from html, no boilerplate classification
    paragraphs = [
        p.text
        for part in parts[1:]
        for p in justext.justext(part.get_body_content(), frozenset())
    ]
    return {"header": header, "story_content": "\n\n".join(paragraphs)}


def add_epub_content(record, key="path"):
    path = DATADIR.joinpath(record[key])
    if path.exists():
        try:
            content_data = get_epub_story_text(path)
            record = record | content_data
        except Exception:
            return None
    return record


def story_generator(skiplist):
    with Pool(N_PROCESSES) as pool:
        for record in pool.imap_unordered(
            add_epub_content, records_from_sqlite(skiplist)
        ):
            if record:
                record["_id"] = record["path"]
                yield record


def records_from_sqlite(skiplist):
    con = sqlite3.connect("ao3_current.sqlite3")
    cur = con.cursor()
    cur.execute("SELECT * FROM metadata")
    for row in cur:
        record = row_to_record(row)
        if record["path"] not in skiplist:
            yield record
