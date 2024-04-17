from common import ELASTIC_PASSWORD
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
from tqdm.auto import tqdm


def characters_from_header(header):
    characters = []
    for line in header.split("\n"):
        if line.startswith("Characters:"):
            characters = [
                c.strip() for c in line.removeprefix("Characters: ").split(",")
            ]
    return characters


if __name__ == "__main__":

    es = Elasticsearch(
        "http://localhost:9200",
        basic_auth=("elastic", ELASTIC_PASSWORD),
    )

    index = "stories_ingest"

    new_mapping = {
        "properties": {
            "author": {"type": "keyword"},
            "author_url": {"type": "keyword"},
            "category": {"type": "keyword"},
            "characters": {"type": "keyword"},
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

    es.indices.put_mapping(index=index, properties={"characters": {"type": "keyword"}})
    n_records = es.count(index=index)["count"]
    batch_size = 1000

    input_records = scan(
        es,
        query={"query": {"match_all": {}}, "_source": False, "fields": ["header"]},
        index=index,
        size=batch_size,
    )

    actions = []

    for doc in tqdm(input_records, total=n_records):
        doc_id = doc["_id"]
        characters = characters_from_header(doc["fields"]["header"][0])
        action = {
            "_op_type": "update",
            "_index": index,
            "_id": doc_id,
            "doc": {"characters": characters},
        }
        actions.append(action)

        if len(actions) >= batch_size:
            bulk(es.options(request_timeout=300), actions)
            actions = []

    if actions:
        bulk(es.options(request_timeout=300), actions)
