from common import ELASTIC_PASSWORD
from elasticsearch import Elasticsearch

from elasticsearch.helpers import scan, bulk
from tqdm.auto import tqdm

CATEGORIES = set(("F/F", "F/M", "Gen", "M/M", "Multi", "Other"))
INPUT_FIELDS = ["category", "genre"]


def split_and_update(category, genre):
    additional_tags = [tag for tag in genre if tag not in CATEGORIES]
    category_new = [tag for tag in genre if tag in CATEGORIES]
    updates = {
        "additional_tags": additional_tags,
        "category_new": category_new,
    }
    if not category is None:
        updates["fandom"] = category
    return updates


if __name__ == "__main__":
    es = Elasticsearch(
        "http://localhost:9200",
        basic_auth=("elastic", ELASTIC_PASSWORD),
    )

    index = "stories_ingest"

    es.indices.put_mapping(
        index=index,
        properties={
            "additional_tags": {"type": "keyword"},
            "fandom": {"type": "keyword"},
            "category_new": {"type": "keyword"},
        },
    )

    n_records = es.count(index=index)["count"]
    batch_size = 1000

    input_records = scan(
        es,
        query={"query": {"match_all": {}}, "_source": False, "fields": INPUT_FIELDS},
        index=index,
        size=batch_size,
    )

    actions = []

    for doc in tqdm(input_records, total=n_records):
        doc_id = doc["_id"]
        if "fields" in doc:
            updates = split_and_update(doc["fields"].get("category", None), doc["fields"].get("genre", []))
            action = {
                "_op_type": "update",
                "_index": index,
                "_id": doc_id,
                "doc": updates,
            }
            actions.append(action)

        if len(actions) >= batch_size:
            bulk(es.options(request_timeout=300), actions)
            actions = []

    if actions:
        bulk(es.options(request_timeout=300), actions)
