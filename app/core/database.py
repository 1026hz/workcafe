from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from app.core.config import settings

_client: MongoClient = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(settings.mongodb_url)
    return _client


def get_db():
    return get_client()[settings.mongodb_db_name]


def get_cafes_collection() -> Collection:
    db = get_db()
    col = db["cafes"]
    col.create_index([("kakao_id", ASCENDING)], unique=True, background=True)
    col.create_index([("district", ASCENDING)], background=True)
    col.create_index([("updated_at", DESCENDING)], background=True)
    return col


def get_reviews_collection() -> Collection:
    db = get_db()
    col = db["reviews"]
    col.create_index([("cafe_id", ASCENDING), ("source_id", ASCENDING)], unique=True, background=True)
    col.create_index([("cafe_id", ASCENDING), ("created_at", DESCENDING)], background=True)
    return col


def get_batch_logs_collection() -> Collection:
    db = get_db()
    col = db["batch_logs"]
    col.create_index([("job_name", ASCENDING), ("started_at", DESCENDING)], background=True)
    return col
