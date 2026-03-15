from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

def get_db():
    client = MongoClient(
        os.getenv("MONGO_URI"),
        serverSelectionTimeoutMS=30000,
        socketTimeoutMS=None,        # không timeout khi đọc data
        connectTimeoutMS=30000,
    )
    return client[os.getenv("MONGO_DB")]

def get_collection():
    db = get_db()
    return db[os.getenv("MONGO_COLLECTION", "summary")]