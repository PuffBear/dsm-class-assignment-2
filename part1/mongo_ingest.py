import json
from pymongo import MongoClient
import os
from tqdm import tqdm

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "yelp_db"
SUBSET_DIR = "./subset/"

def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

def ingest_businesses(db):
    b_col = db["businesses"]
    b_col.drop()
    print("Ingesting businesses...")
    
    # We will embed checkins into businesses. First, load checkins into memory.
    checkin_map = {}
    with open(os.path.join(SUBSET_DIR, "checkin.json"), "r") as f:
        for line in f:
            chk = json.loads(line)
            # dates are comma separated strings
            dates = [d.strip() for d in chk.get("date", "").split(",") if d.strip()]
            checkin_map[chk["business_id"]] = dates

    docs = []
    with open(os.path.join(SUBSET_DIR, "business.json"), "r") as f:
        for line in f:
            b = json.loads(line)
            # Embed checkins
            b["checkins"] = checkin_map.get(b["business_id"], [])
            docs.append(b)
            if len(docs) == 5000:
                b_col.insert_many(docs)
                docs = []
    if docs:
        b_col.insert_many(docs)
    print(f"Businesses ingested: {b_col.count_documents({})}")

def ingest_collection(db, file_name, col_name, batch_size=10000):
    col = db[col_name]
    col.drop()
    print(f"Ingesting {col_name}...")
    docs = []
    with open(os.path.join(SUBSET_DIR, file_name), "r") as f:
        for line in f:
            doc = json.loads(line)
            # Convert friends string into array for user.json
            if col_name == "users" and "friends" in doc:
                friends_list = [fr.strip() for fr in doc["friends"].split(",") if fr.strip() and fr.strip() != "None"]
                doc["friends"] = friends_list
            
            docs.append(doc)
            if len(docs) == batch_size:
                col.insert_many(docs)
                docs = []
    if docs:
        col.insert_many(docs)
    print(f"{col_name.capitalize()} ingested: {col.count_documents({})}")

def create_indexes(db):
    print("Creating Indexes...")
    db["businesses"].create_index("business_id", unique=True)
    db["businesses"].create_index("city")
    db["businesses"].create_index("stars")
    db["users"].create_index("user_id", unique=True)
    db["reviews"].create_index("business_id")
    db["reviews"].create_index("user_id")
    print("Indexes created.")

def main():
    db = get_db()
    ingest_businesses(db)
    ingest_collection(db, "user.json", "users")
    ingest_collection(db, "review.json", "reviews")
    ingest_collection(db, "tip.json", "tips")
    create_indexes(db)
    print("MongoDB ingestion complete!")

if __name__ == "__main__":
    main()
