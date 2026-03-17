import os
import json
from neo4j import GraphDatabase
from tqdm import tqdm

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "testpassword") 
SUBSET_DIR = "./subset/"

def get_driver():
    # If using authentication: return GraphDatabase.driver(URI, auth=AUTH)
    return GraphDatabase.driver(URI) # without auth for local dev

def drop_all(tx):
    tx.run("MATCH (n) DETACH DELETE n;")

def build_constraints(driver):
    queries = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (b:Business) REQUIRE b.business_id IS UNIQUE;",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE;",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE;"
    ]
    with driver.session() as session:
        for q in queries:
            try:
                session.run(q)
            except Exception as e:
                print(f"Skipping constraint: {e}")

def create_businesses(driver):
    with driver.session() as session:
        print("Ingesting Businesses and Categories...")
        docs = []
        with open(os.path.join(SUBSET_DIR, "business.json"), "r") as f:
            for line in f:
                b = json.loads(line)
                cats = [c.strip() for c in (b.get("categories") or "").split(",") if c.strip()]
                docs.append({
                    "id": b["business_id"], 
                    "name": b["name"], 
                    "city": b["city"], 
                    "stars": b["stars"],
                    "categories": cats
                })
                
                if len(docs) >= 500:
                    session.execute_write(_insert_businesses, docs)
                    docs = []
            if docs:
                session.execute_write(_insert_businesses, docs)

def _insert_businesses(tx, docs):
    query = """
    UNWIND $batch AS b
    MERGE (bus:Business {business_id: b.id})
    SET bus.name = b.name, bus.city = b.city, bus.stars = b.stars
    WITH bus, b
    UNWIND b.categories as cat
    MERGE (c:Category {name: cat})
    MERGE (bus)-[:IN_CATEGORY]->(c)
    """
    tx.run(query, batch=docs)

def create_users(driver):
    with driver.session() as session:
        print("Ingesting Users and FRIEND connections...")
        docs = []
        with open(os.path.join(SUBSET_DIR, "user.json"), "r") as f:
            for line in f:
                u = json.loads(line)
                friends = [f.strip() for f in u.get("friends", "").split(",") if f.strip() and f.strip() != "None"]
                docs.append({
                    "id": u["user_id"],
                    "name": u["name"],
                    "review_count": u["review_count"],
                    "stars": u["average_stars"],
                    "friends": friends
                })
                
                if len(docs) >= 500:
                    session.execute_write(_insert_users, docs)
                    docs = []
            if docs:
                session.execute_write(_insert_users, docs)

def _insert_users(tx, docs):
    query = """
    UNWIND $batch AS u
    MERGE (usr:User {user_id: u.id})
    SET usr.name = u.name, usr.review_count = u.review_count, usr.average_stars = u.stars
    WITH usr, u
    UNWIND u.friends as friend_id
    MERGE (f:User {user_id: friend_id})
    MERGE (usr)-[:KNOWS]->(f)
    """
    tx.run(query, batch=docs)

def create_reviews(driver):
    with driver.session() as session:
        print("Ingesting Reviews and WROTE/REVIEWS connections...")
        docs = []
        with open(os.path.join(SUBSET_DIR, "review.json"), "r") as f:
            for line in f:
                r = json.loads(line)
                docs.append({
                    "id": r["review_id"],
                    "u_id": r["user_id"],
                    "b_id": r["business_id"],
                    "stars": r["stars"],
                    "useful": r["useful"]
                })
                
                if len(docs) >= 500:
                    session.execute_write(_insert_reviews, docs)
                    docs = []
            if docs:
                session.execute_write(_insert_reviews, docs)

def _insert_reviews(tx, docs):
    query = """
    UNWIND $batch AS r
    MERGE (rev:Review {review_id: r.id})
    SET rev.stars = r.stars, rev.useful = r.useful
    WITH rev, r
    MATCH (u:User {user_id: r.u_id})
    MATCH (b:Business {business_id: r.b_id})
    MERGE (u)-[:WROTE]->(rev)
    MERGE (rev)-[:REVIEWS]->(b)
    // Direct shortcut relationship for faster traversal
    MERGE (u)-[rel:RATED]->(b)
    SET rel.stars = r.stars
    """
    tx.run(query, batch=docs)

def main():
    driver = get_driver()
    with driver.session() as session:
        print("Clearing DB...")
        session.execute_write(drop_all)
        print("Building Constraints...")
        
    build_constraints(driver)
        
    create_businesses(driver)
    create_users(driver)
    create_reviews(driver)
    print("Neo4j Ingestion Complete.")

if __name__ == "__main__":
    main()
