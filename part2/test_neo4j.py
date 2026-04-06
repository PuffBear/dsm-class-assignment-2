from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "testpassword") 

def test_connection():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        driver.verify_connectivity()
        print("Connected to Neo4j successfully.")
        with driver.session() as session:
            # Check GDS version
            res = session.run("CALL gds.version()")
            print(f"GDS Version: {res.single()[0]}")
            
            # Check node counts
            print("Node counts:")
            print("\tUser:", session.run("MATCH (n:User) RETURN count(n)").single()[0])
            print("\tBusiness:", session.run("MATCH (n:Business) RETURN count(n)").single()[0])
            print("\tRATED edges:", session.run("MATCH ()-[r:RATED]->() RETURN count(r)").single()[0])
            print("\tKNOWS edges:", session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()[0])
            
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_connection()
