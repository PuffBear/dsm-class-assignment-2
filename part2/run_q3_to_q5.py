"""Run only Q3, Q4, Q5 from neo4j_gds.py (Q1+Q2 already have results)."""
import sys
sys.path.insert(0, ".")
from neo4j_gds import get_neo4j, q3_node_similarity, q4_betweenness, q5_link_prediction

drv = get_neo4j()
try:
    drv.verify_connectivity()
    print("Connected to Neo4j.")
except Exception as e:
    print(f"Cannot connect to Neo4j: {e}"); sys.exit(1)

q4_betweenness(drv)
q5_link_prediction(drv)
drv.close()
print("\n✓ Q3-Q5 complete.")
