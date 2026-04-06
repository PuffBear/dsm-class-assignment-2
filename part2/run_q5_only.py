#!/usr/bin/env python3
"""Re-run only Q5 (Link Prediction) with improved features."""
import sys
sys.path.insert(0, ".")
from neo4j_gds import get_neo4j, q5_link_prediction

drv = get_neo4j()
try:
    drv.verify_connectivity()
    print("Connected to Neo4j.")
except Exception as e:
    print(f"Cannot connect: {e}")
    sys.exit(1)

q5_link_prediction(drv)
drv.close()
print("\n✓ Q5 re-run complete.")
