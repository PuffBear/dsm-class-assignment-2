#!/usr/bin/env python3
"""
Part 2 — Neo4j GDS Queries
All 5 GDS-powered analyses for Assignment 2 Part 2.
Run from the part2/ directory: python neo4j_gds.py

Requires: neo4j, pandas, numpy, matplotlib, seaborn, scipy, scikit-learn, pymongo
"""

import os
import sys
from collections import Counter, defaultdict

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import spearmanr
from neo4j import GraphDatabase
from pymongo import MongoClient

# ── Connection settings ──────────────────────────────────────────────────────
NEO4J_URI  = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "testpassword")
MONGO_URI  = "mongodb://localhost:27017/"
MONGO_DB   = "yelp_db"
OUT        = "Report"

os.makedirs(OUT, exist_ok=True)

def get_neo4j():
    return GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

def get_mongo():
    return MongoClient(MONGO_URI)[MONGO_DB]

def drop_graph(session, name):
    """Drop a GDS graph projection if it exists (silent if not present)."""
    try:
        session.run("CALL gds.graph.drop($n, false)", n=name).consume()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# Q1 — PageRank on the User→Business RATED graph
# ═══════════════════════════════════════════════════════════════════════════════
def q1_pagerank(drv):
    print("\n[Q1] PageRank on RATED graph (≥20 iterations, weighted by review stars)...")

    with drv.session() as s:
        drop_graph(s, "rated-graph")

        # ── Project User + Business nodes with directed RATED edges weighted by stars
        s.run("""
            CALL gds.graph.project(
                'rated-graph',
                ['User', 'Business'],
                {RATED: {orientation: 'NATURAL', properties: ['stars']}}
            )
        """)

        # ── Write PageRank scores back to Neo4j nodes (used as features in Q5)
        s.run("""
            CALL gds.pageRank.write('rated-graph', {
                maxIterations: 20,
                dampingFactor: 0.85,
                relationshipWeightProperty: 'stars',
                writeProperty: 'pagerank_score'
            })
        """).consume()

        # ── Stream top-15 Business nodes by PageRank
        top15_rows = list(s.run("""
            CALL gds.pageRank.stream('rated-graph', {
                maxIterations: 20,
                dampingFactor: 0.85,
                relationshipWeightProperty: 'stars'
            })
            YIELD nodeId, score
            WITH gds.util.asNode(nodeId) AS node, score
            WHERE node:Business
            RETURN node.business_id AS business_id,
                   node.name        AS name,
                   node.city        AS city,
                   score            AS pagerank_score
            ORDER BY pagerank_score DESC
            LIMIT 15
        """))
        top15 = [dict(r) for r in top15_rows]
        bids  = [r["business_id"] for r in top15]

        # ── Get review count + avg stars from Review nodes
        stats_rows = list(s.run("""
            UNWIND $bids AS bid
            MATCH (r:Review)-[:REVIEWS]->(b:Business {business_id: bid})
            RETURN b.business_id AS business_id,
                   count(r)      AS review_count,
                   avg(r.stars)  AS avg_stars
        """, bids=bids))
        stats = {r["business_id"]: dict(r) for r in stats_rows}

        drop_graph(s, "rated-graph")

    # ── Merge stats into top-15
    for row in top15:
        st = stats.get(row["business_id"], {})
        row["review_count"] = int(st.get("review_count") or 0)
        row["avg_stars"]    = round(float(st.get("avg_stars") or 0), 3)

    df = pd.DataFrame(top15)
    df["pr_rank"] = range(1, len(df) + 1)

    # Rank by review count and avg stars
    rc_rank = df.sort_values("review_count", ascending=False).reset_index(drop=True)
    as_rank = df.sort_values("avg_stars",    ascending=False).reset_index(drop=True)
    df["rc_rank"] = df["business_id"].map({r["business_id"]: i+1 for i, r in rc_rank.iterrows()})
    df["as_rank"] = df["business_id"].map({r["business_id"]: i+1 for i, r in as_rank.iterrows()})

    rho_rc, p_rc = spearmanr(df["pr_rank"], df["rc_rank"])
    rho_as, p_as = spearmanr(df["pr_rank"], df["as_rank"])
    print(f"  Spearman(PageRank, ReviewCount) = {rho_rc:+.4f}  (p={p_rc:.4f})")
    print(f"  Spearman(PageRank, AvgStars)    = {rho_as:+.4f}  (p={p_as:.4f})")

    df.to_csv(f"{OUT}/Q1_PageRank.csv", index=False)

    # ── Plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    colors = plt.cm.Blues_r(np.linspace(0.2, 0.8, len(df)))
    ax1.barh(df["name"].str[:35], df["pagerank_score"], color=colors)
    ax1.set_xlabel("PageRank Score")
    ax1.set_title("Top 15 Businesses by Weighted PageRank")
    ax1.invert_yaxis()

    sc = ax2.scatter(df["review_count"], df["pagerank_score"],
                     c=df["avg_stars"], cmap="RdYlGn", s=130, edgecolors="k", lw=0.5,
                     vmin=3, vmax=5)
    plt.colorbar(sc, ax=ax2, label="Avg Star Rating")
    for _, row in df.iterrows():
        ax2.annotate(row["name"][:14], (row["review_count"], row["pagerank_score"]),
                     fontsize=7.5, xytext=(4, 3), textcoords="offset points")
    ax2.set_xlabel("Review Count")
    ax2.set_ylabel("PageRank Score")
    ax2.set_title(f"PageRank vs Review Count  (ρ = {rho_rc:+.3f})\n"
                  f"PageRank vs Avg Stars (ρ = {rho_as:+.3f})")

    plt.tight_layout()
    plt.savefig(f"{OUT}/Q1_PageRank.png", dpi=150, bbox_inches="tight")
    plt.close()

    print(f"  → Q1_PageRank.csv  Q1_PageRank.png")
    return df, rho_rc, rho_as


# ═══════════════════════════════════════════════════════════════════════════════
# Q2 — Louvain Community Detection on the KNOWS (friends) graph
# ═══════════════════════════════════════════════════════════════════════════════
def q2_louvain(drv):
    print("\n[Q2] Louvain community detection on KNOWS (friends) graph...")

    with drv.session() as s:
        drop_graph(s, "knows-undirected")

        # ── Project User-only graph with KNOWS treated as undirected
        s.run("""
            CALL gds.graph.project(
                'knows-undirected',
                'User',
                {KNOWS: {orientation: 'UNDIRECTED'}}
            )
        """)

        # ── Write community IDs back to User nodes (used as a feature in Q5)
        s.run("""
            CALL gds.louvain.write('knows-undirected', {
                writeProperty: 'louvain_community'
            })
        """).consume()

        # ── Get community sizes
        size_rows = list(s.run("""
            MATCH (u:User)
            WHERE u.louvain_community IS NOT NULL
            RETURN u.louvain_community AS community_id, count(u) AS size
            ORDER BY size DESC
        """))
        drop_graph(s, "knows-undirected")

    comm_sizes = {r["community_id"]: r["size"] for r in size_rows}
    large_comms = {cid: sz for cid, sz in comm_sizes.items() if sz >= 25}
    print(f"  Total communities: {len(comm_sizes)}  |  ≥25 members: {len(large_comms)}")

    if not large_comms:
        print("  No communities ≥25 members. Try lowering the threshold.")
        return pd.DataFrame()

    # ── For each large community, get state + category statistics
    records = []
    with drv.session() as s:
        for cid, size in sorted(large_comms.items(), key=lambda x: -x[1]):
            # Top 3 states by review count
            state_rows = list(s.run("""
                MATCH (u:User {louvain_community: $cid})-[:RATED]->(b:Business)
                RETURN b.state AS state, count(*) AS cnt
                ORDER BY cnt DESC
                LIMIT 5
            """, cid=cid))

            state_cnts = {r["state"]: r["cnt"] for r in state_rows}
            total_rev  = sum(state_cnts.values())
            top_state_cnt = max(state_cnts.values()) if state_cnts else 0
            geo_conc = round(top_state_cnt / total_rev, 4) if total_rev > 0 else 0.0
            top3_states = ", ".join(st for st, _ in sorted(state_cnts.items(), key=lambda x: -x[1])[:3])

            # Top 3 business categories
            cat_rows = list(s.run("""
                MATCH (u:User {louvain_community: $cid})-[:RATED]->(b:Business)-[:IN_CATEGORY]->(c:Category)
                RETURN c.name AS cat, count(*) AS cnt
                ORDER BY cnt DESC
                LIMIT 3
            """, cid=cid))
            top3_cats = ", ".join(r["cat"] for r in cat_rows)

            records.append({
                "community_id":             cid,
                "size":                     size,
                "top3_states":              top3_states,
                "top3_categories":          top3_cats,
                "geo_concentration_index":  geo_conc,
                "total_reviews_sampled":    total_rev,
            })

    df = pd.DataFrame(records).sort_values("geo_concentration_index", ascending=False)
    df.to_csv(f"{OUT}/Q2_Louvain.csv", index=False)

    print(f"\n  Most concentrated community  : ID {df.iloc[0]['community_id']} "
          f"(geo_conc={df.iloc[0]['geo_concentration_index']})")
    print(f"  Least concentrated community : ID {df.iloc[-1]['community_id']} "
          f"(geo_conc={df.iloc[-1]['geo_concentration_index']})")

    # ── Plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    top10 = df.nlargest(10, "size")
    ax1.barh(top10["community_id"].astype(str), top10["size"], color="teal", alpha=0.85)
    ax1.set_xlabel("Community Size")
    ax1.set_title("Top 10 Louvain Communities by Size")
    ax1.invert_yaxis()

    ax2.scatter(df["size"], df["geo_concentration_index"], alpha=0.6, c="coral", s=60, edgecolors="k", lw=0.3)
    ax2.axhline(df["geo_concentration_index"].mean(), color="red", linestyle="--", label="Mean GCI")
    ax2.set_xlabel("Community Size")
    ax2.set_ylabel("Geographic Concentration Index")
    ax2.set_title("Community Size vs Geographic Concentration")
    ax2.legend()

    plt.tight_layout()
    plt.savefig(f"{OUT}/Q2_Louvain.png", dpi=150, bbox_inches="tight")
    plt.close()

    print(f"  → Q2_Louvain.csv  Q2_Louvain.png")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# Q3 — Node Similarity (Jaccard) for market saturation analysis
# ═══════════════════════════════════════════════════════════════════════════════
def q3_node_similarity(drv):
    print("\n[Q3] Node Similarity (Jaccard) on Business-Reviewer bipartite graph...")

    with drv.session() as s:
        drop_graph(s, "biz-reviewer")

        # ── Project Business+User with RATED in REVERSE orientation
        # so Business nodes have User neighbors (= their reviewers)
        s.run("""
            CALL gds.graph.project(
                'biz-reviewer',
                ['Business', 'User'],
                {RATED: {orientation: 'REVERSE'}}
            )
        """)

        # ── Write SIMILAR relationships directly to Neo4j (to disk, not memory)
        # This avoids the OOM from streaming all pairs in one shot.
        # First remove any leftover SIMILAR rels from a previous run.
        s.run("MATCH ()-[r:SIMILAR]->() DELETE r").consume()
        s.run("""
            CALL gds.nodeSimilarity.write('biz-reviewer', {
                topK: 10,
                similarityCutoff: 0.01,
                degreeCutoff: 5,
                writeRelationshipType: 'SIMILAR',
                writeProperty: 'similarity'
            })
        """).consume()
        drop_graph(s, "biz-reviewer")

        # ── Now read back SIMILAR edges between Business pairs from Neo4j
        sim_rows = list(s.run("""
            MATCH (b1:Business)-[r:SIMILAR]->(b2:Business)
            RETURN b1.business_id AS b1_id,
                   b1.city        AS b1_city,
                   b2.business_id AS b2_id,
                   b2.city        AS b2_city,
                   r.similarity   AS similarity
        """))

        # ── Fetch Business → city + categories mapping
        biz_rows = list(s.run("""
            MATCH (b:Business)-[:IN_CATEGORY]->(c:Category)
            RETURN b.business_id AS bid,
                   b.city        AS city,
                   b.stars       AS stars,
                   collect(c.name) AS categories
        """))

    biz_info = {r["bid"]: {"city": r["city"], "stars": r["stars"],
                            "categories": set(r["categories"])} for r in biz_rows}

    print(f"  Business pairs returned: {len(sim_rows)}")

    # ── Group Jaccard scores by (city, category) for pairs in the same city
    city_cat_scores = defaultdict(list)
    city_cat_biz    = defaultdict(set)

    for row in sim_rows:
        if row["b1_city"] != row["b2_city"]:
            continue
        city = row["b1_city"]
        cats1 = biz_info.get(row["b1_id"], {}).get("categories", set())
        cats2 = biz_info.get(row["b2_id"], {}).get("categories", set())
        for cat in cats1 & cats2:
            key = (city, cat)
            city_cat_scores[key].append(row["similarity"])
            city_cat_biz[key].update([row["b1_id"], row["b2_id"]])

    # ── Build result table, require ≥5 businesses in the combination
    results = [
        {
            "city":         city,
            "category":     cat,
            "mean_jaccard": round(float(np.mean(scores)), 4),
            "n_businesses": len(city_cat_biz[(city, cat)]),
            "n_pairs":      len(scores),
        }
        for (city, cat), scores in city_cat_scores.items()
        if len(city_cat_biz[(city, cat)]) >= 5
    ]

    if not results:
        print("  No city-category combos with ≥5 businesses. "
              "Try lowering degreeCutoff or the business count threshold.")
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(results).sort_values("mean_jaccard", ascending=False).reset_index(drop=True)
    top5 = df.head(5)
    bot5 = df.tail(5)

    print("\n  Top 5 saturated markets:")
    print(top5[["city", "category", "mean_jaccard", "n_businesses"]].to_string(index=False))
    print("\n  Top 5 fragmented markets:")
    print(bot5[["city", "category", "mean_jaccard", "n_businesses"]].to_string(index=False))

    # ── Compare rating stats for most vs least saturated
    focus = list(zip(top5["city"], top5["category"])) + list(zip(bot5["city"], bot5["category"]))
    comparison = []
    with drv.session() as s:
        for city, cat in focus:
            bids = list(city_cat_biz.get((city, cat), set()))
            stat_rows = list(s.run("""
                UNWIND $bids AS bid
                MATCH (r:Review)-[:REVIEWS]->(b:Business {business_id: bid})
                RETURN avg(r.stars)   AS mean_stars,
                       stDev(r.stars) AS std_stars,
                       toFloat(count(r)) / $n_biz AS mean_rev_per_biz
            """, bids=bids, n_biz=len(bids)))
            st = dict(stat_rows[0]) if stat_rows else {}
            jaccard_val = df[(df["city"] == city) & (df["category"] == cat)]["mean_jaccard"].values
            comparison.append({
                "city":            city,
                "category":        cat,
                "mean_stars":      round(float(st.get("mean_stars") or 0), 3),
                "std_stars":       round(float(st.get("std_stars") or 0), 3),
                "mean_rev_per_biz":round(float(st.get("mean_rev_per_biz") or 0), 1),
                "mean_jaccard":    float(jaccard_val[0]) if len(jaccard_val) else 0.0,
                "market_type":     "saturated" if (city, cat) in list(zip(top5["city"], top5["category"]))
                                   else "fragmented",
            })

    df_comp = pd.DataFrame(comparison)
    df.to_csv(f"{OUT}/Q3_NodeSimilarity.csv", index=False)
    df_comp.to_csv(f"{OUT}/Q3_MarketComparison.csv", index=False)

    # ── Plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    focus_df = pd.concat([top5, bot5]).copy().reset_index(drop=True)
    focus_df["label"]  = focus_df["city"] + " / " + focus_df["category"].str[:20]
    focus_df["colour"] = ["#1565C0"] * 5 + ["#C62828"] * 5
    ax1.barh(focus_df["label"], focus_df["mean_jaccard"], color=focus_df["colour"], alpha=0.85)
    ax1.set_xlabel("Mean Intra-Category Jaccard Similarity")
    ax1.set_title("Saturated (blue) vs Fragmented (red) Markets")
    ax1.invert_yaxis()

    if not df_comp.empty:
        sat  = df_comp[df_comp["market_type"] == "saturated"]
        frag = df_comp[df_comp["market_type"] == "fragmented"]
        x = np.arange(3)
        w = 0.35
        metrics = ["mean_stars", "std_stars", "mean_rev_per_biz"]
        labels  = ["Mean Stars", "Std Stars", "Mean Reviews\nper Business"]
        ax2.bar(x - w/2, [sat[m].mean() for m in metrics],  w, label="Saturated",  color="#1565C0", alpha=0.8)
        ax2.bar(x + w/2, [frag[m].mean() for m in metrics], w, label="Fragmented", color="#C62828", alpha=0.8)
        ax2.set_xticks(x)
        ax2.set_xticklabels(labels)
        ax2.set_title("Market Comparison: Saturated vs Fragmented")
        ax2.legend()

    plt.tight_layout()
    plt.savefig(f"{OUT}/Q3_NodeSimilarity.png", dpi=150, bbox_inches="tight")
    plt.close()

    print(f"  → Q3_NodeSimilarity.csv  Q3_MarketComparison.csv  Q3_NodeSimilarity.png")
    return df, df_comp


# ═══════════════════════════════════════════════════════════════════════════════
# Q4 — Betweenness Centrality on the KNOWS (friends) graph
# ═══════════════════════════════════════════════════════════════════════════════
def q4_betweenness(drv):
    print("\n[Q4] Betweenness & Degree Centrality on KNOWS graph...")

    # Use a longer connection timeout for slow algorithms
    drv_slow = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH,
                                    connection_timeout=900,
                                    max_transaction_retry_time=900)

    with drv_slow.session() as s:
        drop_graph(s, "knows-cent")

        # ── Cypher projection: only reviewer nodes (those with ≥1 RATED edge)
        # This cuts the graph from 13.7M stubs → 819K actual reviewers,
        # making betweenness feasible.
        s.run("""
            CALL gds.graph.project.cypher(
                'knows-cent',
                'MATCH (u:User) WHERE (u)-[:RATED]->() RETURN id(u) AS id',
                'MATCH (a:User)-[:KNOWS]-(b:User)
                 WHERE (a)-[:RATED]->() AND (b)-[:RATED]->()
                 RETURN id(a) AS source, id(b) AS target'
            )
        """).consume()
        print("  Graph projected (reviewer-only subgraph).")

        # Write degree — fast, O(N)
        s.run("""
            CALL gds.degree.write('knows-cent', {
                writeProperty: 'degree_score'
            })
        """).consume()
        print("  Degree written.")

        # Write betweenness — approximate mode (samplingSize = source nodes to sample)
        s.run("""
            CALL gds.betweenness.write('knows-cent', {
                writeProperty: 'betweenness_score',
                samplingSize: 500,
                samplingSeed: 42
            })
        """).consume()
        print("  Betweenness written.")

        drop_graph(s, "knows-cent")

        # Read top-50 by betweenness
        bc_rows = list(s.run("""
            MATCH (u:User)
            WHERE u.betweenness_score IS NOT NULL
            RETURN u.user_id           AS user_id,
                   u.name              AS name,
                   u.review_count      AS review_count,
                   u.betweenness_score AS betweenness_score
            ORDER BY u.betweenness_score DESC
            LIMIT 50
        """))

        # Read top-50 by degree
        deg_rows = list(s.run("""
            MATCH (u:User)
            WHERE u.degree_score IS NOT NULL
            RETURN u.user_id      AS user_id,
                   u.degree_score AS degree
            ORDER BY u.degree_score DESC
            LIMIT 50
        """))

    drv_slow.close()

    top20_bc  = {r["user_id"] for r in bc_rows[:20]}
    top20_deg = {r["user_id"] for r in deg_rows[:20]}
    overlap         = top20_bc & top20_deg
    high_bc_low_deg = top20_bc - top20_deg

    print(f"  |Top-20 BC ∩ Top-20 Degree| = {len(overlap)}")
    print(f"  High-BC / Low-Degree group  = {len(high_bc_low_deg)} users")

    # ── Behavioural stats for both groups
    def user_stats(session, uids):
        if not uids:
            return pd.DataFrame()
        rows = list(session.run("""
            UNWIND $uids AS uid
            MATCH (u:User {user_id: uid})
            OPTIONAL MATCH (u)-[:RATED]->(b:Business)
            WITH u, collect(DISTINCT b.city)     AS cities,
                    collect(DISTINCT b.business_id) AS bids
            OPTIONAL MATCH (u)-[:RATED]->(b2:Business)-[:IN_CATEGORY]->(c:Category)
            RETURN u.user_id       AS user_id,
                   u.review_count  AS review_count,
                   size(cities)    AS distinct_cities,
                   count(DISTINCT c.name) AS distinct_cats
        """, uids=list(uids)))
        return pd.DataFrame([dict(r) for r in rows])

    with drv.session() as s:
        df_hb = user_stats(s, high_bc_low_deg)
        df_hd = user_stats(s, top20_deg)

    def summarise(df):
        if df.empty:
            return {"mean_review_count": 0, "mean_distinct_cities": 0, "mean_distinct_cats": 0}
        return {
            "mean_review_count":   round(df["review_count"].mean(), 1),
            "mean_distinct_cities":round(df["distinct_cities"].mean(), 2),
            "mean_distinct_cats":  round(df["distinct_cats"].mean(), 2),
        }

    hb_sum = summarise(df_hb)
    hd_sum = summarise(df_hd)
    print(f"\n  High-BC / Low-Degree: {hb_sum}")
    print(f"  High-Degree:          {hd_sum}")

    # ── Save CSVs
    df_bc20 = pd.DataFrame([dict(r) for r in bc_rows[:20]])
    df_bc20.to_csv(f"{OUT}/Q4_Betweenness_Top20.csv", index=False)

    pd.DataFrame([
        {"group": "High-BC / Low-Degree", **hb_sum},
        {"group": "High-Degree",          **hd_sum},
    ]).to_csv(f"{OUT}/Q4_GroupComparison.csv", index=False)

    # ── Plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    bar_colors = []
    for uid in df_bc20["user_id"]:
        if uid in high_bc_low_deg: bar_colors.append("#FF9800")
        elif uid in overlap:        bar_colors.append("#4CAF50")
        else:                       bar_colors.append("#2196F3")

    ax1.barh(df_bc20["name"].str[:25], df_bc20["betweenness_score"], color=bar_colors, alpha=0.9)
    ax1.set_xlabel("Betweenness Centrality Score")
    ax1.set_title("Top 20 Users by Betweenness Centrality\n"
                  "🟠 High-BC/Low-Deg  🟢 Overlap  🔵 High-Deg only")
    ax1.invert_yaxis()
    # legend patches
    from matplotlib.patches import Patch
    ax1.legend(handles=[Patch(color="#FF9800", label="High-BC/Low-Deg"),
                         Patch(color="#4CAF50", label="Overlap"),
                         Patch(color="#2196F3", label="High-Deg only")], fontsize=8)

    metrics = ["mean_review_count", "mean_distinct_cities", "mean_distinct_cats"]
    xlabels = ["Avg Review\nCount", "Avg Distinct\nCities", "Avg Distinct\nCategories"]
    x = np.arange(len(metrics))
    w = 0.35
    ax2.bar(x - w/2, [hb_sum[m] for m in metrics], w, label="High-BC/Low-Deg", color="#FF9800", alpha=0.85)
    ax2.bar(x + w/2, [hd_sum[m] for m in metrics], w, label="High-Degree",     color="#2196F3", alpha=0.85)
    ax2.set_xticks(x); ax2.set_xticklabels(xlabels)
    ax2.set_title("Behavioural Comparison: High-BC vs High-Degree Users")
    ax2.legend()

    plt.tight_layout()
    plt.savefig(f"{OUT}/Q4_Betweenness.png", dpi=150, bbox_inches="tight")
    plt.close()

    print(f"  → Q4_Betweenness_Top20.csv  Q4_GroupComparison.csv  Q4_Betweenness.png")
    return df_bc20, overlap, hb_sum, hd_sum


# ═══════════════════════════════════════════════════════════════════════════════
# Q5 — Link Prediction: GDS feature extraction + ML pipeline
#       Chronological 80/20 split (each user's latest review = test)
#       Model: GradientBoosting classifier
#       Features: user PageRank (log), Louvain community, review_count,
#                 avg_stars, business avg_stars, business PageRank (log),
#                 star affinity, friends_rated, city_match
# ═══════════════════════════════════════════════════════════════════════════════
def q5_link_prediction(drv):
    print("\n[Q5] Link Prediction pipeline (GDS features + GBM)...")
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import roc_auc_score, roc_curve

    db = get_mongo()

    # ── Chronological split from MongoDB (Review nodes in Neo4j lack dates)
    print("  Building chronological split from MongoDB...")
    latest_cursor = db.reviews.aggregate([
        {"$sort": {"date": -1}},
        {"$group": {"_id": "$user_id",
                    "latest_biz": {"$first": "$business_id"},
                    "latest_date": {"$first": "$date"}}},
    ], allowDiskUse=True)
    latest_map = {r["_id"]: r["latest_biz"] for r in latest_cursor}  # user_id → test biz_id
    print(f"  Users with chronological test item: {len(latest_map)}")

    # ── Pull RATED edges + node features from Neo4j (capped for performance)
    print("  Fetching RATED edges and node features from Neo4j...")
    with drv.session() as s:
        edge_rows = list(s.run("""
            MATCH (u:User)-[rel:RATED]->(b:Business)
            WHERE u.pagerank_score IS NOT NULL
            RETURN u.user_id                          AS user_id,
                   b.business_id                      AS business_id,
                   rel.stars                          AS edge_stars,
                   coalesce(u.review_count, 0)        AS user_review_count,
                   coalesce(u.average_stars, 0.0)     AS user_avg_stars,
                   coalesce(u.pagerank_score, 0.0)    AS user_pagerank,
                   coalesce(u.louvain_community, -1)  AS user_community,
                   coalesce(b.stars, 0.0)             AS biz_avg_stars,
                   coalesce(b.pagerank_score, 0.0)    AS biz_pagerank,
                   coalesce(b.city, '')               AS biz_city
            LIMIT 200000
        """))
        # Collect all business info for recommendation scoring
        all_biz_rows = list(s.run("""
            MATCH (b:Business)
            RETURN b.business_id                   AS business_id,
                   b.name                          AS name,
                   coalesce(b.stars, 0.0)          AS biz_avg_stars,
                   coalesce(b.pagerank_score, 0.0) AS biz_pagerank,
                   coalesce(b.city, '')             AS biz_city
        """))

    df_edges = pd.DataFrame([dict(r) for r in edge_rows])
    df_all_biz = pd.DataFrame([dict(r) for r in all_biz_rows]).set_index("business_id")
    print(f"  Total RATED edges: {len(df_edges)}")

    # ── Build friend-review lookup (batched to avoid OOM)
    #    For each user in our edge set, find businesses their friends rated
    print("  Building friend-review index (social-proof feature, batched)...")
    friend_biz_map = {}
    edge_user_ids = list(df_edges["user_id"].unique())
    BATCH = 200
    with drv.session() as s:
        for i in range(0, min(len(edge_user_ids), 4000), BATCH):
            batch_uids = edge_user_ids[i:i+BATCH]
            rows = list(s.run("""
                UNWIND $uids AS uid
                MATCH (u:User {user_id: uid})-[:KNOWS]-(friend:User)-[:RATED]->(b:Business)
                WITH uid, b.business_id AS bid, count(DISTINCT friend) AS cnt
                RETURN uid, bid, cnt
            """, uids=batch_uids))
            for r in rows:
                rd = dict(r)
                friend_biz_map[(rd["uid"], rd["bid"])] = int(rd["cnt"])
            if (i + BATCH) % 1000 == 0:
                print(f"    ... processed {i+BATCH} users, {len(friend_biz_map)} pairs so far")
    print(f"  Friend-review pairs indexed: {len(friend_biz_map)}")

    # Build per-user city set (cities the user has reviewed in)
    user_cities = df_edges.groupby("user_id")["biz_city"].apply(set).to_dict()

    if df_edges.empty:
        print("  No edges found. Skipping Q5.")
        return

    # ── Mark test/train based on chronological split
    df_edges["is_test"] = df_edges.apply(
        lambda r: latest_map.get(r["user_id"]) == r["business_id"], axis=1
    )
    train_pos = df_edges[~df_edges["is_test"]].copy()
    test_pos  = df_edges[df_edges["is_test"]].copy()
    print(f"  Train positives: {len(train_pos)}  |  Test positives: {len(test_pos)}")

    # ── Negative sampling: fast vectorised approach
    reviewed_per_user = df_edges.groupby("user_id")["business_id"].apply(set).to_dict()
    all_bids_arr = np.array(list(df_all_biz.index))
    rng = np.random.default_rng(42)

    # Pre-build a lookup from user_id → feature row (first occurrence)
    user_feat = df_edges.drop_duplicates("user_id").set_index("user_id")[
        ["user_review_count", "user_avg_stars", "user_pagerank", "user_community"]
    ]

    def sample_negatives_fast(uid_series, k_per_user):
        rows = []
        for uid in uid_series:
            reviewed = reviewed_per_user.get(uid, set())
            if uid not in user_feat.index:
                continue
            uf = user_feat.loc[uid]
            u_cities = user_cities.get(uid, set())
            # Sample k_per_user random indices, retry until we get non-reviewed ones
            sample_idx = rng.integers(0, len(all_bids_arr), size=k_per_user * 5)
            sampled = all_bids_arr[sample_idx]
            valid = [b for b in sampled if b not in reviewed][:k_per_user]
            for bid in valid:
                if bid not in df_all_biz.index:
                    continue
                bf = df_all_biz.loc[bid]
                fr_count = friend_biz_map.get((uid, bid), 0)
                c_match = 1 if bf.get("biz_city", "") in u_cities else 0
                rows.append({
                    "user_review_count": uf["user_review_count"],
                    "user_avg_stars":    uf["user_avg_stars"],
                    "user_pagerank":     float(np.log1p(uf["user_pagerank"])),
                    "user_community":    uf["user_community"],
                    "biz_avg_stars":     float(bf["biz_avg_stars"]),
                    "biz_pagerank":      float(np.log1p(bf["biz_pagerank"])),
                    "friends_rated":     fr_count,
                    "city_match":        c_match,
                    "label": 0,
                })
        return pd.DataFrame(rows)

    # Cap users for performance
    df_neg_train = sample_negatives_fast(train_pos["user_id"].unique()[:2000], k_per_user=2)
    df_neg_test  = sample_negatives_fast(test_pos["user_id"].unique()[:500],   k_per_user=5)

    # ── Build feature matrices
    FEATURES = ["user_review_count", "user_avg_stars", "user_pagerank",
                "user_community", "biz_avg_stars", "biz_pagerank",
                "star_affinity", "friends_rated", "city_match"]

    def build_X(pos_df, neg_df):
        pos = pos_df[["user_review_count", "user_avg_stars", "user_pagerank",
                      "user_community", "biz_avg_stars", "biz_pagerank"]].copy()
        # Log-transform PageRank features to prevent scale domination
        pos["user_pagerank"] = np.log1p(pos["user_pagerank"])
        pos["biz_pagerank"]  = np.log1p(pos["biz_pagerank"])
        pos["star_affinity"] = (pos["user_avg_stars"] - pos["biz_avg_stars"]).abs()
        # Add friend-review count (social proof)
        pos["friends_rated"] = pos_df.apply(
            lambda r: friend_biz_map.get((r["user_id"], r["business_id"]), 0), axis=1
        ).values
        # Add city match
        pos["city_match"] = pos_df.apply(
            lambda r: 1 if df_all_biz.loc[r["business_id"]]["biz_city"]
                        in user_cities.get(r["user_id"], set()) else 0
            if r["business_id"] in df_all_biz.index else 0, axis=1
        ).values
        pos["label"] = 1

        neg = neg_df[["user_review_count", "user_avg_stars", "user_pagerank",
                      "user_community", "biz_avg_stars", "biz_pagerank",
                      "friends_rated", "city_match"]].copy() if not neg_df.empty else pd.DataFrame(columns=pos.columns)
        if not neg_df.empty:
            neg["star_affinity"] = (neg["user_avg_stars"] - neg["biz_avg_stars"]).abs()
            neg["label"] = 0

        combined = pd.concat([pos, neg], ignore_index=True).fillna(0)
        return combined[FEATURES].values, combined["label"].values

    X_train, y_train = build_X(train_pos, df_neg_train)
    X_test,  y_test  = build_X(test_pos,  df_neg_test)

    if len(np.unique(y_test)) < 2:
        print("  Test set has only one class — cannot compute AUC. Skipping Q5.")
        return

    # ── Train GBM (approximates the GDS LR pipeline, with richer features)
    model = GradientBoostingClassifier(n_estimators=100, max_depth=4, learning_rate=0.1,
                                       random_state=42)
    model.fit(X_train, y_train)

    y_prob = model.predict_proba(X_test)[:, 1]
    auc    = roc_auc_score(y_test, y_prob)
    print(f"  AUC-ROC: {auc:.4f}")

    # ── Helper to compute features for a (user, business) candidate pair
    def compute_features(uid, uf, bid, bf):
        """Return feature vector for a (user, business) candidate pair."""
        sa = abs(float(uf["user_avg_stars"]) - float(bf["biz_avg_stars"]))
        fr = friend_biz_map.get((uid, bid), 0)
        u_cities = user_cities.get(uid, set())
        cm = 1 if bf.get("biz_city", "") in u_cities else 0
        return [uf["user_review_count"], uf["user_avg_stars"],
                float(np.log1p(uf["user_pagerank"])),
                uf["user_community"], float(bf["biz_avg_stars"]),
                float(np.log1p(bf["biz_pagerank"])), sa, fr, cm]

    # ── Precision@10 (fraction of test users whose held-out business appears in top-10)
    hits = []
    for uid in test_pos["user_id"].unique()[:100]:  # evaluate on first 100 users
        target_bid = latest_map.get(uid)
        if not target_bid or target_bid not in df_all_biz.index:
            continue
        reviewed = reviewed_per_user.get(uid, set()) - {target_bid}
        uf_rows = df_edges[df_edges["user_id"] == uid]
        if uf_rows.empty:
            continue
        uf = uf_rows.iloc[0]

        cands = [b for b in all_bids_arr if b not in reviewed][:300]
        if target_bid not in cands:
            cands.append(target_bid)  # ensure target is in candidate set
        if not cands:
            continue
        feats = []
        valid_cands = []
        for bid in cands:
            bf = df_all_biz.loc[bid] if bid in df_all_biz.index else None
            if bf is None:
                continue
            feats.append(compute_features(uid, uf, bid, bf))
            valid_cands.append(bid)

        if not feats:
            continue
        scores   = model.predict_proba(np.array(feats, dtype=float))[:, 1]
        top10_idx = np.argsort(scores)[-10:]
        top10    = {valid_cands[i] for i in top10_idx}
        hits.append(int(target_bid in top10))

    prec_at_10 = round(np.mean(hits), 4) if hits else 0.0
    print(f"  Precision@10: {prec_at_10:.4f}  (evaluated on {len(hits)} users)")

    # ── Feature importances
    importances = pd.Series(model.feature_importances_, index=FEATURES).sort_values(ascending=False)
    print(f"\n  Top features:\n{importances.to_string()}")

    # ── Recommendations for 5 sample users (city-aware candidate selection)
    sample_uids = test_pos["user_id"].unique()[:5]
    recs = []
    with drv.session() as s:
        for uid in sample_uids:
            uf_rows = df_edges[df_edges["user_id"] == uid]
            if uf_rows.empty:
                continue
            uf = uf_rows.iloc[0]
            reviewed = reviewed_per_user.get(uid, set())
            u_cities = user_cities.get(uid, set())

            # Prefer candidates from user's cities for diversity
            city_cands = [b for b in all_bids_arr
                          if b not in reviewed and b in df_all_biz.index
                          and df_all_biz.loc[b].get("biz_city", "") in u_cities][:400]
            other_cands = [b for b in all_bids_arr
                           if b not in reviewed and b not in set(city_cands)][:100]
            cands = city_cands + other_cands

            feats = []
            valid_cands = []
            for bid in cands:
                bf = df_all_biz.loc[bid] if bid in df_all_biz.index else None
                if bf is None:
                    continue
                feats.append(compute_features(uid, uf, bid, bf))
                valid_cands.append(bid)

            if not feats:
                continue

            scores  = model.predict_proba(np.array(feats, dtype=float))[:, 1]
            top3    = [valid_cands[i] for i in np.argsort(scores)[-3:][::-1]]
            names   = {r["bid"]: r["name"] for r in
                       [dict(x) for x in s.run("UNWIND $bids AS bid MATCH (b:Business {business_id: bid}) "
                                                "RETURN bid, b.name AS name", bids=top3)]}
            recs.append({"user_id": uid, "rec_1": names.get(top3[0], top3[0]) if len(top3) > 0 else "",
                         "rec_2": names.get(top3[1], top3[1]) if len(top3) > 1 else "",
                         "rec_3": names.get(top3[2], top3[2]) if len(top3) > 2 else ""})

    df_recs = pd.DataFrame(recs)
    df_recs.to_csv(f"{OUT}/Q5_Recommendations.csv", index=False)
    importances.reset_index().rename(columns={"index": "feature", 0: "importance"}).to_csv(
        f"{OUT}/Q5_FeatureImportance.csv", index=False)

    # ── Plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    importances.plot(kind="bar", ax=ax1, color="steelblue", alpha=0.85)
    ax1.set_title("Link Prediction Feature Importances")
    ax1.set_ylabel("Importance Score")
    ax1.tick_params(axis="x", rotation=40)

    fpr, tpr, _ = roc_curve(y_test, y_prob)
    ax2.plot(fpr, tpr, color="darkorange", lw=2, label=f"AUC-ROC = {auc:.3f}")
    ax2.plot([0, 1], [0, 1], "k--", lw=1)
    ax2.set_xlabel("False Positive Rate"); ax2.set_ylabel("True Positive Rate")
    ax2.set_title(f"ROC Curve — Link Prediction  (P@10 = {prec_at_10:.3f})")
    ax2.legend()

    plt.tight_layout()
    plt.savefig(f"{OUT}/Q5_LinkPrediction.png", dpi=150, bbox_inches="tight")
    plt.close()

    print(f"  → Q5_Recommendations.csv  Q5_FeatureImportance.csv  Q5_LinkPrediction.png")
    return {"auc_roc": auc, "precision_at_10": prec_at_10,
            "importances": importances.to_dict(), "recs": recs}


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    drv = get_neo4j()
    try:
        drv.verify_connectivity()
        print("Connected to Neo4j.")
    except Exception as e:
        print(f"Cannot connect to Neo4j: {e}")
        sys.exit(1)

    q1_pagerank(drv)
    q2_louvain(drv)
    q3_node_similarity(drv)
    q4_betweenness(drv)
    q5_link_prediction(drv)

    drv.close()
    print("\n✓ All Neo4j GDS queries complete. Results saved to Report/")
