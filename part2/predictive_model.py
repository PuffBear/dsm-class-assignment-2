#!/usr/bin/env python3
"""
Part 2 — Predictive Modelling
Regression model to predict the number of useful votes a review will receive.

Features extracted from MongoDB (review-level + user-level) and Neo4j (graph-derived).
Model: Random Forest Regressor with log1p target transformation.
Stratified train/test split by useful vote bucket (0, 1-5, 6-20, 21+).

Run: python predictive_model.py
"""

import os
import sys
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from neo4j import GraphDatabase
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder

# ── Connection settings ──────────────────────────────────────────────────────
NEO4J_URI  = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "testpassword")
MONGO_URI  = "mongodb://localhost:27017/"
MONGO_DB   = "yelp_db"
OUT        = "Report"
SAMPLE_N   = 100_000   # cap reviews for speed; increase if RAM allows

os.makedirs(OUT, exist_ok=True)

def get_mongo():
    return MongoClient(MONGO_URI)[MONGO_DB]

def get_neo4j():
    return GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Feature Extraction
# ═══════════════════════════════════════════════════════════════════════════════
def extract_features():
    print("\n[1] Extracting features...")
    db = get_mongo()

    # ── (a) Review-level + (b) User-level features from MongoDB
    print("  Pulling review + user data from MongoDB...")
    pipeline = [
        {"$lookup": {
            "from": "users",
            "localField": "user_id",
            "foreignField": "user_id",
            "as": "user_info"
        }},
        {"$unwind": "$user_info"},
        {"$addFields": {
            "text_length":  {"$strLenCP": {"$ifNull": ["$text", ""]}},
            "word_count":   {"$size": {"$split": [{"$ifNull": ["$text", ""]}, " "]}},
            "review_year":  {"$toInt": {"$substr": ["$date", 0, 4]}},
            "review_month": {"$toInt": {"$substr": ["$date", 5, 2]}},
            "user_tenure_years": {
                "$divide": [
                    {"$subtract": [
                        {"$toDate": "$date"},
                        {"$toDate": "$user_info.yelping_since"}
                    ]},
                    31_536_000_000   # ms in a year
                ]
            },
            "is_elite": {
                "$cond": [
                    {"$gt": [{"$strLenCP": {"$ifNull": ["$user_info.elite", ""]}}, 0]},
                    1, 0
                ]
            }
        }},
        {"$project": {
            "_id": 0,
            "review_id":          1,
            "user_id":            1,
            "business_id":        1,
            "useful":             1,
            # (a) Review-level
            "stars":              1,
            "text_length":        1,
            "word_count":         1,
            "funny":              1,
            "cool":               1,
            "review_year":        1,
            "review_month":       1,
            # (b) User-level
            "user_review_count":  "$user_info.review_count",
            "user_avg_stars":     "$user_info.average_stars",
            "user_useful_total":  "$user_info.useful",
            "user_fans":          "$user_info.fans",
            "is_elite":           1,
            "user_tenure_years":  1,
        }},
        {"$limit": SAMPLE_N}
    ]

    results = list(db.reviews.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(results)
    print(f"  Reviews loaded: {len(df)}")

    # ── (c) Graph-derived features from Neo4j
    # Requires Q1 (pagerank_score) and Q2 (louvain_community) to have been run first
    print("  Pulling graph features from Neo4j (pagerank_score, louvain_community)...")
    drv = get_neo4j()
    with drv.session() as s:
        # User-level graph features
        graph_rows = list(s.run("""
            MATCH (u:User)
            WHERE u.pagerank_score IS NOT NULL
            RETURN u.user_id                        AS user_id,
                   coalesce(u.pagerank_score, 0.0)  AS user_pagerank,
                   coalesce(u.louvain_community, -1) AS user_community,
                   size([(u)-[:KNOWS]->(f) | f])    AS user_degree
        """))
        # Business-level graph features
        biz_graph_rows = list(s.run("""
            MATCH (b:Business)
            WHERE b.pagerank_score IS NOT NULL
            RETURN b.business_id                       AS business_id,
                   coalesce(b.pagerank_score, 0.0)     AS biz_pagerank
        """))
    drv.close()

    df_graph_user = pd.DataFrame([dict(r) for r in graph_rows])
    df_graph_biz  = pd.DataFrame([dict(r) for r in biz_graph_rows])

    if not df_graph_user.empty:
        df = df.merge(df_graph_user, on="user_id", how="left")
        print(f"  Graph user features merged ({len(df_graph_user)} users with PageRank)")
    else:
        print("  WARNING: No graph features found. Run neo4j_gds.py first to compute PageRank + Louvain.")
        df["user_pagerank"]  = 0.0
        df["user_community"] = -1
        df["user_degree"]    = 0

    if not df_graph_biz.empty:
        df = df.merge(df_graph_biz, on="business_id", how="left")
    else:
        df["biz_pagerank"] = 0.0

    # ── Fill NAs and type-cast
    for col in ["user_pagerank", "biz_pagerank", "user_community", "user_degree",
                "user_useful_total", "user_fans", "user_tenure_years",
                "funny", "cool", "is_elite"]:
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

    print(f"  Feature matrix shape: {df.shape}")
    print(f"  Useful vote distribution:\n{df['useful'].describe().round(2)}")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Model Training & Evaluation
# ═══════════════════════════════════════════════════════════════════════════════
def useful_bucket(v):
    if v == 0: return "0"
    if v <= 5: return "1-5"
    if v <= 20: return "6-20"
    return "21+"

def train_and_evaluate(df):
    print("\n[2] Training model...")

    # Feature columns
    FEATURE_COLS = [
        # (a) Review-level
        "stars", "text_length", "word_count", "funny", "cool",
        "review_year", "review_month",
        # (b) User-level
        "user_review_count", "user_avg_stars", "user_useful_total",
        "user_fans", "is_elite", "user_tenure_years",
        # (c) Graph-derived (from Neo4j GDS Q1 + Q2)
        "user_pagerank", "user_community", "user_degree", "biz_pagerank",
    ]
    # Keep only columns that exist
    FEATURE_COLS = [c for c in FEATURE_COLS if c in df.columns]

    TARGET = "useful"

    df_clean = df[FEATURE_COLS + [TARGET]].copy()
    for c in FEATURE_COLS:
        df_clean[c] = pd.to_numeric(df_clean[c], errors="coerce").fillna(0)
    df_clean[TARGET] = pd.to_numeric(df_clean[TARGET], errors="coerce").fillna(0).clip(lower=0)

    # Stratify by useful vote bucket
    df_clean["bucket"] = df_clean[TARGET].apply(useful_bucket)
    print(f"  Bucket distribution:\n{df_clean['bucket'].value_counts().to_string()}")

    X = df_clean[FEATURE_COLS].values
    y = df_clean[TARGET].values
    buckets = df_clean["bucket"].values

    # Log1p transform to handle heavy right skew
    y_log = np.log1p(y)

    # Stratified split (stratify by bucket)
    X_train, X_test, y_log_train, y_log_test, y_train_raw, y_test_raw, buckets_train, buckets_test = \
        train_test_split(X, y_log, y, buckets, test_size=0.2, random_state=42, stratify=buckets)

    print(f"  Train: {len(X_train)}  |  Test: {len(X_test)}")

    # Train RandomForest (good with skewed targets, gives importances)
    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=12,
        min_samples_leaf=5,
        n_jobs=-1,
        random_state=42,
    )
    model.fit(X_train, y_log_train)

    # Predict (back-transform with expm1)
    y_pred_log  = model.predict(X_test)
    y_pred      = np.expm1(y_pred_log).clip(min=0)

    def metrics(y_true, y_pred):
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        mae  = mean_absolute_error(y_true, y_pred)
        r2   = r2_score(y_true, y_pred)
        return {"RMSE": round(rmse, 4), "MAE": round(mae, 4), "R2": round(r2, 4)}

    overall = metrics(y_test_raw, y_pred)
    print(f"\n  Overall test metrics: {overall}")

    # Per-bucket metrics
    bucket_metrics = {}
    for bkt in ["0", "1-5", "6-20", "21+"]:
        mask = buckets_test == bkt
        if mask.sum() >= 5:
            bm = metrics(y_test_raw[mask], y_pred[mask])
            bucket_metrics[bkt] = bm
            print(f"  Bucket {bkt:4s}: n={mask.sum():5d}  {bm}")

    # Feature importances
    importances = pd.Series(model.feature_importances_, index=FEATURE_COLS).sort_values(ascending=False)
    print(f"\n  Top-5 features:\n{importances.head(5).to_string()}")

    # Save metrics
    pd.DataFrame([{"scope": "overall", **overall}] +
                 [{"scope": k, **v} for k, v in bucket_metrics.items()]
                 ).to_csv(f"{OUT}/PM_Metrics.csv", index=False)

    importances.reset_index().rename(columns={"index": "feature", 0: "importance"}).to_csv(
        f"{OUT}/PM_FeatureImportance.csv", index=False)

    return model, FEATURE_COLS, importances, overall, bucket_metrics, \
           y_test_raw, y_pred, buckets_test


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Visualisations
# ═══════════════════════════════════════════════════════════════════════════════
def visualise(df, model, feature_cols, importances, overall, bucket_metrics,
              y_test, y_pred, buckets_test):
    print("\n[3] Generating visualisations...")

    # ── Fig 1: Useful vote distribution (log scale)
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    bins = [0, 1, 2, 5, 10, 20, 50, 200]
    df["useful_clipped"] = df["useful"].clip(upper=50)
    axes[0].hist(df["useful_clipped"], bins=50, color="steelblue", alpha=0.85)
    axes[0].set_xlabel("Useful Votes (clipped at 50)")
    axes[0].set_ylabel("Review Count")
    axes[0].set_title("Distribution of Useful Votes")
    axes[0].set_yscale("log")

    buckets_order = ["0", "1-5", "6-20", "21+"]
    bucket_counts = df["useful"].apply(useful_bucket).value_counts().reindex(buckets_order, fill_value=0)
    axes[1].bar(buckets_order, bucket_counts.values, color=["#2196F3","#4CAF50","#FF9800","#F44336"], alpha=0.85)
    axes[1].set_xlabel("Useful Vote Bucket")
    axes[1].set_ylabel("Review Count")
    axes[1].set_title("Reviews per Useful-Vote Bucket")
    for i, v in enumerate(bucket_counts.values):
        axes[1].text(i, v + 200, f"{v:,}", ha="center", fontsize=9)

    plt.tight_layout()
    plt.savefig(f"{OUT}/PM_UsefulDistribution.png", dpi=150, bbox_inches="tight")
    plt.close()

    # ── Fig 2: Feature importances
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ["#1565C0" if f in ["user_pagerank", "user_community", "user_degree", "biz_pagerank"]
              else "#388E3C" if f in ["user_review_count", "user_avg_stars", "user_useful_total",
                                      "user_fans", "is_elite", "user_tenure_years"]
              else "#E65100"
              for f in importances.index]
    importances.plot(kind="bar", ax=ax, color=colors, alpha=0.85)
    ax.set_title("Feature Importances (Random Forest)\n"
                 "🔵 Graph-derived  🟢 User-level  🟠 Review-level")
    ax.set_ylabel("Importance Score")
    ax.tick_params(axis="x", rotation=45)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color="#1565C0", label="Graph-derived"),
                        Patch(color="#388E3C", label="User-level"),
                        Patch(color="#E65100", label="Review-level")], fontsize=9)
    plt.tight_layout()
    plt.savefig(f"{OUT}/PM_FeatureImportances.png", dpi=150, bbox_inches="tight")
    plt.close()

    # ── Fig 3: Predicted vs Actual scatter + residuals
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    y_plot = np.clip(y_test, 0, 30)
    p_plot = np.clip(y_pred, 0, 30)
    ax1.scatter(y_plot, p_plot, alpha=0.15, s=5, color="steelblue")
    ax1.plot([0, 30], [0, 30], "r--", lw=1.5, label="Perfect")
    ax1.set_xlabel("Actual Useful Votes")
    ax1.set_ylabel("Predicted Useful Votes")
    ax1.set_title(f"Predicted vs Actual  (R² = {overall['R2']:.3f})")
    ax1.legend(fontsize=9)

    residuals = y_pred - y_test
    ax2.hist(np.clip(residuals, -20, 20), bins=60, color="coral", alpha=0.8)
    ax2.axvline(0, color="k", lw=1.5)
    ax2.set_xlabel("Residual (Predicted − Actual, clipped ±20)")
    ax2.set_ylabel("Count")
    ax2.set_title(f"Residual Distribution  (RMSE={overall['RMSE']}, MAE={overall['MAE']})")

    plt.tight_layout()
    plt.savefig(f"{OUT}/PM_PredictedVsActual.png", dpi=150, bbox_inches="tight")
    plt.close()

    # ── Fig 4: Per-bucket RMSE / MAE
    if bucket_metrics:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 5))
        bkts  = list(bucket_metrics.keys())
        rmses = [bucket_metrics[b]["RMSE"] for b in bkts]
        maes  = [bucket_metrics[b]["MAE"]  for b in bkts]
        r2s   = [bucket_metrics[b]["R2"]   for b in bkts]

        ax1.bar(bkts, rmses, color="#F44336", alpha=0.8, label="RMSE")
        ax1.bar(bkts, maes,  color="#FF9800", alpha=0.8, label="MAE", bottom=0)
        ax1.set_xlabel("Useful Vote Bucket")
        ax1.set_ylabel("Error")
        ax1.set_title("RMSE and MAE per Vote Bucket")
        ax1.legend()

        ax2.bar(bkts, r2s, color="#4CAF50", alpha=0.8)
        ax2.axhline(0, color="k", lw=1)
        ax2.set_xlabel("Useful Vote Bucket")
        ax2.set_ylabel("R²")
        ax2.set_title("R² per Vote Bucket")

        plt.tight_layout()
        plt.savefig(f"{OUT}/PM_BucketMetrics.png", dpi=150, bbox_inches="tight")
        plt.close()

    print(f"  → PM_UsefulDistribution.png  PM_FeatureImportances.png")
    print(f"     PM_PredictedVsActual.png   PM_BucketMetrics.png")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    df = extract_features()
    result = train_and_evaluate(df)
    model, feature_cols, importances, overall, bucket_metrics, \
        y_test, y_pred, buckets_test = result
    visualise(df, model, feature_cols, importances, overall, bucket_metrics,
              y_test, y_pred, buckets_test)
    print(f"\n✓ Predictive modelling complete. Results saved to {OUT}/")
