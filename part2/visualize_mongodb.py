#!/usr/bin/env python3
"""
Part 2 — MongoDB Query Visualisations
Reads the three CSVs produced by execute_mongo.py and generates
publication-quality figures for the report.

Run: python visualize_mongodb.py
"""

import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

OUT = "Report"
os.makedirs(OUT, exist_ok=True)

sns.set_theme(style="whitegrid", font_scale=1.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Q1 — Cohort Analysis Visualisations
# ═══════════════════════════════════════════════════════════════════════════════
def plot_q1():
    df = pd.read_csv(f"{OUT}/Query1_Cohorts.csv")
    df = df.sort_values("cohort")
    df["cohort"] = df["cohort"].astype(str)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Q1 — Cohort Analysis of User Reviewing Behaviour", fontsize=14, fontweight="bold")

    # ── (A) Mean star rating per cohort with ±1 SD error band
    ax = axes[0, 0]
    ax.bar(df["cohort"], df["mean_star_rating"], color="steelblue", alpha=0.85, label="Mean Stars")
    ax.errorbar(df["cohort"], df["mean_star_rating"], yerr=df["stddev_star_rating"],
                fmt="none", color="black", capsize=4, lw=1.5)
    ax.set_xlabel("Cohort (Year Joined)")
    ax.set_ylabel("Mean Star Rating")
    ax.set_title("Mean Star Rating ± Std Dev by Cohort")
    ax.tick_params(axis="x", rotation=45)
    ax.set_ylim(3.0, 5.0)
    # Annotate the highest cohort
    max_idx = df["mean_star_rating"].idxmax()
    ax.annotate(f"Highest: {df.loc[max_idx, 'cohort']}",
                xy=(df.loc[max_idx, "cohort"], df.loc[max_idx, "mean_star_rating"]),
                xytext=(0, 10), textcoords="offset points",
                ha="center", fontsize=9, color="red",
                arrowprops=dict(arrowstyle="->", color="red"))

    # ── (B) Mean useful votes per cohort
    ax = axes[0, 1]
    bars = ax.bar(df["cohort"], df["mean_useful_votes"], color="coral", alpha=0.85)
    ax.set_xlabel("Cohort (Year Joined)")
    ax.set_ylabel("Mean Useful Votes per Review")
    ax.set_title("Mean Useful Votes Received by Cohort")
    ax.tick_params(axis="x", rotation=45)
    max_idx2 = df["mean_useful_votes"].idxmax()
    ax.annotate(f"Highest: {df.loc[max_idx2, 'cohort']}",
                xy=(df.loc[max_idx2, "cohort"], df.loc[max_idx2, "mean_useful_votes"]),
                xytext=(0, 10), textcoords="offset points",
                ha="center", fontsize=9, color="darkred",
                arrowprops=dict(arrowstyle="->", color="darkred"))

    # ── (C) Mean review character length per cohort
    ax = axes[1, 0]
    ax.plot(df["cohort"], df["mean_text_length"], marker="o", color="seagreen", lw=2, ms=6)
    ax.fill_between(range(len(df)), df["mean_text_length"], alpha=0.15, color="seagreen")
    ax.set_xticks(range(len(df)))
    ax.set_xticklabels(df["cohort"], rotation=45)
    ax.set_xlabel("Cohort (Year Joined)")
    ax.set_ylabel("Mean Character Length")
    ax.set_title("Mean Review Length by Cohort")

    # ── (D) Stacked proportion of star ratings per cohort
    ax = axes[1, 1]
    prop_cols = ["prop_star_1", "prop_star_2", "prop_star_3", "prop_star_4", "prop_star_5"]
    prop_labels = ["1★", "2★", "3★", "4★", "5★"]
    colors = ["#C62828", "#E53935", "#FFB300", "#43A047", "#1B5E20"]
    bottom = np.zeros(len(df))
    for col, label, color in zip(prop_cols, prop_labels, colors):
        vals = df[col].fillna(0).values
        ax.bar(df["cohort"], vals, bottom=bottom, color=color, alpha=0.9, label=label)
        bottom += vals
    ax.set_xlabel("Cohort (Year Joined)")
    ax.set_ylabel("Proportion")
    ax.set_title("Star Rating Proportions by Cohort")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(loc="upper right", fontsize=8, title="Stars")
    ax.set_ylim(0, 1)

    plt.tight_layout()
    plt.savefig(f"{OUT}/MQ1_CohortAnalysis.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  → MQ1_CohortAnalysis.png")


# ═══════════════════════════════════════════════════════════════════════════════
# Q2 — Month-over-Month Trend Visualisations
# ═══════════════════════════════════════════════════════════════════════════════
def plot_q2():
    df = pd.read_csv(f"{OUT}/Query2_Trends.csv")

    top3_up   = df.nlargest(3, "increase_consistency")
    top3_down = df.nlargest(3, "decrease_consistency")
    top12     = pd.concat([top3_up, top3_down]).drop_duplicates("category")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Q2 — Month-over-Month Trend Consistency (Categories ≥500 Reviews)", fontsize=13, fontweight="bold")

    # ── (A) Top 3 upward + Top 3 downward trend consistency
    ax = axes[0]
    up_cols   = ["#1B5E20", "#2E7D32", "#388E3C"]
    down_cols = ["#B71C1C", "#C62828", "#D32F2F"]

    y_pos = range(len(top12))
    for i, (_, row) in enumerate(top12.iterrows()):
        color = up_cols[i] if i < 3 else down_cols[i - 3]
        ax.barh(i, row["increase_consistency"], color=color, alpha=0.85,
                label=("Upward" if i == 0 else ("Downward" if i == 3 else "_nolegend_")))
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(top12["category"].str[:35].values)
    ax.set_xlabel("Trend Consistency (proportion of month-pairs with increase)")
    ax.set_title("Top 3 Upward (green) & Top 3 Downward (red) Trend Categories")
    ax.axvline(0.5, color="grey", lw=1, linestyle="--", label="0.5 baseline")
    ax.legend(fontsize=9)
    ax.invert_yaxis()

    # ── (B) Scatter: increase vs decrease consistency for all categories
    ax = axes[1]
    sc = ax.scatter(df["increase_consistency"], df["decrease_consistency"],
                    c=np.log1p(df["total_reviews"]),
                    cmap="YlOrRd", s=40, alpha=0.7, edgecolors="k", lw=0.3)
    plt.colorbar(sc, ax=ax, label="log(total_reviews)")

    # Annotate the 3 most consistent upward categories
    for _, row in top3_up.iterrows():
        ax.annotate(row["category"][:20], (row["increase_consistency"], row["decrease_consistency"]),
                    fontsize=7.5, xytext=(4, 2), textcoords="offset points", color="darkgreen")
    for _, row in top3_down.iterrows():
        ax.annotate(row["category"][:20], (row["increase_consistency"], row["decrease_consistency"]),
                    fontsize=7.5, xytext=(4, -8), textcoords="offset points", color="darkred")

    ax.set_xlabel("Increase Consistency")
    ax.set_ylabel("Decrease Consistency")
    ax.set_title("Trend Consistency Scatter (all qualifying categories)")
    ax.plot([0, 1], [1, 0], "k--", lw=1, alpha=0.4, label="Sum = 1 line")
    ax.legend(fontsize=8)

    plt.tight_layout()
    plt.savefig(f"{OUT}/MQ2_TrendConsistency.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  → MQ2_TrendConsistency.png")


# ═══════════════════════════════════════════════════════════════════════════════
# Q3 — Check-in Cross-Tabulation Visualisations
# ═══════════════════════════════════════════════════════════════════════════════
def plot_q3():
    df = pd.read_csv(f"{OUT}/Query3_Checkin_CrossTab.csv")

    # ── Pivot to wide format for heatmaps
    df_stars  = df.pivot(index="category", columns="checkin_class", values="mean_star_rating")
    df_reviews = df.pivot(index="category", columns="checkin_class", values="mean_review_count")
    df_ratio  = df.pivot(index="category", columns="checkin_class", values="tip_to_review_ratio")

    # Reorder columns
    col_order = [c for c in ["low", "medium", "high"] if c in df_stars.columns]
    df_stars   = df_stars[col_order]
    df_reviews = df_reviews[col_order]
    df_ratio   = df_ratio[col_order]

    fig, axes = plt.subplots(1, 3, figsize=(18, 7))
    fig.suptitle("Q3 — Check-in Frequency × Business Category Cross-Tabulation", fontsize=13, fontweight="bold")

    def heatmap(data, ax, title, fmt, cmap, vmin=None, vmax=None):
        sns.heatmap(data, ax=ax, annot=True, fmt=fmt, cmap=cmap,
                    linewidths=0.5, linecolor="white",
                    vmin=vmin, vmax=vmax,
                    annot_kws={"size": 9})
        ax.set_title(title, fontsize=11)
        ax.set_xlabel("Check-in Frequency Class", fontsize=10)
        ax.set_ylabel("Business Category", fontsize=10)
        ax.tick_params(axis="y", labelsize=9)

    heatmap(df_stars.round(2), axes[0],
            "Mean Star Rating",
            ".2f", "RdYlGn", vmin=3.0, vmax=4.5)
    heatmap(df_reviews.round(1), axes[1],
            "Mean Review Count per Business",
            ".1f", "Blues")
    heatmap(df_ratio.round(3), axes[2],
            "Tip-to-Review Ratio",
            ".3f", "Purples")

    plt.tight_layout()
    plt.savefig(f"{OUT}/MQ3_CheckinCrossTab.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  → MQ3_CheckinCrossTab.png")

    # ── Supplementary: grouped bar chart for mean stars
    fig, ax = plt.subplots(figsize=(13, 6))
    df_plot = df.copy()
    df_plot["checkin_class"] = pd.Categorical(df_plot["checkin_class"], categories=col_order, ordered=True)
    df_plot = df_plot.sort_values(["category", "checkin_class"])

    categories = df_plot["category"].unique()
    x = np.arange(len(categories))
    w = 0.25
    palette = {"low": "#F44336", "medium": "#FF9800", "high": "#4CAF50"}

    for i, cls in enumerate(col_order):
        vals = [df_plot[(df_plot["category"] == c) & (df_plot["checkin_class"] == cls)]["mean_star_rating"].values
                for c in categories]
        vals = [v[0] if len(v) > 0 else 0 for v in vals]
        ax.bar(x + (i - 1) * w, vals, w, label=cls.capitalize(), color=palette[cls], alpha=0.85)

    ax.set_xticks(x)
    ax.set_xticklabels([c[:20] for c in categories], rotation=35, ha="right", fontsize=9)
    ax.set_ylabel("Mean Star Rating")
    ax.set_title("Mean Star Rating by Category and Check-in Frequency Class")
    ax.legend(title="Check-in Class")
    ax.set_ylim(2.5, 5.0)

    plt.tight_layout()
    plt.savefig(f"{OUT}/MQ3_CheckinBarChart.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  → MQ3_CheckinBarChart.png")


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("[MongoDB Visualisations]")
    plot_q1()
    plot_q2()
    plot_q3()
    print(f"\n✓ All MongoDB figures saved to {OUT}/")
