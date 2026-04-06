import nbformat as nbf
import os

def main():
    nb = nbf.v4.new_notebook()

    md_intro = """# Assignment 2 Part 2: MongoDB Querying
This notebook contains the complete pipelines and execution logic for the MongoDB queries section of the assignment.
"""
    nb.cells.append(nbf.v4.new_markdown_cell(md_intro))

    # Cell 1: Setup
    code_setup = """from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

client = MongoClient("mongodb://localhost:27017/")
db = client["yelp_db"]
"""
    nb.cells.append(nbf.v4.new_code_cell(code_setup))

    # Cell 2: Query 1
    md_q1 = """## 1. Cohort analysis of user reviewing behaviour"""
    nb.cells.append(nbf.v4.new_markdown_cell(md_q1))

    code_q1 = """q1_pipeline = [
    {'$lookup': {'from': 'users', 'localField': 'user_id', 'foreignField': 'user_id', 'as': 'user_info'}},
    {'$unwind': '$user_info'},
    {'$addFields': {'cohort_year': {'$substr': ['$user_info.yelping_since', 0, 4]}, 'text_length': {'$strLenCP': {'$ifNull': ['$text', '']}}}},
    {'$group': {
        '_id': '$cohort_year', 
        'mean_star_rating': {'$avg': '$stars'}, 
        'stddev_star_rating': {'$stdDevSamp': '$stars'}, 
        'mean_text_length': {'$avg': '$text_length'}, 
        'mean_useful_votes': {'$avg': '$useful'}, 
        'count': {'$sum': 1}, 
        'star_1': {'$sum': {'$cond': [{'$eq': ['$stars', 1.0]}, 1, 0]}}, 
        'star_2': {'$sum': {'$cond': [{'$eq': ['$stars', 2.0]}, 1, 0]}}, 
        'star_3': {'$sum': {'$cond': [{'$eq': ['$stars', 3.0]}, 1, 0]}}, 
        'star_4': {'$sum': {'$cond': [{'$eq': ['$stars', 4.0]}, 1, 0]}}, 
        'star_5': {'$sum': {'$cond': [{'$eq': ['$stars', 5.0]}, 1, 0]}}
    }},
    {'$project': {
        'cohort': '$_id', '_id': 0, 'mean_star_rating': 1, 'stddev_star_rating': 1, 'mean_text_length': 1, 'mean_useful_votes': 1,
        'prop_star_1': {'$divide': ['$star_1', '$count']}, 'prop_star_2': {'$divide': ['$star_2', '$count']},
        'prop_star_3': {'$divide': ['$star_3', '$count']}, 'prop_star_4': {'$divide': ['$star_4', '$count']},
        'prop_star_5': {'$divide': ['$star_5', '$count']}, 'count': 1
    }},
    {'$sort': {'cohort': 1}}
]
res1 = list(db.reviews.aggregate(q1_pipeline, allowDiskUse=True))
df_q1 = pd.DataFrame(res1)
df_q1
"""
    nb.cells.append(nbf.v4.new_code_cell(code_q1))

    # Cell 3: Interpret Q1
    code_q1_ans = """if not df_q1.empty:
    max_stars_cohort = df_q1.loc[df_q1['mean_star_rating'].idxmax()]['cohort']
    max_useful_cohort = df_q1.loc[df_q1['mean_useful_votes'].idxmax()]['cohort']
    print(f"Highest mean star rating cohort: {max_stars_cohort}")
    print(f"Highest mean useful votes cohort: {max_useful_cohort}")
"""
    nb.cells.append(nbf.v4.new_code_cell(code_q1_ans))

    # Cell 4: Query 2
    md_q2 = """## 2. Month-over-month trend consistency for large business categories"""
    nb.cells.append(nbf.v4.new_markdown_cell(md_q2))

    code_q2 = """q2_pipeline = [
    {'$lookup': {'from': 'businesses', 'localField': 'business_id', 'foreignField': 'business_id', 'as': 'business'}},
    {'$unwind': '$business'},
    {'$project': {'stars': 1, 'date': 1, 'categories': {'$split': [{'$ifNull': ['$business.categories', '']}, ', ']}}},
    {'$unwind': '$categories'},
    {'$match': {'categories': {'$ne': ''}}},
    {'$addFields': {'month': {'$substr': ['$date', 0, 7]}}},
    {'$group': {'_id': {'category': '$categories', 'month': '$month'}, 'avg_stars': {'$avg': '$stars'}, 'review_count': {'$sum': 1}}},
    {'$sort': {'_id.month': 1}},
    {'$group': {'_id': '$_id.category', 'months': {'$push': {'month': '$_id.month', 'avg_stars': '$avg_stars'}}, 'total_reviews': {'$sum': '$review_count'}}},
    {'$match': {'total_reviews': {'$gte': 500}}}
]
res2 = list(db.reviews.aggregate(q2_pipeline, allowDiskUse=True))

q2_results = []
for doc in res2:
    months_data = doc['months']
    inc_count, dec_count = 0, 0
    total_pairs = len(months_data) - 1
    if total_pairs > 0:
        for i in range(1, len(months_data)):
            diff = months_data[i]['avg_stars'] - months_data[i-1]['avg_stars']
            if diff > 0: inc_count += 1
            elif diff < 0: dec_count += 1
        q2_results.append({
            'category': doc['_id'], 'total_reviews': doc['total_reviews'],
            'increase_consistency': inc_count / total_pairs, 'decrease_consistency': dec_count / total_pairs
        })
df_q2 = pd.DataFrame(q2_results)

if not df_q2.empty:
    print("Top 3 Upward Trends:\\n", df_q2.nlargest(3, 'increase_consistency')[['category', 'increase_consistency']])
    print("\\nTop 3 Downward Trends:\\n", df_q2.nlargest(3, 'decrease_consistency')[['category', 'decrease_consistency']])
"""
    nb.cells.append(nbf.v4.new_code_cell(code_q2))

    # Cell 6: Query 3
    md_q3 = """## 3. Check-ins vs Ratings and Tips for Top Categories"""
    nb.cells.append(nbf.v4.new_markdown_cell(md_q3))

    code_q3 = """# Compute check-in classes and top 10 categories
b_pipeline = [{'$project': {'business_id': 1, 'categories': {'$split': [{'$ifNull': ['$categories', '']}, ', ']}, 'checkin_count': {'$size': {'$ifNull': ['$checkins', []]}}}}]
df_b = pd.DataFrame(list(db.businesses.aggregate(b_pipeline)))

if not df_b.empty:
    q1, q3 = df_b['checkin_count'].quantile(0.25), df_b['checkin_count'].quantile(0.75)
    df_b['checkin_class'] = df_b['checkin_count'].apply(lambda x: 'low' if x <= q1 else ('high' if x > q3 else 'medium'))
    
    cat_counts = {}
    for cats in df_b['categories'].dropna():
        for c in cats: cat_counts[c] = cat_counts.get(c, 0) + 1
    top_10_cats = [c[0] for c in sorted(cat_counts.items(), key=lambda x: x[1], reverse=True)[:10]]
    
    tips_map = {d['_id']: d['count'] for d in db.tips.aggregate([{'$group': {'_id': '$business_id', 'count': {'$sum': 1}}}])}
    rev_map = {d['_id']: {'avg': d['avg_stars'], 'cnt': d['count']} for d in db.reviews.aggregate([{'$group': {'_id': '$business_id', 'avg_stars': {'$avg': '$stars'}, 'count': {'$sum': 1}}}])}
    
    results_q3 = []
    for cat in top_10_cats:
        for cls in ['low', 'medium', 'high']:
            b_in_cat = df_b[(df_b['checkin_class'] == cls) & (df_b['categories'].apply(lambda x: isinstance(x, list) and cat in x))]
            if len(b_in_cat) > 0:
                sum_stars = sum([rev_map.get(b, {'avg': 0})['avg'] for b in b_in_cat['business_id']])
                sum_revs = sum([rev_map.get(b, {'cnt': 0})['cnt'] for b in b_in_cat['business_id']])
                sum_tips = sum([tips_map.get(b, 0) for b in b_in_cat['business_id']])
                total_b = len(b_in_cat)
                results_q3.append({
                    'category': cat, 'checkin_class': cls, 'mean_star': sum_stars/total_b,
                    'mean_rev': sum_revs/total_b, 'tip_rev_ratio': sum_tips/sum_revs if sum_revs > 0 else 0
                })

    df_q3 = pd.DataFrame(results_q3)
    display(df_q3.pivot(index='category', columns='checkin_class', values=['mean_star', 'mean_rev', 'tip_rev_ratio']))
"""
    nb.cells.append(nbf.v4.new_code_cell(code_q3))

    with open('MongoDB_Queries.ipynb', 'w') as f:
        nbf.write(nb, f)
    print("MongoDB_Queries.ipynb generated successfully!")

if __name__ == "__main__":
    main()
