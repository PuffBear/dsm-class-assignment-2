import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Make Report directory
os.makedirs("Report", exist_ok=True)

def main():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["yelp_db"]
    
    # ----------------------------------------------------
    # Query 1: Cohort analysis
    # ----------------------------------------------------
    print("Executing Query 1...")
    q1_pipeline = [
        {
            '$lookup': {
                'from': 'users', 
                'localField': 'user_id', 
                'foreignField': 'user_id', 
                'as': 'user_info'
            }
        }, {
            '$unwind': '$user_info'
        }, {
            '$addFields': {
                'cohort_year': {'$substr': ['$user_info.yelping_since', 0, 4]}, 
                'text_length': {'$strLenCP': {'$ifNull': ['$text', '']}}
            }
        }, {
            '$group': {
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
            }
        }, {
            '$project': {
                'cohort': '$_id',
                '_id': 0,
                'mean_star_rating': 1,
                'stddev_star_rating': 1,
                'mean_text_length': 1,
                'mean_useful_votes': 1,
                'prop_star_1': {'$divide': ['$star_1', '$count']},
                'prop_star_2': {'$divide': ['$star_2', '$count']},
                'prop_star_3': {'$divide': ['$star_3', '$count']},
                'prop_star_4': {'$divide': ['$star_4', '$count']},
                'prop_star_5': {'$divide': ['$star_5', '$count']},
                'count': 1
            }
        }, {
            '$sort': {'cohort': 1}
        }
    ]
    
    # Run the query and store in dataframe
    res = list(db.reviews.aggregate(q1_pipeline, allowDiskUse=True))
    df_q1 = pd.DataFrame(res)
    df_q1.to_csv("Report/Query1_Cohorts.csv", index=False)
    print("Query 1 saved!")

    # Verify output 1
    if len(df_q1) > 0:
        max_stars_cohort = df_q1.loc[df_q1['mean_star_rating'].idxmax()]['cohort']
        max_useful_cohort = df_q1.loc[df_q1['mean_useful_votes'].idxmax()]['cohort']
        print(f"Highest mean star rating cohort: {max_stars_cohort}")
        print(f"Highest mean useful votes cohort: {max_useful_cohort}")
    else:
        print("Query 1 returned NO results.")
    
    # ----------------------------------------------------
    # Query 2: Month-over-month trend
    # ----------------------------------------------------
    print("Executing Query 2...")
    q2_pipeline = [
        {
            '$lookup': {
                'from': 'businesses', 
                'localField': 'business_id', 
                'foreignField': 'business_id', 
                'as': 'business'
            }
        }, {
            '$unwind': '$business'
        }, {
            '$project': {
                'stars': 1, 
                'date': 1, 
                'categories': {
                    '$split': [{'$ifNull': ['$business.categories', '']}, ', ']
                }
            }
        }, {
            '$unwind': '$categories'
        }, {
            '$match': {'categories': {'$ne': ''}}
        }, {
            '$addFields': {
                'month': {'$substr': ['$date', 0, 7]}
            }
        }, {
            '$group': {
                '_id': {'category': '$categories', 'month': '$month'}, 
                'avg_stars': {'$avg': '$stars'}, 
                'review_count': {'$sum': 1}
            }
        }, {
            '$sort': {'_id.month': 1}
        }, {
            '$group': {
                '_id': '$_id.category', 
                'months': {
                    '$push': {'month': '$_id.month', 'avg_stars': '$avg_stars'}
                }, 
                'total_reviews': {'$sum': '$review_count'}
            }
        }, {
            '$match': {'total_reviews': {'$gte': 500}}
        }
    ]
    
    res2 = list(db.reviews.aggregate(q2_pipeline, allowDiskUse=True))
    q2_results_list = []
    
    for doc in res2:
        cat = doc['_id']
        total_rev = doc['total_reviews']
        months_data = doc['months']
        # Compute consistency
        inc_count = 0
        dec_count = 0
        total_pairs = len(months_data) - 1
        
        if total_pairs > 0:
            for i in range(1, len(months_data)):
                diff = months_data[i]['avg_stars'] - months_data[i-1]['avg_stars']
                if diff > 0:
                    inc_count += 1
                elif diff < 0:
                    dec_count += 1
            
            inc_prop = inc_count / total_pairs
            dec_prop = dec_count / total_pairs
        else:
            inc_prop = 0.0
            dec_prop = 0.0
            
        q2_results_list.append({
            'category': cat,
            'total_reviews': total_rev,
            'increase_consistency': inc_prop,
            'decrease_consistency': dec_prop
        })
        
    df_q2 = pd.DataFrame(q2_results_list)
    df_q2.to_csv("Report/Query2_Trends.csv", index=False)
    
    if len(df_q2) > 0:
        top_inc = df_q2.nlargest(3, 'increase_consistency')
        top_dec = df_q2.nlargest(3, 'decrease_consistency')
        print("Top 3 Upward Trends:\n", top_inc[['category', 'increase_consistency']])
        print("Top 3 Downward Trends:\n", top_dec[['category', 'decrease_consistency']])
    else:
        print("Query 2 returned NO results.")

    # ----------------------------------------------------
    # Query 3: Check-in quartiles
    # ----------------------------------------------------
    print("Executing Query 3...")
    # First, calculate quartiles for check-in counts
    # The checkins are inside businesses as an array
    b_pipeline = [
        {
            '$project': {
                'business_id': 1,
                'categories': {'$split': [{'$ifNull': ['$categories', '']}, ', ']},
                'checkin_count': {'$size': {'$ifNull': ['$checkins', []]}}
            }
        }
    ]
    businesses = list(db.businesses.aggregate(b_pipeline))
    df_b = pd.DataFrame(businesses)
    
    if len(df_b) > 0:
        q1 = df_b['checkin_count'].quantile(0.25)
        q3 = df_b['checkin_count'].quantile(0.75)
        
        def assign_class(row):
            count = row['checkin_count']
            if count <= q1: return 'low'
            elif count > q3: return 'high'
            else: return 'medium'
            
        df_b['checkin_class'] = df_b.apply(assign_class, axis=1)
        
        # Now find top 10 categories
        cat_counts = {}
        for cats in df_b['categories'].dropna():
            for c in cats:
                cat_counts[c] = cat_counts.get(c, 0) + 1
        top_10_cats = sorted(cat_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        top_10_cats = [c[0] for c in top_10_cats]
        print(f"Top 10 Categories (by total business count): {top_10_cats}")
        
        # Filter to only top 10 categories
        # Then we need to compute mean star rating, mean review count, ratio of tips
        # Actually, "mean star rating, mean review count" per business
        # Let's join businesses, reviews, and tips OR fetch them separately since data fits in memory
        
        # We can just update businesses with their class in mongo, or do the aggregation
        # Since we have df_b in memory, let's create a map: business_id -> class
        b_class_map = dict(zip(df_b['business_id'], df_b['checkin_class']))
        
        # Let's calculate tips ratio. We need tip counts per business.
        # aggregate tips:
        tips_counts = list(db.tips.aggregate([{'$group': {'_id': '$business_id', 'count': {'$sum': 1}}}]))
        tips_map = {d['_id']: d['count'] for d in tips_counts}
        
        # Aggregate reviews:
        rev_counts = list(db.reviews.aggregate([{'$group': {'_id': '$business_id', 'avg_stars': {'$avg': '$stars'}, 'count': {'$sum': 1}}}]))
        rev_map = {d['_id']: {'avg_stars': d['avg_stars'], 'count': d['count']} for d in rev_counts}
        
        results_q3 = []
        # Populate each category and class combination
        for cat in top_10_cats:
            for cls in ['low', 'medium', 'high']:
                # Find businesses in this category and class
                # Note: A business can be in multiple categories. The df_b has categories as list.
                b_in_cat_cls = df_b[(df_b['checkin_class'] == cls) & (df_b['categories'].apply(lambda x: isinstance(x, list) and cat in x))]
                
                sum_stars = 0
                sum_reviews = 0
                sum_tips = 0
                total_b = len(b_in_cat_cls)
                
                if total_b > 0:
                    for bid in b_in_cat_cls['business_id']:
                        r_data = rev_map.get(bid, {'avg_stars': 0, 'count': 0})
                        t_count = tips_map.get(bid, 0)
                        
                        # Use average stars from review_map or from db.businesses?
                        # Assignment: "compute the mean star rating, mean review count"
                        # mean of business' mean ratings
                        sum_stars += r_data['avg_stars']
                        sum_reviews += r_data['count']
                        sum_tips += t_count
                        
                    mean_star = sum_stars / total_b
                    mean_rev = sum_reviews / total_b
                    tip_rev_ratio = sum_tips / sum_reviews if sum_reviews > 0 else 0
                else:
                    mean_star, mean_rev, tip_rev_ratio = 0, 0, 0
                    
                results_q3.append({
                    'category': cat,
                    'checkin_class': cls,
                    'mean_star_rating': mean_star,
                    'mean_review_count': mean_rev,
                    'tip_to_review_ratio': tip_rev_ratio
                })
                
        df_q3 = pd.DataFrame(results_q3)
        df_q3.to_csv("Report/Query3_Checkin_CrossTab.csv", index=False)
        print("Query 3 crosstab generated!")
    else:
        print("No businesses found.")
        
    print("Done!")

if __name__ == "__main__":
    main()
