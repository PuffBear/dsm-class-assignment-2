import os
import json
from pymongo import MongoClient

def main():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["yelp_db"]

    print("--- User doc ---")
    print(db.users.find_one())
    print("\n--- Business doc ---")
    print(db.businesses.find_one())
    
    # Query 1 structure check
    pipeline1 = [
        {'$limit': 100},
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
                'cohort_year': {
                    '$substr': ['$user_info.yelping_since', 0, 4]
                }, 
                'text_length': {
                    '$strLenCP': {'$ifNull': ['$text', '']}
                }
            }
        }, {
            '$group': {
                '_id': '$cohort_year', 
                'avg_stars': {'$avg': '$stars'},
                'count': {'$sum': 1}
            }
        }
    ]
    
    print("\n--- Test Query 1 (Limit 100) ---")
    for doc in db.reviews.aggregate(pipeline1):
        print(doc)

if __name__ == "__main__":
    main()
