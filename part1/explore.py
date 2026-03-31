import pandas as pd
import json

def main():
    print("Loading businesses...")
    df_bus = pd.read_json('./Yelp JSON/yelp_dataset/yelp_academic_dataset_business.json', lines=True)
    philly_bids = set(df_bus[df_bus['city'] == 'Philadelphia']['business_id'].unique())
    print(f"Total Philadelphia businesses: {len(philly_bids)}")
    
    print("Counting reviews...")
    count = 0
    with open('./Yelp JSON/yelp_dataset/yelp_academic_dataset_review.json', 'r') as f:
        for line in f:
            r = json.loads(line)
            if r['business_id'] in philly_bids:
                count += 1
    
    print(f"Total Philadelphia reviews: {count}")

if __name__ == '__main__':
    main()
