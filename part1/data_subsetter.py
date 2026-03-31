import json
import os
from tqdm import tqdm

DATA_DIR = './Yelp JSON/yelp_dataset/'
OUT_DIR = './subset/'
CITIES = {'Philadelphia', 'Tucson', 'Tampa', 'Indianapolis', 'Nashville'}

def main():
    if not os.path.exists(OUT_DIR):
        os.makedirs(OUT_DIR)
        
    print(f"Subsetting data for cities: {CITIES}")
    
    # 1. Businesses
    bids = set()
    print("Processing businesses...")
    with open(f"{DATA_DIR}yelp_academic_dataset_business.json", "r") as f_in, \
         open(f"{OUT_DIR}business.json", "w") as f_out:
        for line in f_in:
            bus = json.loads(line)
            if bus.get("city") in CITIES:
                bids.add(bus["business_id"])
                f_out.write(line)
    print(f"Total businesses kept: {len(bids)}")
    
    # 2. Reviews
    uids = set()
    print("Processing reviews...")
    with open(f"{DATA_DIR}yelp_academic_dataset_review.json", "r") as f_in, \
         open(f"{OUT_DIR}review.json", "w") as f_out:
        for line in f_in:
            rev = json.loads(line)
            if rev["business_id"] in bids:
                uids.add(rev["user_id"])
                f_out.write(line)
    print(f"Total reviews kept...")
    
    # 3. Users
    uids_kept = 0
    print("Processing users...")
    with open(f"{DATA_DIR}yelp_academic_dataset_user.json", "r") as f_in, \
         open(f"{OUT_DIR}user.json", "w") as f_out:
        for line in f_in:
            usr = json.loads(line)
            if usr["user_id"] in uids:
                uids_kept += 1
                f_out.write(line)
    print(f"Total users kept: {uids_kept}")
    
    # 4. Tips
    tips_kept = 0
    print("Processing tips...")
    with open(f"{DATA_DIR}yelp_academic_dataset_tip.json", "r") as f_in, \
         open(f"{OUT_DIR}tip.json", "w") as f_out:
        for line in f_in:
            tip = json.loads(line)
            if tip["business_id"] in bids:
                tips_kept += 1
                f_out.write(line)
    print(f"Total tips kept: {tips_kept}")
    
    # 5. Checkins
    checkins_kept = 0
    print("Processing checkins...")
    with open(f"{DATA_DIR}yelp_academic_dataset_checkin.json", "r") as f_in, \
         open(f"{OUT_DIR}checkin.json", "w") as f_out:
        for line in f_in:
            chk = json.loads(line)
            if chk["business_id"] in bids:
                checkins_kept += 1
                f_out.write(line)
    print(f"Total checkins kept: {checkins_kept}")
    
    print("Subsetting completed successfully! Data saved to ./subset/")

if __name__ == "__main__":
    main()
