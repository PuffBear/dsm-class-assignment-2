# CS-3510: Data Science and Management — Assignment 2, Part 1
## NoSQL Database Design and Querying with the Yelp Open Dataset

---

## 1. Data Acquisition and MongoDB Schema Design (20 marks)

### 1.1 Collection Structure (5 marks)

The Yelp Open Dataset was subsetted by filtering for five major U.S. cities: **Philadelphia, Tucson, Tampa, Indianapolis, and Nashville**. This city-based subsetting strategy ensures that all related entities (businesses, reviews, users, tips, checkins) remain interconnected, which is critical for both MongoDB aggregation pipelines and Neo4j graph traversals.

The MongoDB database (`yelp_db`) is organized into **four collections**:

| Collection | Records | Source File(s) |
|---|---|---|
| `businesses` | 47,380 | `business.json` + `checkin.json` (embedded) |
| `users` | 819,169 | `user.json` |
| `reviews` | 2,640,381 | `review.json` |
| `tips` | 347,238 | `tip.json` |

The source dataset contains six JSON files, but only four were mapped to collections. **Checkins** were embedded directly into business documents (as an array of date strings) rather than kept as a standalone collection. **Photos** (`photo.json`) were excluded entirely because no analytical query in this assignment requires photo data, and including them would add storage overhead without value.

### 1.2 Collection Descriptions (5 marks)

**`businesses`** — The core entity collection storing information about each Yelp-listed business. Each document is identified by `business_id` (unique index) and contains attributes such as `name`, `city`, `state`, `stars`, `review_count`, `address`, `latitude`, `longitude`, and `is_open`. The `categories` field stores a comma-separated string of business categories. Two nested sub-documents — `attributes` (e.g., WiFi, parking, price range) and `hours` (operating hours by day of week) — are embedded directly. The `checkins` field is an embedded array of ISO-formatted date strings sourced from `checkin.json`, representing every check-in event for that business. This collection is referenced by `reviews` and `tips` via `business_id`.

**`users`** — Stores Yelp user account data. Each document is identified by `user_id` (unique index) and includes `name`, `review_count`, `yelping_since` (join date), engagement metrics (`useful`, `funny`, `cool` vote counts), `elite` status (comma-separated years), `average_stars`, `fans`, and various compliment counters. The `friends` field is an array of `user_id` strings — a self-referential reference list pointing to other documents in the same collection. This collection is referenced by `reviews` and `tips` via `user_id`.

**`reviews`** — Contains user-written reviews of businesses. Each document is identified by `review_id` and includes `user_id` and `business_id` (both cross-collection references acting as foreign keys), `stars` (1–5 integer rating), vote counts (`useful`, `funny`, `cool`), the full `text` of the review, and `date` (ISO timestamp string). This is the largest collection and serves as the primary analytical join point between users and businesses.

**`tips`** — Stores short-form user tips about businesses — similar to reviews but without star ratings. Each document has no single unique identifier; the combination of `user_id`, `business_id`, and `date` serves as a compound key. It includes `text` (the tip content) and `compliment_count`. Like reviews, it references both `users` and `businesses` via their respective ID fields.

### 1.3 Schema Justifications (10 marks)

#### (a) Embed vs. Reference Decisions

| Relationship | Decision | Justification |
|---|---|---|
| **Business ↔ Checkins** | **Embedded** | Checkins have no independent existence — they are never queried separately from their business. Every analytical use (Query 7) accesses them alongside the business document. The data is bounded: each checkin is just a date string, and even heavily-visited businesses generate a manageable array. Embedding enables single-document reads without joins. |
| **Business ↔ Attributes/Hours** | **Embedded** (sub-documents) | These are descriptive metadata that belong exclusively to a single business. They are always accessed as part of the business document and never queried independently. As nested objects, they naturally map to embedded sub-documents. |
| **User → Friends** | **Array of references** | Friends are full User entities with their own large documents (engagement metrics, compliments, review history). Duplicating entire user documents inside each friend list would create massive redundancy and risk exceeding the 16 MB document limit for well-connected users. Storing an array of `user_id` strings keeps the user document lean while enabling friend-count analytics via `$size`. |
| **Review → Business/User** | **Cross-collection reference** | Reviews are large documents (the `text` field alone averages ~550 characters) and grow unboundedly — a popular business can accumulate thousands of reviews. Embedding them into business documents would make those documents enormous and eventually exceed the 16 MB limit. Reviews are also independently queried (filtered by date, grouped by user, paginated) — a separate collection with indexed foreign keys is the right model. |
| **Tip → Business/User** | **Cross-collection reference** | Same reasoning as reviews: tips grow unboundedly per business, and they are queried independently. A separate collection with references to `businesses` and `users` via their ID fields is appropriate. |

#### (b) Read/Write Trade-offs

**Embedding checkins** — *Read trade-off*: Positive. Loading a business document automatically loads its entire check-in history in a single fetch. Query 7 (`$size` on the `checkins` array) runs as a single-collection aggregation with no joins required. *Write trade-off*: Slight negative. Every new check-in appends to the array and re-writes the document, causing document growth. However, since the Yelp dataset is a static analytical dataset (no live writes), the read benefit decisively wins.

**Referencing reviews** — *Read trade-off*: Negative. Queries that need both review content and business details (Queries 2, 4) require a `$lookup` stage, which is MongoDB's equivalent of a join and incurs additional I/O. *Write trade-off*: Positive. New reviews are inserted as standalone documents without touching the business document at all — no document re-writes, no array growth concerns. For a write-heavy production system, this is essential.

**Referencing friends** — *Read trade-off*: Negative for friend-detail queries (must resolve each `user_id` in the array to fetch friend profiles), but positive for friend-count queries (`$size` on the array is O(1)). *Write trade-off*: Moderate negative. Adding or removing a friend requires updating the `friends` array on both user documents (mutual operation). However, no query in this assignment requires friend-detail resolution in MongoDB — the social graph analysis is delegated to Neo4j.

#### (c) Indexes

| Index | Collection | Type | Query Justification |
|---|---|---|---|
| `business_id` | `businesses` | Unique | Queries 2, 4, 7 — serves as the foreign key join target for `$lookup` from `reviews` into `businesses`. Without this index, every `$lookup` would require a full collection scan of 47K documents. |
| `city` | `businesses` | Standard | Query 1 groups businesses by city; Query 2 groups reviews by city+year. The index enables efficient grouping on the city field. |
| `stars` | `businesses` | Standard | Query 3 buckets businesses by star rating; Query 1 sorts by average stars. The index supports range-based bucketing and sorting. |
| `user_id` | `users` | Unique | Query 6 performs a `$lookup` from `reviews` into `users` on this field. The unique index ensures O(log n) lookups across 819K user documents. |
| `business_id` | `reviews` | Standard | Queries 2, 4 — every aggregation that joins reviews to businesses filters or groups on `business_id`. This index enables the `$lookup` to efficiently find all reviews for a given business. |
| `user_id` | `reviews` | Standard | Query 6 — joining reviews to users. The index ensures the `$lookup` from reviews to users is efficient over 2.6M review documents. |

---

## 2. Entity-Relationship (E-R) Diagram (8 marks)

*(See ER_Diagram.png)*

The E-R diagram represents the four MongoDB collections as entities and their inter-relationships. Key features:

- **BUSINESS ↔ CHECKIN**: Shown with an **embedded** relationship (double-bordered or labelled connection). Checkins are not a separate collection — they exist as an array within each business document. This is visually distinguished from cross-collection references to emphasize the modelling choice.

- **REVIEW → BUSINESS** and **REVIEW → USER**: Standard relationship lines with FK labels indicating cross-collection references via `business_id` and `user_id`. Reviews form the primary many-to-many junction between users and businesses.

- **TIP → BUSINESS** and **TIP → USER**: Same referencing pattern as reviews. Tips connect users to businesses without star ratings.

- **USER ↔ USER (friends_with)**: A self-referential relationship represented by a looping arrow. This models the `friends` array within each user document — an array of `user_id` references pointing to other documents in the same `users` collection. This is not an embedding (no user documents are nested inside other user documents); it is an array of foreign key references.

The distinction between embedded and referenced relationships in the diagram directly mirrors the schema design decisions justified in Section 1.3(a). Checkins are embedded because they are bounded and always co-accessed with their business. Reviews and tips are referenced because they grow unboundedly and are independently queried.

---

## 3. Document Schema Diagram (7 marks)

*(See Document_Schema_Diagram.png)*

The document schema diagram shows the internal structure of each collection — fields, data types, nested sub-documents, and cross-collection references.

**Key structural decisions shown in the diagram:**

- **`businesses.checkins: Array<String>`** — Labelled as EMBEDDED. This array of date strings was sourced from `checkin.json` and merged into business documents during ingestion. The diagram shows it as an internal array, not a separate entity.

- **`businesses.attributes` and `businesses.hours`** — Shown as nested sub-documents (child classes connected with composition arrows). `attributes` contains variable key-value pairs (e.g., `RestaurantsTakeOut`, `WiFi`, `BikeParking`), while `hours` contains day-of-week strings (e.g., `Monday: "9:0-17:0"`). These are objects nested within the business document, not separate collections.

- **`users.friends: Array<user_id refs>`** — The diagram shows this as an array field with a reference arrow pointing back to the `users` collection itself. This clarifies that friend data is not embedded user documents — it is an array of string references.

- **`reviews.business_id [INDEX]` and `reviews.user_id [INDEX]`** — Shown with reference arrows pointing to `businesses` and `users` respectively, labelled with INDEX to indicate both fields are indexed for efficient `$lookup` joins. The same pattern appears for `tips`.

- **Index annotations** are marked directly on fields: `businesses.business_id [UNIQUE INDEX]`, `businesses.city [INDEX]`, `businesses.stars [INDEX]`, `users.user_id [UNIQUE INDEX]`, `reviews.business_id [INDEX]`, `reviews.user_id [INDEX]`.

---

## 4. Neo4j Property Graph Model (8 marks)

*(See Graph_Model_Diagram.png)*

### Node Labels and Properties

| Node Label | Properties | Constraint |
|---|---|---|
| **User** | `user_id`, `name`, `review_count`, `average_stars` | `user_id` UNIQUE |
| **Business** | `business_id`, `name`, `city`, `state`, `stars` | `business_id` UNIQUE |
| **Review** | `review_id`, `stars`, `useful` | `review_id` UNIQUE |
| **Category** | `name` | `name` UNIQUE |

### Relationship Types

| Relationship | Direction | Properties | Description |
|---|---|---|---|
| `WROTE` | `(User)→(Review)` | — | A user authored a review |
| `REVIEWS` | `(Review)→(Business)` | — | A review is about a specific business |
| `RATED` | `(User)→(Business)` | `stars: Float` | Shortcut denormalization — direct link from user to business with the star rating, avoiding the two-hop traversal through Review |
| `IN_CATEGORY` | `(Business)→(Category)` | — | A business belongs to a category (many-to-many modelled as edges) |
| `KNOWS` | `(User)→(User)` | — | Social friendship connection between users |

### Modelling Choices

**Review as a node, not an edge**: Reviews carry rich properties (`stars`, `useful`) and sit between two entity types (User and Business). Modelling Review as a node allows properties to be attached naturally and enables flexible traversal patterns (e.g., "find all 5-star reviews of restaurants written by friends of user X"). If reviews were edges, attaching multiple properties would be less clean, and multi-hop queries would lose expressiveness.

**RATED shortcut relationship**: The `RATED` edge is a denormalized shortcut from User directly to Business, storing the `stars` property. This avoids the two-hop traversal `(User)-[:WROTE]->(Review)-[:REVIEWS]->(Business)` for common queries like "which businesses has this user rated?" — making Query 4 (category-specific rating comparisons) significantly faster. The trade-off is slight storage redundancy.

**Category as a separate node**: In MongoDB, categories are stored as a comma-separated string inside the business document, requiring `$split` and `$unwind` for analysis. In Neo4j, each category is a separate node with `IN_CATEGORY` edges. This naturally handles the many-to-many relationship (a business has many categories; a category contains many businesses) as simple edge traversal — no string parsing needed.

**KNOWS relationship (directed)**: Friend connections are modelled as directed edges. The source data provides one direction (user A lists user B as a friend), so edges are created accordingly. Cypher queries account for this by matching outgoing edges `(u)-[:KNOWS]->(f)`.

### Data That Does Not Map Naturally to a Graph

- **Checkins** — A time-series list of timestamps per business. Graphs don't have a natural ordered sequence structure; modelling each checkin as a node would create millions of nodes with minimal traversal value. This data is better represented as the embedded array in MongoDB and was excluded from the graph.

- **Business attributes/hours** — A sparse, schema-less key-value set (e.g., WiFi availability, parking options). Graph nodes expect consistent property sets. Modelling each attribute as a separate node would create unnecessary complexity. These were left out of the graph model.

- **Review text** — Free-text fields have no graph traversal value. Including the full review text as a property on Review nodes would drastically increase graph size without enabling any meaningful path-based queries. Text analysis is performed in MongoDB instead.

---

## 5. MongoDB Queries (20 marks)

### Query 1: Safest and Least-Safe Cities and Business Categories (2 marks)

**Part A — By City** (businesses with ≥20 reviews):

| City | Avg Stars | Total Reviews | Business Count |
|---|---|---|---|
| Philadelphia | 3.69 | 863,550 | 7,148 |
| Indianapolis | 3.66 | 307,878 | 3,257 |
| Nashville | 3.65 | 405,627 | 3,355 |
| Tampa | 3.63 | 390,233 | 3,909 |
| Tucson | 3.59 | 335,105 | 3,853 |

Philadelphia leads with a 3.69 average, while Tucson ranks lowest at 3.59. The range is narrow (0.10 stars), indicating all five cities are generally well-rated. The `$match: { review_count: { $gte: 20 } }` filter ensures statistical significance by excluding businesses with too few reviews to be reliable.

**Part B — By Category** (top 5 safest and least-safe):

| Rank | Safest Categories | Avg Stars | Least-Safe Categories | Avg Stars |
|---|---|---|---|---|
| 1 | Barre Classes | 4.71 | Television Service Providers | 1.63 |
| 2 | Home Window Tinting | 4.63 | Utilities | 1.77 |
| 3 | Challenge Courses | 4.61 | Internet Service Providers | 1.94 |
| 4 | Event Photography | 4.61 | Truck Rental | 2.18 |
| 5 | Dog Walkers | 4.60 | Banks & Credit Unions | 2.20 |

The highest-rated categories are niche, passion-driven services (fitness, photography, pet care) where customers self-select and providers are intrinsically motivated. The lowest-rated categories are utility/necessity services (telecom, banking, truck rental) — industries where customers have little choice and friction is inherent to the service model.

### Query 2: Star Rating Trends Over Time (3 marks)

The aggregation joins reviews with businesses to extract year and city, then computes average ratings per city-year combination.

**Key findings:**
- **Early years (2005–2007)**: Inflated ratings (3.9–4.7) due to very few reviews — enthusiastic early adopters writing positive reviews, creating selection bias.
- **Stabilization (2010–2019)**: Ratings converge around 3.7–3.9 as review volume grows into tens of thousands per city per year. The law of large numbers smooths out extremes.
- **COVID year (2020)**: Review counts drop sharply (e.g., Indianapolis drops from 50,236 in 2019 to 30,620 in 2020), but average ratings increase slightly (3.83 → 3.91). This suggests people only visited places they trusted during lockdowns, creating a positive selection bias.
- **2022 truncation**: Review counts are dramatically lower (e.g., Indianapolis has only 1,829 reviews in 2022 vs. 34,140 in 2021) — the dataset ends mid-year. This should be noted as a dataset limitation.

### Query 3: Review Volume vs. Average Star Rating (2 marks)

| Review Count Bucket | Avg Stars | Business Count |
|---|---|---|
| 0–9 | 3.59 | 14,608 |
| 10–49 | 3.55 | 21,986 |
| 50–99 | 3.66 | 4,980 |
| 100–499 | 3.79 | 5,181 |
| 500–999 | 3.97 | 489 |
| 1000–4999 | 4.07 | 134 |
| 5000+ | 4.50 | 2 |

There is a clear **positive correlation**: businesses with more reviews tend to have higher average ratings. This is explained by **survivorship bias** — poorly-rated businesses close down or stop attracting customers, so only high-quality businesses accumulate large review counts over time. The 0–9 bucket is an exception (3.59 vs. 3.55 for 10–49) likely because many new businesses start with a few enthusiastic early reviews.

**Practical implication**: Raw star ratings should always be contextualized with review count. A 4.5-star business with 3 reviews is far less reliable than a 4.0-star business with 500 reviews.

### Query 4: Review Behaviour Across Business Categories (4 marks)

| Category | Avg Stars | Avg Useful Votes | Avg Text Length | Total Reviews |
|---|---|---|---|---|
| Counseling & Mental Health | 3.06 | 4.52 | 906 chars | 1,370 |
| Lawyers | 3.39 | 4.51 | 575 chars | 1,809 |
| Insurance | 2.71 | 3.87 | 642 chars | 2,497 |
| Colleges & Universities | 3.21 | 3.79 | 838 chars | 1,913 |
| Apartments | 2.67 | 2.86 | 961 chars | 15,548 |

**Key insights:**
- **Counseling & Mental Health** and **Lawyers** top the "useful votes" ranking — these are high-stakes categories where readers specifically seek detailed, written guidance before making decisions. The community recognizes and upvotes these reviews because the stakes (mental health, legal outcomes) justify careful reading.
- These same categories have the **longest reviews** (906 and 838 avg characters) — emotional investment and decision weight drive longer, more detailed writing.
- **Low-rated categories** (Insurance 2.71, Apartments 2.67, Property Management 2.53) reflect structural dissatisfaction — these are services people use out of necessity, not choice. The negative sentiment is systemic, not business-specific.
- The `useful_per_review` metric reveals which categories produce the most community-valued content, independent of volume.

### Query 5: Impact of User Tenure on Reviewing Behaviour (3 marks)

| Tenure (years) | Avg Stars | Avg Useful Votes | Total Users |
|---|---|---|---|
| 0–1 | 2.79 | 0.16 | 908 |
| 2–4 | 3.30 | 4.24 | 72,811 |
| 5–9 | 3.65 | 22.49 | 401,631 |
| 10–14 | 3.74 | 91.39 | 321,286 |
| 15+ | 3.76 | 431.73 | 22,533 |

**Key insights:**
- **Dramatic increase in useful votes with tenure**: 0–1 year users average 0.16 useful votes; 15+ year veterans average 431.73 — a **2,700x difference**. Veteran users contribute disproportionately to platform value despite being a small minority (22,533 out of ~819,000).
- **Stars increase slightly with tenure** (2.79 → 3.76) — newer users tend to give extreme ratings (either very high or very low, often from a single strong experience), while experienced users calibrate more moderately over time as they develop a personal baseline.
- The 0–1 year cohort is very small (908 users) and likely represents users who joined near the dataset's end date (2022), so their low engagement metrics are expected.

### Query 6: Elite vs. Non-Elite Users (3 marks)

The updated query joins reviews with users to compute metrics from actual review content (not just user-level aggregates).

| Metric | Non-Elite | Elite | Difference |
|---|---|---|---|
| Mean star rating given | 3.70 | 3.98 | +0.28 |
| Mean review character length | 507 chars | 780 chars | +54% |
| Mean useful votes per review | 0.82 | 2.11 | +2.6x |
| Total reviews | 1,874,725 | 765,646 | — |

**Key insights:**
- Elite users write **54% longer reviews** on average (780 vs. 507 characters) — they invest significantly more effort in each review.
- Elite users receive **2.6x more useful votes** per review — the community recognizes their contributions as higher quality.
- Elite users give **slightly higher ratings** (3.98 vs. 3.70) — they may be more discerning in choosing where to visit, leading to more positive experiences overall.
- Only ~6.5% of users have elite status (52,973 / 819,169), but they account for **29% of all reviews** (765,646 / 2,640,371). Elite status is selective and correlates strongly with review quality and platform engagement.

### Query 7: Check-in Activity Patterns vs. Star Ratings (3 marks)

**Part A — Overall checkin volume vs. average stars:**

| Checkin Bucket | Avg Stars | Business Count |
|---|---|---|
| 0–9 | 3.59 | 18,865 |
| 10–49 | 3.58 | 13,820 |
| 50–199 | 3.61 | 8,735 |
| 200–999 | 3.69 | 5,052 |
| 1000+ | 3.89 | 908 |

Positive correlation: businesses with more check-ins tend to have higher ratings (3.59 → 3.89). Popular businesses attract more foot traffic and maintain higher quality.

**Part B — By Category (top categories by avg check-ins):**

| Category | Avg Checkins | Avg Stars | Business Count |
|---|---|---|---|
| Airports | 2,838 | 3.10 | 57 |
| Shopping Centers | 1,371 | 3.54 | 80 |
| Gastropubs | 680 | 3.85 | 189 |
| Cheese Shops | 654 | 4.26 | 55 |
| Airlines | 589 | 2.45 | 59 |
| Wineries | 538 | 4.13 | 73 |
| Pubs | 468 | 3.69 | 583 |
| Breweries | 460 | 4.09 | 262 |

**Category-level insights:**
- **Airports** and **Shopping Centers** have the highest average check-ins but only moderate ratings — high foot traffic is driven by necessity, not quality.
- **Airlines** break the overall positive correlation entirely: 589 avg check-ins but only 2.45 stars — captive customers checking in at airports, not expressing satisfaction.
- **Gastropubs, Wineries, and Breweries** confirm the positive correlation within experiential/leisure categories — high check-ins *and* high ratings (3.85–4.13), suggesting genuine repeat patronage.
- This validates the **embedding decision**: check-in data being stored on the business document made this entire aggregation a single-collection scan with no joins required.

---

## 6. Neo4j / Cypher Queries (12 marks)

### Query 1: Top 10 Users by Friend Count (2 marks)

| Name | Friend Count | Review Count | Mean Star Rating |
|---|---|---|---|
| Walker | 14,995 | 585 | 3.91 |
| Ruggy | 12,395 | 2,434 | 3.98 |
| Randy | 11,026 | 3,315 | 3.77 |
| Scott | 10,366 | 789 | 3.86 |
| Steven | 10,072 | 1,371 | 3.62 |
| Katie | 9,390 | 1,825 | 4.23 |
| Rodney | 8,809 | 1,237 | 4.09 |
| Stephanie | 8,087 | 2,000 | 3.85 |
| Emi | 8,028 | 1,926 | 4.33 |
| Frank | 7,945 | 1,006 | 4.08 |

*(See neo4j_query1_graph.png for graph visualization showing the social network clusters of the top connected users.)*

**Analysis:** There is no strong correlation between friend count and review count. Walker leads with 14,995 friends but only 585 reviews — a highly social but not prolific reviewer. Conversely, Randy has the most reviews (3,315) with fewer friends (11,026). Social connectivity and review activity appear to be independent dimensions of user behaviour. Mean star ratings cluster tightly (3.62–4.33), suggesting that social influence does not systematically bias rating behaviour.

### Query 2: Top 3 Businesses per State (3 marks)

| State | Business Name | City | Avg Stars | Reviews |
|---|---|---|---|---|
| AZ | Let's Sweat | Tucson | 5.00 | 59 |
| AZ | Premier Pest Solutions | Tucson | 5.00 | 56 |
| AZ | Tremendez Jewelry and Repair | Tucson | 4.98 | 55 |
| FL | AllVitae Health & Chiropractic | Tampa | 5.00 | 67 |
| FL | Gerardo Luna Photographs | Tampa | 4.98 | 51 |
| FL | Tampa Bay Rum Company | Tampa | 4.96 | 57 |
| IN | Alvin Lui | Indianapolis | 5.00 | 50 |
| IN | Brick & Mortar Barber Shop | Indianapolis | 4.97 | 72 |
| IN | kOMpose | Indianapolis | 4.97 | 58 |
| PA | Guldner's Collision Service | Philadelphia | 5.00 | 57 |
| PA | Campbell Jewelers | Philadelphia | 5.00 | 55 |
| PA | Sugar Bar Salon | Philadelphia | 5.00 | 55 |
| TN | Taylor Home Solutions | Nashville | 5.00 | 50 |
| TN | Music City Cats | Nashville | 5.00 | 54 |
| TN | Walls Jewelry Repairing | Nashville | 5.00 | 114 |

All top businesses have near-perfect ratings (4.96–5.00) with modest review counts (50–114). This reflects **statistical fragility** — a small number of very satisfied customers can maintain a perfect score. These businesses are genuinely exceptional at their niche, but the results would look different with a higher review threshold.

**Category diversity across states**: AZ favours wellness/fitness (yoga, pest control), FL has hospitality (chiropractic, rum distillery), IN has entertainment/grooming (magic shows, barbershop), PA has automotive/jewellery (collision repair, jewellers), TN has home services (carpet cleaning, pet sitting, jewellery repair).

### Query 3: Top 10 Users Who Reviewed Across the Most Distinct Cities (3 marks)

All 10 users in the results covered **all 5 cities** in the subset (Philadelphia, Tucson, Tampa, Indianapolis, Nashville). Since the dataset was subsetted to exactly 5 cities, 5 is the ceiling — this is a dataset limitation, not a meaningful differentiation. In a full-dataset analysis, this query would reveal travelling reviewers and food tourism patterns.

The differentiator is **per-city mean rating variation**. For example:
- **David**: rates Tampa highly (4.50) but Indianapolis lower (3.60) — possible preference for Tampa's business landscape.
- **Olivier**: rates Tucson at 4.00 but Indianapolis at 2.50 — significant city-level bias.
- **S**: highest average across all cities (3.75–5.00) — a consistently generous rater.

Note: User "S" appearing in results is a data quality issue in the Yelp source data (truncated name).

### Query 4: Category-Specific Rating Comparison — Restaurants (3 marks)

*(See neo4j_query4_graph.png for the Business → Category traversal visualization.)*

| User | Restaurants Reviewed | User Avg in Restaurants | User Overall Avg | Deviation |
|---|---|---|---|---|
| Michelle | 714 | 4.01 | — | +0.18 |
| Brett | 556 | 4.19 | — | +0.37 |
| Karen | 538 | 3.53 | — | −0.30 |
| Peter | 486 | 3.31 | — | −0.51 |
| Mark | 373 | 3.29 | — | −0.54 |
| Brittany | 371 | 4.30 | — | +0.48 |

The query compares each user's average rating specifically for Restaurants against their own overall average across all reviews. The deviation reveals **category-specific biases**:

- **Positive deviations** (Brittany +0.48, Ken +0.38, Brett +0.37): These users rate Restaurants higher than their typical behaviour — they either have genuinely better experiences at restaurants or are inherently more generous with food-related ratings.
- **Negative deviations** (Mark −0.54, Mike −0.52, Peter −0.51): These users are systematically harsher specifically in Restaurants compared to how they review everything else — a category-specific critical lens.
- **Michelle** reviewed 714 restaurants — an extreme power user with a modest +0.18 deviation, suggesting her restaurant ratings are closely aligned with her general behaviour.

### Query 5: Reproducing MongoDB Query 1 in Cypher (1 mark)

**Cypher reproduction of MongoDB Query 1** (cities ranked by average business star rating, businesses with ≥20 reviews):

| City | Avg Stars | Total Reviews | Business Count |
|---|---|---|---|
| Philadelphia | 3.69 | 895,422 | 7,288 |
| Indianapolis | 3.64 | 320,311 | 3,330 |
| Nashville | 3.64 | 416,408 | 3,414 |
| Tampa | 3.63 | 405,732 | 3,988 |
| Tucson | 3.58 | 352,978 | 3,963 |

**Results match closely**: Rankings are identical (Philadelphia first, Tucson last). Minor differences in total review counts (MongoDB: 863,550 vs. Cypher: 895,422 for Philadelphia) arise because MongoDB used the pre-stored `review_count` field on the business document, while Cypher counted actual Review nodes present in the ingested graph subset.

**Comparison — Which database is better suited?**

MongoDB is better suited for this particular query. It is a pure aggregation on a single collection (`businesses`) — no relationship traversal is needed. The `$match → $group → $sort` pipeline is highly optimized for flat, tabular aggregations. The query executes as a single-collection scan.

Cypher, by contrast, must traverse `(Business)<-[:REVIEWS]-(Review)` edges to count reviews, which is elegant and avoids relying on a pre-stored field, but slower for aggregate-only analytics. The graph model's strength shows when queries need relationship traversal — friend networks, multi-hop paths, category-based recommendations — not flat aggregations. For this specific query, MongoDB's document model wins on simplicity and performance.

---

## Appendix: Query Scripts

All MongoDB queries are provided in `MongoDB_Queries.txt`.
All Neo4j/Cypher queries are provided in `Neo4j_Cypher_Queries.txt`.
