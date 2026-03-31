# CS-3510 Assignment 2: Yelp Dataset NoSQL Walkthrough Plan

This document outlines a structured plan to tackle the Data Science and Management Assignment 2, which involves designing and querying MongoDB and Neo4j databases using the large-scale Yelp Open Dataset.

## Overview
- **Objective:** Design and implement document (MongoDB) and graph (Neo4j) databases to analyze business, user, and social data. Do predictive modelling.
- **Dataset:** Yelp Open Dataset (business, review, user, tip, checkin, photo). Subsetting is allowed and recommended.
- **Deliverables:** PDF Report, E-R Diagram, Document Schema Diagram, Graph Model Diagram, MongoDB Query Scripts (.txt), Cypher Query Scripts (.txt).

---

## Step-by-Step Implementation Plan

### Phase 1: Data Preparation & Intelligent Subsetting
Working with the entire 7-million review dataset is computationally expensive. 
1. **Analyze Dataset Structure:** Understand the JSON schemas of the provided files.
2. **Intelligent Subsetting:** Since the Neo4j portion requires interconnected social data and graph algorithms (PageRank, Community Detection), a random sample will result in a disconnected sparse graph.
   - *Action:* Write a Python script to sample the dataset intelligently—e.g., filtering for a specific major city (like Philadelphia or Las Vegas) or using a "Snowball Sampling" technique starting from highly connected users.
3. **Data Cleaning:** Handle missing values and format dates/arrays to fit NoSQL insertions.

### Phase 2: MongoDB Design & Implementation (35 Marks)
1. **Schema Design:**
   - Map out E-R diagrams and Document Schemas. 
   - Decide between embedding (e.g. `checkins` inside `business` if bounded) vs referencing (e.g. `reviews` referencing `business_id` and `user_id` due to unbounded growth).
2. **Data Ingestion:** Write Python scripts to bulk insert the sampled JSON data into MongoDB collections.
3. **Indexing:** Define at least 4 indexes (e.g., compound index on `city` and `stars`) based on the query requirements to optimize read queries.
4. **Querying (20 Marks):** Write and execute the 7 specific MongoDB aggregation pipelines (safest cities, review behaviors, elite users impact, etc.).

### Phase 3: Neo4j Graph Model & Implementation (20 Marks)
1. **Graph Modeling:** Design the Property Graph (Nodes: `User`, `Business`, `Review`, `Category`. Edges: `WROTE`, `REVIEWS`, `FRIENDS_WITH`, `IN_CATEGORY`).
2. **Data Ingestion:** Export the clean sampled dataset to CSVs and use Neo4j's `LOAD CSV` or the `neo4j` Python driver for fast ingestion.
3. **Cypher Querying & Algorithms (12 marks):**
   - Write paths queries (recommendations based on friends).
   - Use Graph Data Science (GDS) library to run **PageRank**.
   - Use GDS to run **Community Detection** (e.g., Louvain method).

### Phase 4: Predictive Modeling (10 Marks)
1. **Feature Engineering:** 
   - Extract tabular features from MongoDB (user average rating, review count, etc.).
   - Extract graph features from Neo4j (PageRank scores, community IDs, average ratings of user's friends).
2. **Model Training:** Build a machine learning model (e.g., XGBoost, LightGBM, or Logistic Regression) to predict the star rating a user gives a business.
3. **Evaluation:** Evaluate metrics and document the feature importances (specifically the graph features).

### Phase 5: Documentation & Deliverables Packaging
1. **Diagrams:** Create neat, professional diagrams for E-R, Document Schema, and Graph Model using tools like draw.io or Mermaid.
2. **Report Compilation:** Compile justifications, query results, and ML model performance into the final written PDF report.
3. **Code Formatting:** Export the MongoDB and Cypher queries to cleanly formatted `.txt` files.
4. **Final Zip:** Package the PDF, images, and text files.

---

## How To Go "Above and Beyond" 

### 1. Advanced Graph-Based Sampling Strategy
Instead of haphazardly dropping rows, we can implement **Random Walk with Restart (RWR)** or **Forest Fire Sampling** to subset the data. This guarantees that the subgraph we feed into Neo4j remains densely connected, which is critical for accurate PageRank and Community Detection results.

### 2. Natural Language Processing (NLP) on Text Reviews
The dataset provides the text of the reviews. For the predictive model, we can run a quick sentiment analysis using a pre-trained model (like VADER or an HuggingFace transformer) on the review text. This sentiment score can be used as a powerful engineered feature alongside the graph features.

### 3. Interactive Streamlit Dashboard
Since we have experience with Streamlit, we could build a quick local Streamlit application that provides a UI to interact with our Neo4j and MongoDB queries. Instead of just static text queries, the dashboard could:
- Show an interactive Map (using PyDeck/Folium) of the safest/least-safe cities.
- Allow the user to input a `user_id` and get real-time Cypher recommendations for businesses.

### 4. Advanced ML: Embeddings from Neo4j (Node2Vec)
For the predictive modelling component, instead of just using basic graph metrics (like degree or PageRank), we can generate dense vector embeddings for users and businesses using **Node2Vec** or **FastRP** (available in Neo4j GDS). Feeding these embeddings into our XGBoost model will significantly boost predictive accuracy.

### 5. Advanced Geospatial Queries & Clustering
We can leverage MongoDB's powerful `2dsphere` indexes to perform geo-spatial queries. Instead of just analyzing text categories, we could identify high-density "foodie corridors" geographically or build a tool that routes you through the highest-rated businesses across a specific area boundary.

### 6. Temporal Graph Analysis
A standard graph models a single point in time, but the Yelp data spans years. By adding time attributes to our edges (`REVIEWED_ON`, `BECAME_FRIENDS_ON`), we can visualize how the Yelp graph evolves dynamically. We could track how a specific business's PageRank influence rose or fell over different years.

### 7. Real-Time Streaming Pipeline Simulation
To demonstrate big-data prowess, we could simulate a live stream of user reviews. Using Python concurrency (or even a lightweight Kafka/Spark stream), we could show how "live" data gets ingested into MongoDB and updates Neo4j recommendations in real-time without taking the databases offline.

### 8. Hybrid Recommendation Engine
Rather than relying purely on graph connections (collaborative filtering via Neo4j), we could build a hybrid engine. It would combine the user-friend paths from Neo4j with **content-based similarity**—for instance, using MongoDB text-index searches on the text of the reviews themselves to match users who like similar ambiances or specific dishes.

### 9. Explainable AI (SHAP) For Rating Predictions
For the predictive modeling task, taking it a step further would mean not just generating a prediction, but fully breaking it down using **SHAP (SHapley Additive exPlanations)** values. By plotting the SHAP summary graphs, we can explicitly show exactly *why* our model predicted a 4-star rating vs. a 2-star rating, pointing specifically to how much the graph features vs. the MongoDB tabular features influenced the model's decision.
