# E-R Diagram — MongoDB Schema

> Render this Mermaid diagram at https://mermaid.live or in any Markdown viewer that supports Mermaid.

```mermaid
erDiagram
    BUSINESS {
        string business_id PK
        string name
        string city
        string state
        float stars
        int review_count
        string address
        float latitude
        float longitude
        int is_open
        string categories
        object attributes
        object hours
        array checkins "EMBEDDED from checkin.json"
    }

    USER {
        string user_id PK
        string name
        int review_count
        string yelping_since
        int useful
        int funny
        int cool
        string elite
        float average_stars
        int fans
        int compliment_hot
        int compliment_more
        int compliment_profile
        int compliment_cute
        int compliment_list
        int compliment_note
        int compliment_plain
        int compliment_cool
        int compliment_funny
        int compliment_writer
        int compliment_photos
        array friends "Array of user_id refs"
    }

    REVIEW {
        string review_id PK
        string user_id FK
        string business_id FK
        float stars
        int useful
        int funny
        int cool
        string text
        string date
    }

    TIP {
        string user_id FK
        string business_id FK
        string text
        string date
        int compliment_count
    }

    CHECKIN {
        string business_id FK
        string date "EMBEDDED into Business"
    }

    USER ||--o{ REVIEW : "writes (ref via user_id)"
    BUSINESS ||--o{ REVIEW : "receives (ref via business_id)"
    USER ||--o{ TIP : "leaves (ref via user_id)"
    BUSINESS ||--o{ TIP : "receives (ref via business_id)"
    BUSINESS ||--|| CHECKIN : "contains (EMBEDDED)"
    USER }o--o{ USER : "friends_with (ref via friends array)"
```

## Relationship Explanations

| Relationship | Type | Justification |
|---|---|---|
| **Business ↔ Checkins** | **Embedded** | Checkins are bounded per business and always queried alongside business data (Query 7). Embedding avoids a `$lookup` and keeps reads fast. |
| **Business ↔ Reviews** | **Referenced** (via `business_id` in reviews collection) | Reviews are unbounded and can grow to thousands per business. Embedding would exceed the 16MB document limit. Referencing keeps writes cheap. |
| **User ↔ Reviews** | **Referenced** (via `user_id` in reviews collection) | Same reasoning — a user can write thousands of reviews. `$lookup` is used when needed (Queries 2, 4). |
| **User ↔ Tips** | **Referenced** (via `user_id` in tips collection) | Tips are unbounded per user. Separate collection keeps user documents lean. |
| **Business ↔ Tips** | **Referenced** (via `business_id` in tips collection) | Tips are unbounded per business. |
| **User ↔ User (Friends)** | **Referenced** (array of `user_id` strings embedded in user doc) | The friends list is stored as an array of IDs within the user document. This is a hybrid approach — the array is embedded, but each ID references another user document. This avoids a separate junction collection while keeping friend lookups efficient. |
