# HW2 - Cassandra (AstraDB) Assignment

## Tables Design

| Table | Partition Key | Clustering Columns | Purpose |
|-------|--------------|-------------------|---------|
| `items_by_asin` | asin | - | Item lookup |
| `reviews_by_user` | reviewer_id | review_time DESC, asin ASC | User's reviews |
| `reviews_by_item` | asin | review_time DESC, reviewer_id ASC | Item's reviews |

## Tests Performed

### item()
```
> item B005QB09TU          # Existing item - returns details ✓
> item NONEXISTENT123      # Non-existing - returns "not exists" ✓
> item 0078800242          # Item with missing title - shows "na" ✓
```

### userReviews()
```
> userReviews A17OJCRPMYWXWV   # Returns 2 reviews, sorted by time DESC ✓
```

### itemReviews()
```
> itemReviews B005QDQXGQ       # Returns 3 reviews, sorted by time DESC ✓
```

## Setup Requirements

**astradb folder** must contain:
- `secure-connect-hw2.zip` - AstraDB secure connect bundle
- `login-token.json` - Contains `clientId` and `secret`

**data folder** must contain:
- `meta_Office_Products.json` - Product metadata
- `reviews_Office_Products.json` - Product reviews

## Compile & Run

### Option 1: Without JAR

Compile:
```bash
javac -d bin -cp "lib/*:lib/lib/*:bin" src/bigdatacourse/hw2/studentcode/HW2StudentAnswer.java
```

Run:
```bash
java -cp "lib/*:lib/lib/*:bin" bigdatacourse.hw2.HW2CLI <astradb-folder> <data-folder>
```

### Option 2: With JAR

Compile (if not already done):
```bash
javac -d bin -cp "lib/*:lib/lib/*:bin" src/bigdatacourse/hw2/studentcode/HW2StudentAnswer.java
```

Create JAR:
```bash
jar cfm hw2cli.jar MANIFEST.MF -C bin .
```

Run:
```bash
java -jar hw2cli.jar <astradb-folder> <data-folder>
```
