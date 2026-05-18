<div align="center">

# Grocery Orders Analytics Dashboard - Pipeline Detailing Explanation

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-3.x-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-latest-FFCC01?style=flat-square&logo=clickhouse&logoColor=black)
![Metabase](https://img.shields.io/badge/Metabase-latest-509EE3?style=flat-square&logo=metabase&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)

</div>

---

## Table of Contents

1. [Team](#-team)
2. [Pipeline Architecture](#-pipeline-architecture)
3. [Fetch API & Data Ingestion](#-fetch-api--data-ingestion)
4. [Dataset Explanation & EDA](#-dataset-explanation--eda)
5. [Spark Processing](#-spark-processing)
6. [Docker Setup](#-docker-setup)
7. [Proof: ClickHouse & Airflow](#proof-clickhouse--airflow)
8. [Data Visualization](#-data-visualization)
9. [Dashboard Overview](#-dashboard-overview)
10. [Insights](#-insights)
11. [End-to-End Running Guide](#-end-to-end-running-guide)
12. [Closing](#-closing)

---

## Team

<table align="center">
    <td align="center" width="320">
      <b>Ibrahim Ferel</b>
      <br>
      <code>5025241049</code>
      <br><br>
    </td>
    <td align="center" width="320">
      <b>Afarrel Febryan Putra Andy</b>
      <br>
      <code>5025241137</code>
      <br><br>
    </td>
  </tr>
</table>

---

## Pipeline Architecture

This pipeline follows an **ELT (Extract → Load → Transform)** architecture based on Big Data principles, with full orchestration using Apache Airflow.

![alt text](asset_documentation/image.png)

### Flow Summary

| Step | Script | Output |
|------|--------|--------|
| Fetch | `fetch_task2_stream.py` | `.parquet` files in the Data Lake |
| Process | `process_task2_spark.py` | 4 tables in ClickHouse |
| Visualize | Metabase | Analytics dashboard |
| Orchestrator | Airflow DAG | Schedules & monitors all steps |

---

## Fetch API & Data Ingestion

The `fetch_task2_stream.py` script is responsible for retrieving order data from the source (an Instacart-style dataset) and storing it in the Data Lake in **Parquet** format.

### Workflow

```text
Orders API
    |
HTTP Request (GET)
    |
JSON Response
    |
Nested Data Parsing
    |
Feature Extraction
    |
Flattening Orders + Products
    |
Add Ingestion Timestamp
    |
Save as Parquet
```

### Steps

1. **Fetching data from the Orders API**

   The script sends an HTTP GET request to the following endpoint:

   ```text
   http://96.9.212.102:8000/orders
   ```

   to retrieve the order transaction dataset.

2. **Parsing the JSON Response**

   The API response is converted into a Python dictionary using:

   ```python
   response.json()
   ```

   The dataset has a **nested JSON** structure, where each order contains transaction metadata and a list of products under the `products` key.

   Example data structure:

   ```json
   {
     "order_id": 1,
     "user_id": 5,
     "products": [
       {
         "product_id": 10,
         "product_name": "Banana"
       }
     ]
   }
   ```

3. **Feature Extraction from Nested JSON**

   Since the dataset is nested, the pipeline extracts features from two levels of data:

   **A. Order-level features**

   Main transaction metadata extracted from the order object:

   - `order_id`
   - `user_id`
   - `order_number`
   - `order_dow`
   - `order_hour_of_day`
   - `days_since_prior_order`

   **B. Product-level features**

   Product information extracted from the `products` list:

   - `product_id`
   - `product_name`
   - `department`
   - `aisle`
   - `add_to_cart_order`
   - `reordered`

4. **Flattening the Nested Structure**

   Since a single order can contain many products, the dataset is transformed from nested form into tabular form.

   Example:

   Before flattening:

   ```text
   Order 1
    |-- Products:
         |-- Banana
         |-- Milk
         |-- Bread
   ```

   After flattening:

   | order_id | product_name |
   |----------|--------------|
   | 1        | Banana       |
   | 1        | Milk         |
   | 1        | Bread        |

   This transformation simplifies analytics using Spark SQL and storage into the warehouse.

5. **Adding an Ingestion Timestamp**

   Each row is assigned an attribute:

   ```text
   ingestion_timestamp
   ```

   to record when the data was retrieved by the pipeline.

6. **Creating the DataFrame**

   The parsed and flattened data is converted into a **Pandas DataFrame**.

7. **Saving to the Data Lake**

   The dataset is saved to:

   ```text
   /opt/airflow/data_lake/task2/
   ```

   in **Apache Parquet** format.

### Final Dataset Schema

| Feature |
|---------|
| order_id |
| user_id |
| order_number |
| order_dow |
| order_hour_of_day |
| days_since_prior_order |
| product_id |
| product_name |
| department |
| aisle |
| add_to_cart_order |
| reordered |
| ingestion_timestamp |

### Output

Example output file:

```text
orders_20260517_143022.parquet
```

**Apache Parquet** format was chosen because:

- Columnar storage
- More efficient compression
- Optimal for Spark processing
- Commonly used in Data Engineering pipelines

### Output Structure

Before the pipeline was orchestrated using Apache Airflow, each processing stage was tested individually via the terminal to validate functions, data flow, and output:

![alt text](asset_documentation/image-1.png)

Example Output:

![alt text](asset_documentation/image-2.png)

---

## Dataset Explanation

### Dataset Overview

The dataset contains e-commerce / grocery transaction data (Instacart-style) that records user purchasing behavior in detail.

### Nested JSON Dataset Structure Visualization

![alt text](asset_documentation/image-3.png)

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | UInt32 | Unique ID for each order |
| `user_id` | UInt32 | ID of the user who placed the order |
| `order_number` | UInt32 | Sequential number of the order for that user |
| `order_dow` | UInt8 | Day of the week (0=Sunday, 6=Saturday) |
| `order_hour_of_day` | UInt8 | Hour of order placement (0–23) |
| `days_since_prior_order` | Nullable(UInt16) | Days elapsed since the previous order (NULL = first order) |
| `product_id` | UInt32 | Product ID |
| `product_name` | String | Product name |
| `department` | String | Product department (produce, dairy, etc.) |
| `aisle` | String | More specific product subcategory / aisle |
| `add_to_cart_order` | UInt8 | Order in which the product was added to the cart |
| `reordered` | UInt8 | 1 = previously purchased, 0 = first time |
| `ingestion_timestamp` | String | Timestamp when data was ingested into the system |

### Parquet File Preview

![alt text](asset_documentation/image-4.png)

---

## Spark Processing

The `process_task2_spark.py` script uses Apache Spark to process all Parquet files in the Data Lake in parallel and produce 4 analytics tables.

#### Workflow

```text
Parquet Files (Data Lake)
        |
Spark Read Parquet
        |
Data Processing & Aggregation
        |
Generate Analytics Tables
        |
Data Type Validation & Casting
        |
Load to ClickHouse Warehouse
        |
Cleanup Processed Parquet Files
```

### 1. Reading Data from the Data Lake

The pipeline reads all available parquet files from:

```text
/opt/airflow/data_lake/task2/
```

using Apache Spark:

```python
spark.read.parquet(...)
```

Since Spark natively supports reading multiple files at once, the entire batch of data can be processed in one pass.

The dataset is then cached using:

```python
df_raw.cache()
```

to improve performance for analytics processes that reference the same dataframe multiple times.

### 2. Product Analytics Processing

The pipeline performs aggregated analytics grouped by:

- `product_name`
- `department`

Computed metrics:

| Metric | Description |
|--------|-------------|
| total_orders | Total number of product transactions |
| reorder_count | Total number of reordered products |
| reorder_rate | Percentage of reorders relative to total transactions |

Reorder rate formula:

```text
reorder_rate = reorder_count / total_orders
```

Example insights that can be derived:

- Most popular products.
- Products with the highest repurchase rate.
- Product performance per department.

### 3. Hourly Analytics Processing

The pipeline groups data by:

```text
order_hour_of_day
```

to obtain the distribution of transaction counts per hour.

Metrics:

- `total_orders`

Insights that can be obtained:

- The busiest hours for customer orders.
- Daily purchasing activity patterns.

### 4. Department Analytics Processing

Department analytics are computed by grouping on:

```text
department
```

to obtain total transaction volume per department.

Insights:

- Departments with the highest purchasing activity.
- Sales distribution across product categories.

### 5. ClickHouse Data Warehouse Initialization

The pipeline establishes a connection to the ClickHouse Warehouse.

The analytics database is created automatically if it does not already exist:

```sql
CREATE DATABASE IF NOT EXISTS analytics
```

Four warehouse tables are then created:

| Table | Function |
|-------|----------|
| raw_orders | Stores the raw transactional dataset |
| product_analytics | Stores product analytics |
| hourly_analytics | Stores per-hour analytics |
| department_analytics | Stores per-department analytics |

### 6. Data Type Validation & Safe Casting

Before inserting into ClickHouse, the pipeline validates data types.

Several helper functions are used:

#### safe_int()

Used to:

- Handle NULL values
- Avoid type mismatches
- Ensure all numeric columns become Python native integers.

#### safe_str()

Used to:

- Handle empty values
- Ensure all string columns are valid before insertion.

This step is important because ClickHouse enforces strict data type validation.

### 7. Loading Data to the Warehouse

The pipeline uses:

```text
TRUNCATE + INSERT
```

mode to ensure the dashboard always displays the latest data.

Flow:

```text
TRUNCATE TABLE
      |
Prepare tuples
      |
INSERT INTO ClickHouse
```

Each analytics table is overwritten with the latest batch.

### 8. Data Lake Cleanup

After the warehouse loading is complete, old parquet files are automatically deleted:

```text
/opt/airflow/data_lake/task2/*.parquet
```

Reasons for cleanup:

- Prevents accumulation of old batches
- Keeps storage efficient
- Avoids duplicate processing.

### Output Warehouse Tables

#### 1. analytics.raw_orders

Raw transactional dataset.

#### 2. analytics.product_analytics

Product and reorder behavior analytics.

#### 3. analytics.hourly_analytics

Transaction distribution analytics per hour.

#### 4. analytics.department_analytics

Transaction volume analytics per department.

---

## Docker Setup

### Versions & Environment

```env
AIRFLOW_VERSION=2.9.1
PYTHON_VERSION=3.11
SPARK_VERSION=3.5.0
CLICKHOUSE_VERSION=23.8
POSTGRES_VERSION=13
METABASE_VERSION=latest
```

### docker-compose.yml (summary)

```yaml
version: '3.8'

services:

  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow

  airflow-webserver:
    image: apache/airflow:2.8.1-python3.11
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data_lake:/opt/airflow/data_lake
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow

  airflow-scheduler:
    image: apache/airflow:2.8.1-python3.11
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data_lake:/opt/airflow/data_lake

  clickhouse-server:
    image: clickhouse/clickhouse-server:23.8
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      CLICKHOUSE_USER: admin
      CLICKHOUSE_PASSWORD: rahasia
      CLICKHOUSE_DB: analytics

  metabase:
    image: metabase/metabase:latest
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: h2
```

### Dockerfile (Custom Airflow)

```dockerfile
FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y \
    default-jdk \
    && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    clickhouse-driver==0.2.6 \
    pandas==2.0.3 \
    pyarrow==14.0.1
```

---

## Proof: ClickHouse & Airflow

### Airflow: DAG Successful

The pipeline runs on an hourly schedule via the Airflow DAG `task2_orders_pipeline`.

![alt text](asset_documentation/image-5.png)
![alt text](asset_documentation/image-6.png)

### ClickHouse: Data Verified

```sql
-- Verify data ingestion
SELECT table, formatReadableQuantity(sum(rows)) AS total_rows
FROM system.parts
WHERE database = 'analytics' AND active
GROUP BY table;
```

```
|--------------------------|--------------|
| table                    | total_rows   |
|--------------------------|--------------|
| raw_orders               | 21           |
| product_analytics        | 912          |
| hourly_analytics         | 736          |
| department_analytics     | 20           |
|--------------------------|--------------|
```

![alt text](asset_documentation/image-7.png)

---

## Data Visualization

All visualizations are built in Metabase using direct queries to ClickHouse.

### Table: `product_analytics`

| Query | Title | Chart |
|-------|-------|-------|
| A1 | Top 10 Most Ordered Products | Row |
| A2 | Top 10 Products by Reorder Rate | Row |
| A3 | Products with Lowest Reorder Rate | Row |
| A4 | Unique Products & Total Orders per Department | Bar |
| A5 | Average Reorder Rate per Department | Bar |
| A6 | Top 5 Products per Department | Table |
| A7 | Evergreen Products: High Orders & High Loyalty | Scatter |
| A8 | Reorder Rate Distribution by Bucket | Pie |
| A9 | Total Orders vs Reorders by Department | Combo |
| A10 | First-Time Buyer Dominant Products | Row |

### Table: `hourly_analytics`

| Query | Title | Chart |
|-------|-------|-------|
| B1 | Distribution Orders per Hour | Pie |
| B2 | Time Segmentation | Bar |
| B3 | Running Total Orders Throughout the Day | Area |
| B4 | Hourly Order Performance vs Daily Average | Sankey |

### Table: `department_analytics`

| Query | Title | Chart |
|-------|-------|-------|
| C1 | Department Contribution to Total Transactions | Donut |
| C2 | Concentration Risk Analysis | Scalar |
| C3 | Anomaly Detection by Total Orders | Bar |
| C4 | Department with Total Orders Above and Below Average | Sankey |
| C5 | Total Orders Gap to Next Department | Combo |

### Table: `raw_orders`

| Query | Title | Chart |
|-------|-------|-------|
| D1 | All Transaction Count | Scalar |
| D2 | Order Distribution per Day of the Week | Combo |
| D3 | Top 10 Most Active User by Order Count | Bar |
| D4 | Product Reorder Distribution | Area |
| D5 | Top 10 Most Popular Aisle | Donut |
| D6 | The Most Consistent Users Reorder | Bar |
| D7 | Department Contribution to Total Transactions | Bar |
| D8 | Customer Shopping Intensity by Time | Table |
| D9 | Department with the Most Loyal Customers | Line |
| D10 | Customer Retention by Interval Order | Donut |
| D11 | Market Basket Depth Analysis | Bar |

---

## Dashboard Overview

The Metabase dashboard is divided into **4 main sections:**

### <u>1. Product Performance</u>

Presents product performance analysis through order volumes, customer reorder behavior, and the distribution of product metrics. These visualizations help identify which products are purchased most frequently, which are reordered most consistently, and which have the greatest influence across the entire dataset.

![alt text](asset_documentation/image-8.png)
![alt text](asset_documentation/image-9.png)
![alt text](asset_documentation/image-10.png)

#### <u>A1. Top 10 Most Ordered Products (Row Chart)</u>

Displays the 10 products with the highest number of orders to identify the most popular products and those appearing most frequently in transactions. Based on the dataset analysis, the products with the highest order volumes are dominated by items from the produce department. Additionally, order counts within the top 10 range from approximately 7 to 17 transactions, indicating products with relatively higher demand compared to the rest of the dataset.

#### <u>A2. Top 10 Products by Reorder Rate (Row Chart)</u>

Explores products with the highest repurchase rates as an indicator of customer loyalty or tendency to repeatedly buy certain products. The results reveal a different composition when products are ranked by reorder rate versus total order count. In this category, a product from the beverages department — Lime Sparkling Water — appears, while most other products remain similar to those in the high-order-volume list. This shows that product popularity and customer loyalty do not always produce identical rankings.

#### <u>A3. Products with Lowest Reorder Rate (Row Chart)</u>

Shows products with the lowest reorder rates to help identify items that are rarely repurchased or have low customer loyalty. The results indicate that the list of products with the lowest reorder rates is still largely dominated by produce items. However, products from other departments that were not prominent in earlier analyses also appear here, such as canned goods, frozen, and pantry. This suggests that products in those departments tend to have lower repurchase rates, meaning customers are less likely to buy them again.

#### <u>A4. Unique Products & Total Orders per Department (Bar Chart)</u>

Analyzes the number of unique products and total order activity per department to assess the scale and diversity of each product catalog. The departments with the highest number of unique products and the highest order activity are dominated by produce, followed by dairy eggs and snacks. This indicates that these three departments have relatively broad product variety and consistently high customer demand compared to others.

#### <u>A5. Average Reorder Rate per Department (Bar Chart)</u>

Compares the average reorder rate across departments to understand which product categories exhibit stronger customer loyalty patterns. The bulk department shows the highest average reorder rate, but its total order count is very low, making the insight less representative of general customer behavior. When balancing reorder rate against transaction volume, produce, dairy eggs, and snacks emerge as the strongest and most consistent departments in terms of both high order activity and good customer loyalty.

#### <u>A6. Top 5 Products per Department (Table)</u>

Displays the top five products in each department based on order performance, revealing standout products within each category. This visualization allows comparison of the top products from every department based on a combination of total orders and reorder rate. The results show that several products have outstanding performance in their respective categories, with Lime Sparkling Water standing out as one of the top-ranked products based on the balance between order volume and customer repurchase rate.

#### <u>A7. Evergreen Products: High Orders & High Loyalty (Scatter Chart)</u>

Maps the relationship between order count and reorder rate to identify products that have both high demand and strong customer loyalty. Based on patterns observed in earlier analyses, produce products consistently show the strongest performance in both order volume and customer repurchase rates. It is therefore expected that the top positions in this visualization are dominated by produce items such as Organic Strawberries, Banana, and similar products that combine high popularity with strong loyalty.

#### <u>A8. Reorder Rate Distribution by Bucket (Pie Chart)</u>

Through this pie chart, reorder rates are grouped into several categories — for example, Very Low, Low, Medium, High, and Very High — based on defined value ranges (e.g., 0.2, 0.5, 0.8, etc.). The analysis reveals that the dataset contains a large proportion of products with high reorder rates, accounting for nearly more than half of the distribution. Notably, the Very Low reorder rate category also holds a significant share. This indicates a polarized distribution pattern, where some products enjoy strong customer loyalty while others are rarely repurchased.

#### <u>A9. Total Orders vs Reorders by Department (Combo Chart)</u>

Compares total orders and total reorders per department to assess the balance between sales volume and customer repurchase behavior. The combo chart again shows that the produce department delivers the most balanced and consistent performance — it records high total orders and is also followed by a large total reorder count, indicating that its products are not only purchased frequently but are also successful in driving repeat purchases. Some other departments show a more significant gap between total orders and total reorders, which may indicate an imbalance between product popularity and customer loyalty.

#### <u>A10. First-Time Buyer Dominant Products (Row Chart)</u>

This chart highlights products that are frequently purchased on a customer's first order but have a low tendency to be reordered. Notably, this pattern differs from earlier analyses and is no longer dominated by the produce department. Some examples of products that appear here include Sour Cream, Organic Zucchini, Peach Preserves, Original Spread, and Blueberries. This suggests that even if a product successfully attracts an initial purchase, it does not necessarily retain customer interest for repeat buying.

---

### <u>2. Hourly & Time Pattern</u>

Displays ordering patterns throughout the day. The line chart B1 shows the order curve per hour, while the area chart B3 shows the cumulative accumulation of orders. This dashboard is useful for determining the best times for push notifications, flash sales, or restocking.

![alt text](asset_documentation/image-11.png)
![alt text](asset_documentation/image-12.png)

#### <u>B1. Distribution Orders per Hour (Pie Chart)</u>

Displays the proportion of orders placed at each hour of the day. Based on the analysis, hour 15 (3:00 PM) recorded the highest percentage at 12.41%, followed by hour 17 at 12.31%, and hour 9 at 11.10%. This indicates that customer ordering activity is most concentrated in the midday to afternoon range, with a total of 1,072 transactions overall.

#### <u>B2. Time Segmentation (Bar Chart)</u>

Groups total orders into four main time segments: Afternoon (12–17), Morning (05–11), Evening (18–21), and Night (22–04). The Afternoon segment dominates with the highest total orders approaching 500, followed by Morning at around 400 orders. Evening and Night segments have significantly lower volumes, indicating that customers tend to be most active during midday to afternoon hours.

#### <u>B3. Running Total Orders Throughout the Day (Area Chart)</u>

Visualizes the cumulative accumulation of orders throughout the day from hour 0 to hour 23. The chart shows slow growth in the early morning hours, a significant increase starting around 7–8 AM, accelerated accumulation between 9 AM and 5 PM, and a slowdown after 6 PM. This pattern reinforces the finding that the majority of transactions occur during daytime hours.

#### <u>B4. Hourly Order Performance vs Daily Average (Sankey Chart)</u>

Classifies each hour into two categories: Above Average and Below Average based on comparison against the daily average. From this visualization, hours that fall above the average include 8, 9, 10, 12, 14, 15, and 17 — all concentrated in the morning to afternoon period. Conversely, the early morning hours (0, 6) and evening hours (18, 19, 20, 22) fall below the average, confirming that order activity is centered around productive daytime hours.

---

### <u>3. Department Breakdown</u>

Analyzes the contribution of each department to total orders. The Pareto chart shows which departments account for the largest share of total transactions — highly useful for stock allocation and promotional budget decisions.

![alt text](asset_documentation/image-13.png)
![alt text](asset_documentation/image-14.png)

#### <u>C1. Department Contribution to Total Transactions (Donut Chart)</u>

Displays the proportional contribution of each department to the total 1,072 transactions. The produce department dominates at 28.82%, followed by dairy eggs at 15.58%, beverages at 9.51%, snacks at 8.58%, and frozen at 6.44%. Other departments each contribute less than 5%, with the remaining group (Others) accounting for 10.35% of total transactions.

#### <u>C2. Concentration Risk Analysis (Scalar)</u>

Displays a concentration risk metric to measure how much transaction volume is concentrated in a small number of departments. The analysis shows a total of 20 departments, with 5 classified as high volume. These five departments account for 68.94% of all transactions, indicating a relatively high concentration and a potential risk if any of these key departments experiences disruption.

#### <u>C3. Anomaly Detection by Total Orders (Bar Chart)</u>

Identifies departments with order volumes far above the average that can be classified as anomalies. The produce department stands out significantly as an anomaly, with total orders far exceeding all other departments. Most other departments fall within a relatively uniform normal range, highlighting the extraordinary dominance of produce compared to other categories.

#### <u>C4. Department with Total Orders Above and Below Average (Sankey Chart)</u>

Classifies all departments into two groups based on comparison against the average total order count. Departments above the average include produce, dairy eggs, beverages, and snacks, while all others fall below the average. This Sankey visualization makes it easy to identify which departments should receive priority attention in procurement and promotional strategies.

#### <u>C5. Total Orders Gap to Next Department (Combo Chart)</u>

Displays the gap in order count between each department and the next in the ranked list. Based on the visualization, the largest gap occurs between produce and dairy eggs, highlighting produce's extraordinary dominance. The gap then narrows gradually as the department ranking decreases, forming a curve that drops steeply at the top and flattens toward the bottom.

---

### <u>4. Raw Transaction Summary</u>

An overview of all transaction data, including unique order counts, active users, unique products, day-of-week distribution, and various in-depth customer behavior analyses.

![alt text](asset_documentation/image-15.png)
![alt text](asset_documentation/image-16.png)
![alt text](asset_documentation/image-17.png)

#### <u>D1. All Transaction Count (Scalar)</u>

Displays a summary of overall dataset statistics. Total rows reach 1,084, with 100 unique orders and 100 unique users, and 825 unique products. The average number of items per order is 10.84, with a maximum of 50 items in a single order and a minimum of 1, providing a general overview of the scale and characteristics of this dataset.

#### <u>D2. Order Distribution per Day of the Week (Combo Chart)</u>

Displays the distribution of total orders by day of the week. Wednesday and Monday record the highest order volumes, while Sunday and Friday have lower volumes. This pattern suggests that customers tend to purchase groceries at the beginning and middle of the week.

#### <u>D3. Top 10 Most Active User by Order Count (Bar Chart)</u>

Identifies the 10 most active users based on number of orders, total items purchased, and average days between orders. User 188,196 is the most active user by order count, while user 55,276 shows the longest average interval between orders with the most total items — indicating a shopping pattern of large baskets but lower frequency.

#### <u>D4. Product Reorder Distribution (Area Chart)</u>

Displays the distribution of reorder rate, the number of reordered items, and total items per department. The produce department shows a very dominant peak across all three metrics, followed by a significant spike in the meat seafood department. This visualization helps identify which departments have strong customer loyalty alongside high transaction volumes.

#### <u>D5. Top 10 Most Popular Aisle (Donut Chart)</u>

Displays the 10 aisles most frequently appearing in transactions. Fresh fruits dominate at 17.06%, followed by fresh vegetables at 16.05%, packaged vegetables at 11.71%, and yogurt at 11.04%. Overall, aisles related to fresh products and dairy dominate customer shopping preferences out of the 299 total transactions analyzed.

#### <u>D6. The Most Consistent Users Reorder (Bar Chart)</u>

Identifies users who most consistently reorder based on a combination of total items, reorder rate, and unique products. The results show that users with a high reorder rate are not always the users with the most total items, indicating variation in loyalty patterns among the most active users.

#### <u>D7. Department Contribution to Total Transactions (Bar Chart)</u>

Displays each department's contribution to total transactions in a bar chart sorted from highest to lowest. Produce again dominates with values far above all other departments, followed by dairy eggs and snacks. This visualization provides a more readable perspective for directly comparing transaction volume across departments.

#### <u>D8. Customer Shopping Intensity by Time (Table)</u>

Displays customer shopping intensity by time period, covering total orders, number of active users, and average items per order. The Afternoon segment records 44 orders with 44 active users and an average of 10.59 items per order. Morning has 29 orders with an average of 10.66 items, Evening has 18 orders with an average of 8.44 items, and Night has only 9 orders but the highest average item count at 17.44, indicating that nighttime shoppers tend to buy in larger quantities per transaction.

#### <u>D9. Department with the Most Loyal Customers (Line Chart)</u>

Compares the total number of users and average reorder rate per department to identify categories with the most loyal customer base. Several departments show high reorder rates despite having fewer users than the major departments. The observed pattern suggests that customer loyalty does not always correlate directly with overall department popularity.

#### <u>D10. Customer Retention by Interval Order (Donut Chart)</u>

Classifies customer reorder patterns based on the time interval between two orders. Out of 95 data points analyzed, the majority of customers (61.1%) shop on a weekly basis (Weekly), followed by Monthly at 20.0%, and Biweekly at 18.9%. This finding shows that most customers have a consistent weekly shopping cycle, making weekly strategies the most relevant approach for retention campaigns.

#### <u>D11. Market Basket Depth Analysis (Bar Chart)</u>

Analyzes shopping basket depth across three categories: Large Basket, Medium Basket, and Small Basket. The Large Basket segment shows the highest total orders but also the largest average items per order, indicating a customer segment that shops in large quantities at once. Medium Basket and Small Basket have higher total items than Large Basket in aggregate, showing that mid-sized and small basket purchases dominate in terms of total transaction volume overall.

---

## Insights

The following are the key insights derived from this pipeline analysis:

### Product Insights

Looking at the overall picture, product performance is not as simple as "who gets bought the most" or "who has the highest reorder rate." To get a more complete view, we need to examine the combination of transaction volume, repurchase rate, performance balance, and department category.

Across all analyses, the produce department consistently shows the strongest overall performance. Products in this category are not only bought frequently, but also succeed in bringing customers back to purchase the same items again. That said, other categories still exhibit their own unique characteristics — such as products that perform well on first purchase but are weak on reorder, or products with high loyalty despite modest order volumes.

### Time Insights

- **10:00 AM – 2:00 PM** is the peak order window — the ideal time for push notifications and flash sale promotions.
- The **Afternoon (12–17)** segment contributes the largest share of total daily orders.
- Orders during **1:00 AM – 5:00 AM (Night)** are very low — the ideal window for pipeline maintenance without disruption.

### Department Insights

- **Produce, Dairy Eggs, and Snacks** consistently rank in the top 3 departments.
- Pareto analysis shows that approximately 5 departments already account for more than 70% of total orders.
- The **bulk** and **other** departments have the lowest order counts, making them candidates for reduced display slot allocation.

### Day of Week Insights

- **Sunday and Monday** have the highest order volumes — customers shop for weekly necessities at the start of the week.
- Orders tend to dip in the middle of the week (Wednesday–Thursday).
- Saturday sees an increase again but does not reach Sunday's level.

### Reorder Insights

- The overall reorder rate is approximately 59% — more than half of all transactions are repeat purchases, indicating **strong customer retention**.
- `days_since_prior_order` is most commonly at **7** and **30**, showing consistent weekly and monthly shopping patterns.
- Users with an order_number greater than 10 tend to have reorder rates well above average.

---

## End-to-End Running Guide

Follow these steps to run the pipeline from scratch:

### Prerequisites

```bash
# Make sure the following are installed:
docker --version        # >= 24.x
docker compose version  # >= 2.x
python --version        # >= 3.11
```

### Step 1 - Clone & Setup

```bash
git clone <repo-url>
cd <repo-folder>

# Copy environment file
cp .env.example .env
```

### Step 2 - Build & Start Containers

```bash
# Build all services
docker compose build

# Start all containers
docker compose up -d

# Check status
docker compose ps
```

### Step 3 - Initialize Airflow

```bash
# Initialize Airflow database (only once)
docker compose exec airflow-webserver airflow db init

# Create admin user
docker compose exec airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### Step 4 - Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| ClickHouse HTTP | http://localhost:8123 | admin / rahasia |
| Metabase | http://localhost:3000 | Set up on first launch |

### Step 5 - Trigger the Pipeline

```bash
# Via CLI
docker compose exec airflow-webserver \
    airflow dags trigger task2_orders_pipeline

# Or via Airflow UI -> DAGs -> task2_orders_pipeline -> Trigger
```

### Step 6 - Monitor

```bash
# View task logs live
docker compose exec airflow-webserver \
    airflow tasks logs task2_orders_pipeline process_orders_analytics <run_id>

# Or monitor in Airflow UI -> DAG Runs -> select run -> Task Logs
```

### Step 7 - Verify ClickHouse

```bash
# Enter the ClickHouse client
docker compose exec clickhouse-server clickhouse-client \
    --user admin --password rahasia

# Check data ingestion
SELECT count() FROM analytics.raw_orders;
SELECT count() FROM analytics.product_analytics;
SELECT count() FROM analytics.hourly_analytics;
SELECT count() FROM analytics.department_analytics;
```

### Step 8 - Setup Metabase

1. Open http://localhost:3000
2. Select **Add a database** → ClickHouse
3. Fill in the connection details:
   - Host: `clickhouse-server`
   - Port: `9000`
   - Database: `analytics`
   - User: `admin`
   - Password: `rahasia`
4. Click **Save** → wait for sync to complete
5. Start building your dashboard from queries A1–D11

### Troubleshooting

```bash
# Restart a specific service
docker compose restart clickhouse-server

# View container logs
docker compose logs -f airflow-scheduler

# Reset the pipeline (delete old parquet files)
rm -rf ./data_lake/task2/*.parquet

# Enter the Airflow container for manual debugging
docker compose exec airflow-webserver bash
python /opt/airflow/dags/scripts/process_task2_spark.py
```

---

## Closing

This pipeline successfully implements a **Big Data end-to-end** architecture, spanning ingestion, large-scale processing with Spark, warehousing in ClickHouse, and interactive visualization in Metabase — all automatically orchestrated by Apache Airflow.

Potential future enhancements:

- [ ] Add **streaming ingestion** using Kafka
- [ ] Implement **data quality checks** (Great Expectations)
- [ ] Add a **user_analytics** table for cohort analysis
- [ ] Integrate **alerting** in Airflow when the pipeline fails
- [ ] Apply ClickHouse table **partitioning** by date for faster queries
- [ ] Add an **ML model** for predicting products likely to be reordered

---

<div align="center">

**Institut Teknologi Sepuluh Nopember**
<br>
Department of Informatics - 2024
<br><br>

| | |
|---|---|
| **Afarrel Febryan Putra Andy** | 5025241137 |
| **Ibrahim Ferel** | 5025241049 |

<br>

## Task Division
1. Ibrahim Ferel: Wrote the API fetch script, built and set up the Airflow pipeline, composed Metabase queries and dashboard, wrote the README documentation.
2. Afarrel Febryan G.: Built and set up the Airflow pipeline, composed Metabase queries and dashboard, assisted with README documentation.

*Bismillah Admin Lab Manajemen Cerdas Informasi <3*

</div>
