# Review-2: ML Forecasting Dataset Preparation & Chronological Split

## 1. Dataset Source
The forecasting dataset is derived from the **Olist Brazilian E-Commerce Public Dataset** stored in MySQL.
> **Important Note:** Olist is a historical dataset. It is not itself a real-time data source.
> This historical dataset is utilized for training and validating demand forecasting models, and is decoupled from the simulated real-time event-driven update mechanism demonstrated in Objective-1.

## 2. Tables Used
- `olist_orders`: Source for order identifiers, order status, and purchase timestamp (`order_purchase_timestamp`).
- `olist_order_items`: Source for itemized line items linking `order_id` and `product_id`. Each row represents one purchased item unit.
- `olist_products`: Source for `product_id` and category classification (`product_category_name`).

## 3. Target Definition
- **Entity:** Product Category (`product_category_name`).
  *(Note on granularity selection: Individual product IDs are overwhelmingly sparse, with over 32,000 distinct products appearing across 91 weeks, mostly in only 1-2 weeks. Aggregating at the category level provides a robust, dense historical signal spanning 73 categories across continuous weekly horizons).*
- **Target Column:** `demand`
- **Definition:** Total quantity of product items ordered in delivered orders for a given product category during the specified ISO calendar week.

## 4. Time Granularity
- **Granularity:** Weekly (ISO week, starting on Monday).
- Columns generated:
  - `product_category`: Category name string.
  - `year_week`: ISO format (e.g., `2017-W05`).
  - `week_start_date`: Date of the Monday for that ISO week (`YYYY-MM-DD`).
  - `demand`: Non-negative integer count of items ordered.

## 5. Order Status Handling
Order statuses present in `olist_orders`:
- `delivered`: 96,478 orders (included)
- `shipped`: 1,107 orders (excluded)
- `canceled`: 625 orders (excluded)
- `unavailable`: 609 orders (excluded)
- `invoiced`: 314 orders (excluded)
- `processing`: 301 orders (excluded)
- `created`: 5 orders (excluded)
- `approved`: 2 orders (excluded)

**Decision:**
Only completed, fulfilled demand (`order_status = 'delivered'`) is included. Orders that were cancelled, unavailable, or still pending in the pipeline are excluded so unfulfilled requests do not distort true realized demand figures.
- Total raw order items: 112,650
- Matched items in delivered orders with valid categories: 108,660
- Excluded (not delivered): 2,453 items
- Excluded (missing category): 1,537 items

## 6. Data Preparation Pipeline
Implemented in `ml/data/prepare_dataset.py`:
1. Connects to MySQL using centralized `common.database.get_connection()`.
2. Queries delivered orders, order items, and categorized products without heavy MySQL table joins on unindexed text fields.
3. Maps purchase timestamp to ISO calendar weeks.
4. Aggregates weekly order item count per category.
5. Sorts chronologically by `(week_start_date, product_category)`.
6. Validates non-negativity, presence of required columns, and non-empty output.
7. Saves output to `data/processed/forecasting_dataset.csv`.

## 7. Train / Validation / Test Methodology
Implemented in `ml/data/split_dataset.py`:
- Strict chronological ordering based on distinct time periods (`week_start_date`):
  - **Train:** Earliest 70% of chronological weeks (63 weeks).
  - **Validation:** Next 15% of chronological weeks (14 weeks).
  - **Test:** Final 15% of chronological weeks (14 weeks).
- Data is **never randomly shuffled**, strictly preserving the temporal sequence:
  $$\text{TRAIN} \longrightarrow \text{VALIDATION} \longrightarrow \text{TEST}$$

## 8. Actual Dataset Sizes & Row Counts
- Total records in prepared dataset: **4,372 rows**
- Distinct categories: **73**
- Distinct weeks: **91 weeks**

| Split | Proportion of Weeks | Distinct Weeks | Row Count | Start Date | End Date |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Train** | 70% | 63 weeks | 2,795 rows | 2016-09-12 | 2018-02-12 |
| **Validation** | 15% | 14 weeks | 807 rows | 2018-02-19 | 2018-05-21 |
| **Test** | 15% | 14 weeks | 770 rows | 2018-05-28 | 2018-08-27 |
| **Total** | 100% | 91 weeks | 4,372 rows | 2016-09-12 | 2018-08-27 |

## 9. Leakage Prevention
- **Temporal Non-Overlap Verification:**
  - `Train End Date (2018-02-12) < Validation Start Date (2018-02-19)`
  - `Validation End Date (2018-05-21) < Test Start Date (2018-05-28)`
  - Zero intersection between the dates of Train, Validation, and Test sets.
- **Feature Boundary:** Future demand observations are strictly prohibited from entering past features.

## 10. Automated Validation
Automated tests in `tests/unit/test_dataset_split.py` verify:
1. Chronological order preservation.
2. Complete partition of rows (no loss or duplicate rows).
3. Exact non-overlapping date sets.
4. Schema and non-negative demand values.
5. Error raising on temporal inversion or overlap.

## 11. Limitations & Next Steps
- Current preparation aggregates demand by category and week.
- Next step: Implement baseline forecasting model (Naive baseline) and calculate evaluation metrics (MAE, RMSE, R²).
