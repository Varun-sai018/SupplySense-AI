# Review-2: Baseline Forecasting Model & Evaluation Metrics

## 1. Why a Baseline is Required
In applied machine learning for supply chain forecasting, an advanced model (such as XGBoost) cannot be deemed successful simply by achieving a low error in isolation. A simple, deterministic baseline model is required to establish the reference benchmark. Any machine learning model developed in subsequent phases must prove statistically meaningful improvement over this reference baseline to justify its architectural and operational complexity.

> **Important:** The baseline model described here is **NOT** an AI model. It is a deterministic reference forecasting heuristic (Previous-Period Naive Forecast) used strictly as a benchmark.

## 2. Baseline Method
The baseline method is a **One-Step-Ahead Rolling Naive Forecast**:
$$\hat{y}_{c, t} = y_{c, t-1}$$
For a given product category $c$ and calendar week $t$, the predicted demand $\hat{y}_{c, t}$ is set to the observed demand of the immediately preceding calendar week $t-1$ (i.e., $t - 7\text{ days}$).

## 3. Target Definition
- **Target Variable:** `demand`
- **Definition:** The total quantity of product items in delivered orders for a specified product category during the given ISO calendar week.

## 4. Product-Category Aggregation
The Olist dataset contains 32,951 unique product IDs across 91 weeks. At the individual product level, the data is extremely sparse (the vast majority of products appear in only 1 or 2 isolated weeks). Aggregating demand by `product_category_name` yields a robust, continuous weekly signal across 73 product categories.

## 5. Weekly Granularity
- Temporal unit: ISO calendar week (Monday to Sunday).
- Format: `YYYY-MM-DD` (date of Monday, e.g., `2018-02-19`).
- Across the validation and test horizons, calendar weeks advance contiguously with exactly 7-day increments and zero gaps.

## 6. Chronological Dataset Split
The dataset of 4,372 rows across 91 calendar weeks was split chronologically:
- **Train:** 63 weeks (2016-09-12 to 2018-02-12) — 2,795 rows (70%)
- **Validation:** 14 weeks (2018-02-19 to 2018-05-21) — 807 rows (15%)
- **Test:** 14 weeks (2018-05-28 to 2018-08-27) — 770 rows (15%)

## 7. How the Validation Boundary is Handled
- For the first validation week ($t = \text{2018-02-19}$), the preceding calendar week $t-1$ is $\text{2018-02-12}$, which is the final week of the training period.
- Category demand for week $\text{2018-02-12}$ is drawn directly from `train.csv`.
- If a category had no recorded demand in that final training week (e.g., `portateis_cozinha_e_preparadores_de_alimentos`), its observed historical demand for that week was 0.
- For all subsequent validation weeks, the one-step-ahead rolling prediction uses the actual observation from the preceding validation week.

## 8. How the Test Boundary is Handled
- For the first test week ($t = \text{2018-05-28}$), the preceding calendar week $t-1$ is $\text{2018-05-21}$, which is the final week of the validation period.
- Category demand for week $\text{2018-05-21}$ is drawn directly from `validation.csv`.
- For subsequent test weeks, the rolling naive forecast sequentially uses the known observation from the preceding test week.

## 9. Missing Category-Week Handling
Because the raw data consists of realized purchase events from delivered orders, a missing `(product_category, calendar_week)` pair reflects **zero realized demand** (no customer orders were placed for that category in that calendar week). Consequently, when querying $y_{c, t-1}$, if no record exists for category $c$ in week $t-1$, the demand is assigned as **0**.

## 10. Leakage Prevention
The forecasting pipeline strictly enforces causal temporal ordering:
- Predictions for week $t$ depend exclusively on historical data from $\le t-1$.
- No concurrent or future target observations from week $t$ or beyond are accessible to the model during prediction generation.
- There is zero temporal overlap between Train, Validation, and Test partitions.

## 11. Evaluation Metrics Definitions
Standard regression metrics are calculated without premature rounding:

- **Mean Absolute Error (MAE):**
  $$\text{MAE} = \frac{1}{N} \sum_{i=1}^{N} |y_i - \hat{y}_i|$$
  Measures the average magnitude of prediction errors in units of items ordered.

- **Root Mean Squared Error (RMSE):**
  $$\text{RMSE} = \sqrt{\frac{1}{N} \sum_{i=1}^{N} (y_i - \hat{y}_i)^2}$$
  Penalizes larger prediction deviations more heavily than MAE.

- **Coefficient of Determination ($R^2$):**
  $$R^2 = 1 - \frac{\sum_{i=1}^{N} (y_i - \hat{y}_i)^2}{\sum_{i=1}^{N} (y_i - \bar{y})^2}$$
  Measures the proportion of variance in demand explained by the forecasting model relative to a naive mean predictor.

*(Note: These are regression performance metrics, not classification "accuracy".)*

## 12. Actual Validation Results
Evaluated on `validation.csv` (807 predictions):
- **Prediction Count:** 807
- **MAE:** **7.9802**
- **RMSE:** **15.3196**
- **$R^2$:** **0.8992**

## 13. Actual Test Results
Evaluated on `test.csv` (770 predictions):
- **Prediction Count:** 770
- **MAE:** **9.4636**
- **RMSE:** **17.9912**
- **$R^2$:** **0.8351**

## 14. Performance Summary Table
| Model | Split | Observations | MAE | RMSE | $R^2$ |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Naive Baseline (T-1)** | **Validation** | 807 | **7.9802** | **15.3196** | **0.8992** |
| **Naive Baseline (T-1)** | **Test** | 770 | **9.4636** | **17.9912** | **0.8351** |

Artifacts saved:
- Predictions: `data/processed/baseline_validation_predictions.csv`
- Predictions: `data/processed/baseline_test_predictions.csv`
- Metrics: `ml/results/baseline_metrics.csv`
- Metrics: `ml/results/baseline_metrics.json`

## 15. Limitations
1. **Lag Vulnerability:** A pure previous-period naive forecast lags by exactly one week whenever sudden demand spikes or promotions occur (e.g., Black Friday).
2. **Trend & Seasonality Insensitivity:** It cannot anticipate annual holiday seasonality or gradual multi-month trend changes.
3. **Exogenous Factors:** It does not incorporate pricing, freight, customer review ratings, or seller distribution.

## 16. How XGBoost Will Later Be Compared
In subsequent phases after Review-2:
1. Feature engineering will compute multi-week rolling lags, historical moving averages, category momentum, and seasonal calendar encodings.
2. An **XGBoost Regressor** will be trained on `train.csv`, tuned on `validation.csv`, and finally evaluated on `test.csv`.
3. To validate that the ML model provides genuine business value, the XGBoost model must achieve:
   - Lower MAE than the Naive Baseline ($< 9.46$ on Test)
   - Lower RMSE than the Naive Baseline ($< 17.99$ on Test)
   - Higher $R^2$ than the Naive Baseline ($> 0.835$ on Test)
