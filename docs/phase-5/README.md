# SupplySense AI - Phase 5: XGBoost Demand Forecasting Model

## Executive Summary
Phase 5 upgrades the downstream **Demand Forecast Pipeline** from a naive persistence baseline to a machine-learning forecasting pipeline using **XGBoost (`XGBRegressor`)**, while preserving the naive persistence baseline as the benchmark.

---

## Machine Learning Pipeline Architecture

```mermaid
flowchart TD
    subgraph Data["1. Historical Ingestion & Splitting"]
        A[Olist Raw Tables] --> B[data/processed/forecasting_dataset.csv]
        B --> C[ml/features/engineer_features.py]
    end

    subgraph Features["2. Leak-Free Feature Engineering"]
        C --> D[Continuous Weekly Grid Imputation: demand=0]
        D --> E[Lags: T-1, T-2, T-4]
        D --> F[Rolling: 4w Mean/Std, 8w Mean over T-4..T-1]
        D --> G[Trend Ratio: (lag_1 + 1)/(rolling_4w + 1)]
        D --> H[Calendar: Month, Week, Quarter]
        D --> I[Category Deterministic Encoding]
        E & F & G & H & I --> J[data/processed/forecasting_features.csv]
    end

    subgraph Training["3. Model Training & Evaluation"]
        J --> K[Train Split: <= 2018-02-12]
        J --> L[Validation Split: 2018-02-19 to 2018-05-21]
        J --> M[Test Split: 2018-05-28 to 2018-08-27]
        K & L --> N[XGBRegressor Training with Early Stopping]
        N --> O[Evaluate on Validation & Test]
    end

    subgraph Outputs["4. Serialization & Benchmark Comparison"]
        O --> P[ml/models/xgboost_demand_model.json]
        O --> Q[data/processed/xgboost_validation_predictions.csv]
        O --> R[data/processed/xgboost_test_predictions.csv]
        O --> S[ml/results/xgboost_metrics.json]
        O --> T[ml/results/model_comparison.json]
    end
```

---

## Benchmark Comparison: Naive Baseline vs. XGBoost

### Validation Set Metrics (807 evaluation rows)
| Metric | Naive Baseline | XGBoost Regressor | Difference |
|---|---:|---:|---|
| **MAE** | 7.9802 | 8.3003 | +0.3201 |
| **RMSE** | 15.3196 | 16.6854 | +1.3658 |
| **R²** | 0.8992 | 0.8804 | -0.0188 |

### Test Set Metrics (770 evaluation rows)
| Metric | Naive Baseline | XGBoost Regressor | Difference |
|---|---:|---:|---|
| **MAE** | 9.4636 | 9.4257 | **-0.0379 (Improvement)** |
| **RMSE** | 17.9912 | 18.6006 | +0.6094 |
| **R²** | 0.8351 | 0.8238 | -0.0113 |

---

## Leakage Prevention Analysis

1. **Strict Lagging**: Rolling windows and lags are computed using `shift(1)`, guaranteeing that features for week $T$ only reference $[T-8, T-1]$.
2. **Deterministic Category Encoding**: The category-to-integer mapping is fitted strictly on categories appearing in the training partition ($\le \text{2018-02-12}$), mapping unseen categories in validation/test to `-1`.
3. **Temporal Boundary Integrity**:
   * Train: `2016-09-12` to `2018-02-12`
   * Validation: `2018-02-19` to `2018-05-21`
   * Test: `2018-05-28` to `2018-08-27`
4. **Independent Target Evaluation**: Validation and Test target actuals are never used during model fitting. Early stopping monitors validation RMSE without altering training partition boundaries.

---

## Generated Artifacts
* Model File: `ml/models/xgboost_demand_model.json`
* Features Dataset: `data/processed/forecasting_features.csv`
* Predictions:
  * `data/processed/xgboost_validation_predictions.csv`
  * `data/processed/xgboost_test_predictions.csv`
* Results:
  * `ml/results/xgboost_metrics.json`
  * `ml/results/model_comparison.json`
