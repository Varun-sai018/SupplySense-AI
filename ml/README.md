# SupplySense AI - Machine Learning Forecasting Pipeline

## Overview
This package contains the data preparation, chronological dataset splitting, feature engineering, and baseline forecasting models for SupplySense AI demand prediction.

## Directory Structure
```
ml/
├── __init__.py
├── README.md
├── baseline/
│   ├── __init__.py
│   └── naive_baseline.py    # Rolling one-step-ahead naive baseline forecast & evaluation
├── data/
│   ├── __init__.py
│   ├── prepare_dataset.py   # Extracts Olist data from MySQL & aggregates weekly demand
│   └── split_dataset.py     # Performs strict chronological Train/Val/Test split
└── results/
    ├── baseline_metrics.csv # Generated evaluation metrics table (MAE, RMSE, R²)
    └── baseline_metrics.json# Detailed JSON metrics with split-level breakdowns
```

## Forecasting Formulation
- **Target Entity:** Product Category (`product_category_name`)
- **Time Granularity:** Weekly (ISO calendar weeks starting Monday)
- **Target Variable:** `demand` (Count of delivered items ordered within the week)
- **Evaluation Strategy:** Chronological out-of-time evaluation (no lookahead / temporal leakage)

---

## Pipeline Execution

### 1. Prepare Forecasting Dataset
Extracts delivered orders, order items, and products from the centralized MySQL database and produces `data/processed/forecasting_dataset.csv`:
```bash
python ml/data/prepare_dataset.py
```
*Note: Requires MySQL running with ingested Olist tables (`olist_orders`, `olist_order_items`, `olist_products`, `product_category_name_translation`).*

### 2. Split Dataset Chronologically
Splits `data/processed/forecasting_dataset.csv` into 70% Train, 15% Validation, and 15% Test without temporal leakage:
```bash
python ml/data/split_dataset.py
```
**Output files:**
- `data/processed/train.csv` (70% chronologically, weeks 2016-09-12 to 2018-02-12)
- `data/processed/validation.csv` (15% chronologically, weeks 2018-02-19 to 2018-05-21)
- `data/processed/test.csv` (15% chronologically, weeks 2018-05-28 to 2018-08-27)

### 3. Run Naive Baseline Model & Metrics
Computes the one-step-ahead rolling persistence forecast ($\hat{y}_{t} = y_{t-1}$) across Validation and Test sets:
```bash
python ml/baseline/naive_baseline.py
```
**Generated outputs:**
- `data/processed/baseline_validation_predictions.csv`
- `data/processed/baseline_test_predictions.csv`
- `ml/results/baseline_metrics.csv`
- `ml/results/baseline_metrics.json`

**Verified Benchmark Metrics:**
- **Validation:** MAE = 7.9802 | RMSE = 15.3196 | R² = 0.8992
- **Test:** MAE = 9.4636 | RMSE = 17.9912 | R² = 0.8351

---

## Unit Testing
To run all ML unit tests (dataset splitting and naive baseline):
```bash
python -m unittest tests/unit/test_dataset_split.py tests/unit/test_naive_baseline.py
```
Or execute the full repository test suite:
```bash
python -m unittest discover -s tests
```
