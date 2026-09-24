# Review-2: Machine Learning Test & Validation Summary

This document summarizes the automated verification and empirical validation results for the Review-2 Machine Learning deliverables (Dataset Preparation, Chronological Split, and Baseline Metrics).

## Test Execution Summary

| Test ID | Description | Expected Result | Actual Result | Status |
| :--- | :--- | :--- | :--- | :--- |
| **ML-001** | Dataset preparation | Ingest Olist orders, items, products; filter to delivered status; produce non-empty dataset | Successfully extracted 108,660 items across 96,478 orders; produced 4,372 rows in `forecasting_dataset.csv` | **PASS** |
| **ML-002** | Target generation | Weekly item quantity aggregated per product category; valid non-negative integers | Generated `demand` column spanning 73 categories and 91 calendar weeks; zero negative values | **PASS** |
| **ML-003** | Chronological split | Split dataset into 70% Train, 15% Validation, 15% Test strictly by calendar weeks | Train: 2,795 rows (63 wks), Val: 807 rows (14 wks), Test: 770 rows (14 wks); 100% row preservation | **PASS** |
| **ML-004** | Temporal leakage check | Train dates strictly precede Validation; Validation dates strictly precede Test; 0 shared dates | Train Max: 2018-02-12 < Val Min: 2018-02-19; Val Max: 2018-05-21 < Test Min: 2018-05-28; 0 date overlap | **PASS** |
| **ML-005** | Naive baseline prediction | Generate rolling one-step-ahead predictions for validation and test horizons | Generated 807 validation predictions and 770 test predictions matching 100% of target rows | **PASS** |
| **ML-006** | Validation metrics | Compute MAE, RMSE, and $R^2$ on validation set | MAE: 7.9802, RMSE: 15.3196, $R^2$: 0.8992 | **PASS** |
| **ML-007** | Test metrics | Compute MAE, RMSE, and $R^2$ on test set | MAE: 9.4636, RMSE: 17.9912, $R^2$: 0.8351 | **PASS** |
| **ML-008** | Baseline unit tests | 10 deterministic unit tests covering boundary handling, metric calculations, and causality | 10/10 tests passed in `tests/unit/test_naive_baseline.py` | **PASS** |

---

## Detailed Unit Test Breakdown

### Baseline Unit Tests (`tests/unit/test_naive_baseline.py`)
- **BASE-001:** Previous-period demand is used correctly ($\hat{y}_t = y_{t-1}$) — **PASS**
- **BASE-002:** No future or concurrent demand is used (strict causality) — **PASS**
- **BASE-003:** First validation prediction correctly draws from final training week — **PASS**
- **BASE-004:** First test prediction correctly draws from final validation week — **PASS**
- **BASE-005:** Category separation preserved (no cross-category leakage) — **PASS**
- **BASE-006:** Missing previous-week observation correctly produces 0 demand — **PASS**
- **BASE-007:** MAE calculation verified against mathematical formulation — **PASS**
- **BASE-008:** RMSE calculation verified against mathematical formulation — **PASS**
- **BASE-009:** $R^2$ calculation verified against mathematical formulation — **PASS**
- **BASE-010:** Empty/mismatched input properly raises `ValueError` — **PASS**

### Dataset Split Unit Tests (`tests/unit/test_dataset_split.py`)
- 9/9 synthetic unit tests passed verifying chronological partitioning, proportion logic, boundary validation, and CSV round-tripping — **PASS**

---

## Baseline Performance Reference Table

| Model | Partition | Prediction Count | MAE | RMSE | $R^2$ |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Naive Baseline (T-1)** | **Validation** | 807 | **7.9802** | **15.3196** | **0.8992** |
| **Naive Baseline (T-1)** | **Test** | 770 | **9.4636** | **17.9912** | **0.8351** |

Artifacts generated:
- `data/processed/baseline_validation_predictions.csv`
- `data/processed/baseline_test_predictions.csv`
- `ml/results/baseline_metrics.csv`
- `ml/results/baseline_metrics.json`
