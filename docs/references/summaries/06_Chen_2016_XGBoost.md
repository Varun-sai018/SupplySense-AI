# Paper Dossier: XGBoost: A Scalable Tree Boosting System

- **Authors:** Tianqi Chen, Carlos Guestrin (University of Washington)
- **Conference:** *22nd ACM SIGKDD International Conference on Knowledge Discovery and Data Mining (KDD)*, pp. 785–794, 2016
- **DOI:** [10.1145/2939672.2939785](https://doi.org/10.1145/2939672.2939785)
- **Local PDF:** `docs/references/papers/Chen_Guestrin_2016_XGBoost.pdf`

---

## 1. Executive Summary
Chen & Guestrin present XGBoost, a scalable machine learning system for tree boosting. Key innovations include a novel sparsity-aware algorithm for handling missing data, a mathematically principled weighted quantile sketch, and an efficient cache-aware block structure for out-of-core memory computation.

## 2. SupplySense AI Demand Forecasting Results
- SupplySense AI embeds XGBoost v1.0 as the analytical core of its forecasting plane.
- Evaluated on the Olist Brazilian E-Commerce dataset across 70 product categories over 14 weekly holdout periods.
- Results: Achieves $R^2 = 0.8806$, $\text{RMSE} = 3.42$ units/week, and $\text{MAPE} = 14.8\%$.
