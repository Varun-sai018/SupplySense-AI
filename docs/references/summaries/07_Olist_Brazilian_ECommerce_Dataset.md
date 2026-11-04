# Benchmark Dataset Dossier: Brazilian E-Commerce Public Dataset by Olist

- **Curators:** Olist & André Sionek
- **Platform:** Kaggle Public Benchmark Dataset
- **URL:** [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## 1. Dataset Profile
- **Total Customer Orders:** 99,441 verified orders from 2016 to 2018.
- **Relational Tables:** 8 tables (orders, order_items, order_payments, customers, products, sellers, geolocation, order_reviews).
- **Product Coverage:** 32,951 distinct SKUs mapped into 70 standardized merchandise categories.

## 2. Partitioning Strategy in SupplySense AI
- **Historical Baseline Partition (80.3% / 79,885 orders):** 2016 to May 28, 2018. Used for computing initial moving averages, supplier lead times, and baseline feature tables.
- **Streaming Evaluation Partition (19.7% / 19,556 orders):** May 28, 2018 to August 27, 2018. Replayed as live streaming Kafka events to evaluate AST quorum triggering and 14-week weekly demand forecasting.
