# SupplySense AI — Academic Reference Papers & Technical Dossier

This folder houses the academic reference literature, original downloaded PDF papers, BibTeX bibliographies, and analytical summary notes supporting the research paper:

> **"SupplySense AI: An Event-Conditioned Data Pipeline Orchestrator with Dependency-Aware Execution for Supply Chain Analytics"**  
> *Authors: Gangavaram Mounika, Borra Sathyavathi, V. Varun Sai, Konatham Sandeep*  
> *Department of Computer Science and Engineering, Koneru Lakshmaiah Education Foundation, Vijayawada, India*

---

## 📁 Repository Directory Structure

```
docs/references/
├── REFERENCE_PAPERS_INDEX.md        # Comprehensive index and cross-reference matrix (this document)
├── references.bib                   # Complete BibTeX citations for LaTeX and reference managers
├── papers/                          # Downloaded PDF reference papers
│   ├── Barika_2019_Orchestrating_Big_Data_Workflows.pdf
│   ├── Chen_Guestrin_2016_XGBoost.pdf
│   ├── Dean_Ghemawat_2004_MapReduce.pdf
│   ├── Kreps_2011_Kafka.pdf
│   ├── Wang_2016_Big_Data_Supply_Chain.pdf
│   └── Zaharia_2010_Spark.pdf
└── summaries/                       # Detailed structured engineering dossiers for each citation
    ├── 01_Wang_2016_Big_Data_Analytics_in_Logistics.md
    ├── 02_Barika_2019_Orchestrating_Big_Data_Workflows.md
    ├── 03_Airflow_Data_Aware_Scheduling.md
    ├── 04_Kreps_2011_Kafka_Distributed_Messaging.md
    ├── 05_Debezium_Change_Data_Capture.md
    ├── 06_Chen_2016_XGBoost.md
    ├── 07_Olist_Brazilian_ECommerce_Dataset.md
    ├── 08_Dean_2004_MapReduce.md
    ├── 09_Zaharia_2010_Apache_Spark.md
    └── 10_Hyndman_2021_Forecasting_Principles.md
```

---

## 📚 Master Citation Matrix

| Ref # | Primary Authors & Year | Publication Venue | Core Focus Area | Local PDF in `papers/` | Relevance to SupplySense AI |
|---|---|---|---|:---:|---|
| **[1]** | Wang et al. (2016) | *Int. J. Production Economics* | Supply Chain Big Data Analytics | `Wang_2016_Big_Data_Supply_Chain.pdf` | Provides theoretical basis for multi-source data synthesis and inventory forecasting |
| **[2]** | Barika et al. (2019) | *ACM Computing Surveys* | Cloud Big Data Workflow Orchestration | `Barika_2019_Orchestrating_Big_Data_Workflows.pdf` | Formulates the limitations of DAG schedulers and the need for barrier coordination |
| **[3]** | Apache Airflow (2024) | *Apache Software Foundation* | Data-Aware Workflow Scheduling | Documentation | Evaluates dataset-triggered DAGs and identifies polling/worker starvation bottlenecks |
| **[4]** | Kreps, Narkhede, Rao (2011) | *NetDB / LinkedIn* | Kafka Distributed Messaging Log | `Kreps_2011_Kafka.pdf` | Architectural foundation for our event-ingestion bus across orders, items, and inventory |
| **[5]** | Debezium Project (2024) | *Red Hat Open Source* | Change Data Capture (CDC) | Web / Architecture | Source-level row mutation capture directly feeding Kafka topics without application coupling |
| **[6]** | Chen & Guestrin (2016) | *ACM SIGKDD* | XGBoost Scalable Tree Boosting | `Chen_Guestrin_2016_XGBoost.pdf` | Foundational demand forecasting algorithm implemented in our analytics plane ($R^2=0.8806$) |
| **[7]** | Olist (2018) | *Kaggle Public Benchmark* | Brazilian E-Commerce Dataset | Public Dataset | Empirical benchmark dataset (99,441 authentic orders; 79,885 train / 19,556 test) |
| **[8]** | Dean & Ghemawat (2004/2008) | *OSDI / Commun. ACM* | MapReduce Distributed Computing | `Dean_Ghemawat_2004_MapReduce.pdf` | Conceptual framework for data-parallel batch transformations and barrier semantics |
| **[9]** | Zaharia et al. (2010) | *USENIX HotCloud* | Spark In-Memory Computing | `Zaharia_2010_Spark.pdf` | Iterative distributed state management informing our asynchronous worker pool |
| **[10]**| Hyndman & Athanasopoulos (2021)| *OTexts Academic Textbook* | Forecasting Principles & Practice | Open Textbook | Time-series feature engineering (moving averages, seasonal lags, error metrics) |

---

## 🔬 How These References Connect to SupplySense AI

1. **Orchestration Paradigm (Refs [2], [3]):** Traditional orchestrators use fixed-interval cron ticks, causing 72% redundant runs during quiet periods and partial-data join corruption during asynchronous upstream delays. SupplySense AI replaces periodic polling with an AST dependency engine.
2. **Streaming & Ingestion Bus (Refs [4], [5]):** Kafka provides partition-ordered log delivery with low latency (<50ms). SupplySense AI consumes from 4 distinct topic feeds and provides cryptographic idempotency deduplication.
3. **Machine Learning & Demand Intelligence (Refs [6], [7], [10]):** The Olist dataset provides 99,441 authentic customer records across 70 product categories. Feature vectors extracted from synchronized dependency cycles feed the XGBoost model, achieving $R^2 = 0.8806$ with RMSE of 3.42 units/week.
