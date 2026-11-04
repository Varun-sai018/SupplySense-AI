# Paper Dossier: Spark: Cluster Computing with Working Sets

- **Authors:** Matei Zaharia, Mosharaf Chowdhury, Michael J. Franklin, Scott Shenker, Ion Stoica (UC Berkeley)
- **Conference:** *2nd USENIX Workshop on Hot Topics in Cloud Computing (HotCloud)*, Boston, MA, 2010
- **Local PDF:** `docs/references/papers/Zaharia_2010_Spark.pdf`

---

## 1. Executive Summary
Zaharia et al. introduce Apache Spark and Resilient Distributed Datasets (RDDs). Spark retains working sets in distributed memory across multi-pass iterative computations, outperforming disk-bound MapReduce by up to 10× in machine learning workloads.

## 2. Relevance to SupplySense AI
- Informs SupplySense AI's asynchronous worker pool and memory caching tier (Redis + AsyncIO).
- Demonstrates the necessity of in-memory lineage tracking to maintain high throughput during bursty event arrivals.
