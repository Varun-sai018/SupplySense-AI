# Paper Dossier: MapReduce: Simplified Data Processing on Large Clusters

- **Authors:** Jeffrey Dean, Sanjay Ghemawat (Google)
- **Conference/Journal:** *OSDI 2004* / *Communications of the ACM*, Vol. 51, No. 1, pp. 107–113, 2008
- **Local PDF:** `docs/references/papers/Dean_Ghemawat_2004_MapReduce.pdf`

---

## 1. Executive Summary
Dean & Ghemawat present MapReduce, the programming model that inaugurated modern large-scale distributed data engineering. It abstracts cluster parallelism, fault tolerance, and data distribution behind two simple primitives: Map and Reduce.

## 2. Relevance to SupplySense AI
- Serves as the historical baseline for barrier-synchronized batch computing.
- Highlights the limitations of batch barriers when inputs arrive asynchronously in real-time streaming topologies.
