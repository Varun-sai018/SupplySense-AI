# Paper Dossier: Orchestrating Big Data Analysis Workflows on the Cloud

- **Authors:** Mahdi Barika, Saurabh Garg, Albert Y. Zomaya, Lizhe Wang, Aad van Moorsel, Rajiv Ranjan
- **Journal:** *ACM Computing Surveys (CSUR)*, Vol. 52, No. 5, Article 100, pp. 1–36, 2019
- **DOI:** [10.1145/3332301](https://doi.org/10.1145/3332301)
- **Local PDF:** `docs/references/papers/Barika_2019_Orchestrating_Big_Data_Workflows.pdf`

---

## 1. Executive Summary
This survey examines modern cloud workflow orchestration frameworks. It categorizes existing architectures by workflow specification, scheduling strategies, resource provisioning, and fault tolerance.

## 2. Identified Limitations & Research Gap
- Highlights that prevailing workflow schedulers operate on static DAG representations triggered by scheduled times or manual dispatches.
- Identifies an open challenge: handling dynamic event arrivals where data-driven barriers must be synchronized across disparate streaming sources.
- SupplySense AI directly solves this gap with its AST Dependency Engine supporting `ALL`, `QUORUM m/n`, and `ANY` conditions.
