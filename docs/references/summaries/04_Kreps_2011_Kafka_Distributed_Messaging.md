# Paper Dossier: Kafka: A Distributed Messaging System for Log Processing

- **Authors:** Jay Kreps, Neha Narkhede, Jun Rao (LinkedIn)
- **Conference:** *6th International Workshop on Networking Meets Databases (NetDB)*, Athens, Greece, 2011
- **Local PDF:** `docs/references/papers/Kreps_2011_Kafka.pdf`

---

## 1. Executive Summary
Kreps et al. introduce Kafka, a distributed commit-log messaging system designed for high-throughput, low-latency log collection and stream processing. Kafka unifies offline batch loading with real-time stream consumption using partition-ordered distributed logs.

## 2. SupplySense AI Integration
- SupplySense AI deploys Kafka as its core transport backbone across dedicated topics (`olist.orders`, `olist.items`, `olist.payments`, `olist.inventory`).
- Employs zero-copy consumption, consumer groups for horizontal worker scaling, and cryptographic MD5 deduplication to handle streaming spikes with < 45ms ingestion latency.
