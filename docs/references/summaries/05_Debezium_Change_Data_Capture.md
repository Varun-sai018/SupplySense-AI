# Technical Dossier: Debezium — Change Data Capture for Real-Time Architectures

- **Organization:** Red Hat Open Source & Debezium Community
- **Project URL:** [https://debezium.io/](https://debezium.io/)

---

## 1. Overview
Debezium is an open-source distributed platform for change data capture. It monitors database transaction logs (MySQL binary logs, PostgreSQL WAL) and streams row-level changes (INSERT, UPDATE, DELETE) into Kafka topics.

## 2. Relevance to SupplySense AI
- Eliminates brittle application-level event firing or database polling loops.
- Emits atomic event payloads containing table names, primary keys, and column-level before/after states.
- SupplySense AI consumes these mutation streams to dynamically watermark dataset arrival states.
