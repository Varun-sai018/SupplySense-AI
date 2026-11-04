# SupplySense AI Architecture

This document provides a high-level overview of the current SupplySense AI data flow and architecture.

## Current Data Flow

The project currently operates primarily on a simulated event-driven architecture, utilizing Kafka and MySQL:

Olist / source data
        ↓
Event Generator
        ↓
MySQL dataset_events
        ↓
Kafka Producer
        ↓
Kafka topic: dataset-events
        ↓
Dependency Engine
        ↓
MySQL dependency state
        ↓
TRIGGER / BLOCK
        ↓
pipeline_decisions

## Business Rules (Module 1 AST & Event-Driven Engine)

The platform evaluates three core business rules:
- **Rule A (High-Velocity Cart Abandonment):** Rolling 5-min session drop-off rate > 25% during peak trading hours (18:00–22:00 BRT/UTC-3) triggers customer re-engagement webhook.
- **Rule B (Automated Inventory Reorder):** SKU available stock <= safe threshold (20 units) and primary supplier online triggers the `Inventory Reorder Pipeline`.
- **Rule C (Payment Failure Spike Detection):** Rolling 50-transaction window success rate < 92% triggers payment gateway security incident alert (auto-resolves at >= 92%).

*Note on Rule Taxonomy:* Legacy pre-implementation draft documents previously referred to "Forecast Freshness" and "Safety-Stock Violation". These were preliminary draft concepts superseded during Module 1 development by Cart Abandonment (Rule A) and Payment Failure Spike (Rule C).

## Note on Data Source

The current Event Generator is a manual simulation of dataset updates and does not represent a real-time source system. The Olist dataset is historically loaded and updates are synthesized by the event generator for testing the dependency pipeline orchestrator. We do not claim that Olist provides real-time streaming data.

## Local Development Infrastructure

The local infrastructure leverages Docker Compose to provide reproducible environments for external dependencies:

Developer machine
      |
      v
Docker Compose
      |
      +---- MySQL (port 3306)
      |       |
      |       +---- supplysense database
      |
      +---- Kafka (port 9092)
              |
              +---- dataset-events topic

*Note: The Python services are currently executed directly on the host machine using a virtual environment. They will be containerized in a later phase of the migration.*
