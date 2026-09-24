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
