"""
SupplySense AI - Services Package

Aliases hyphenated service directories (e.g., dependency-engine, pipeline-runner)
so standard Python dot notation (services.dependency_engine, services.pipeline_runner)
works seamlessly.
"""

import sys
import os
import importlib

_SERVICES_DIR = os.path.dirname(__file__)

_MAPPINGS = [
    ("dependency-engine", "dependency_engine"),
    ("pipeline-runner", "pipeline_runner"),
    ("realtime-api", "realtime_api"),
    ("event-generator", "event_generator"),
    ("kafka-producer", "kafka_producer"),
]

for hyphen_name, underscore_name in _MAPPINGS:
    hyphen_path = os.path.join(_SERVICES_DIR, hyphen_name)
    if os.path.isdir(hyphen_path):
        try:
            mod = importlib.import_module(f"services.{hyphen_name}")
            sys.modules[f"services.{underscore_name}"] = mod
            sys.modules[underscore_name] = mod
        except Exception:
            pass
