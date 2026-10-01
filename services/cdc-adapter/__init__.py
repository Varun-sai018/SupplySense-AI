from .adapter import (
    normalize_cdc_record,
    process_cdc_event,
    run_cdc_adapter,
    TABLE_TO_DATASET_MAP,
)
from .register_connector import (
    register_or_update_connector,
    get_connector_status,
    wait_for_connector_running,
    delete_connector,
)

__all__ = [
    "normalize_cdc_record",
    "process_cdc_event",
    "run_cdc_adapter",
    "TABLE_TO_DATASET_MAP",
    "register_or_update_connector",
    "get_connector_status",
    "wait_for_connector_running",
    "delete_connector",
]
