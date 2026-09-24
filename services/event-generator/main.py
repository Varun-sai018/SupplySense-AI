import os
import sys
import logging
from datetime import datetime

# Ensure repository root is in sys.path for common/config imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from common.database import get_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def get_dataset(cursor, dataset_name):
    """Retrieve dataset metadata."""
    cursor.execute(
        """
        SELECT
            dataset_id,
            dataset_name,
            current_version,
            row_count,
            status
        FROM dataset_metadata
        WHERE dataset_name = %s
        """,
        (dataset_name,)
    )
    return cursor.fetchone()


def generate_event(dataset_name, rows_changed):
    """Generate a dataset update event."""
    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()

        dataset = get_dataset(cursor, dataset_name)
        if dataset is None:
            logger.error(f"Dataset '{dataset_name}' does not exist.")
            return

        dataset_id = dataset['dataset_id'] if isinstance(dataset, dict) else dataset[0]
        current_version = dataset['current_version'] if isinstance(dataset, dict) else dataset[2]
        current_row_count = dataset['row_count'] if isinstance(dataset, dict) else dataset[3]

        new_version = current_version + 1
        new_row_count = current_row_count + rows_changed
        event_time = datetime.now()

        # Update metadata
        cursor.execute(
            """
            UPDATE dataset_metadata
            SET
                current_version = %s,
                row_count = %s,
                last_updated_at = %s,
                status = 'READY'
            WHERE dataset_id = %s
            """,
            (new_version, new_row_count, event_time, dataset_id)
        )

        # Create event
        cursor.execute(
            """
            INSERT INTO dataset_events
            (
                dataset_id,
                dataset_name,
                event_type,
                event_time,
                dataset_version,
                rows_changed,
                event_status
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                'NEW'
            )
            """,
            (
                dataset_id,
                dataset_name,
                "DATASET_UPDATED",
                event_time,
                new_version,
                rows_changed
            )
        )

        connection.commit()
        event_id = cursor.lastrowid

        print("\n" + "=" * 60)
        print("        DATASET UPDATE EVENT GENERATED")
        print("=" * 60)
        print(f"Event ID       : {event_id}")
        print(f"Dataset        : {dataset_name}")
        print(f"Event Type     : DATASET_UPDATED")
        print(f"Previous Ver.  : {current_version}")
        print(f"New Version    : {new_version}")
        print(f"Rows Changed   : {rows_changed}")
        print(f"Previous Rows  : {current_row_count}")
        print(f"New Row Count  : {new_row_count}")
        print(f"Event Time     : {event_time}")
        print(f"Status         : NEW")
        print("=" * 60 + "\n")

    except Exception as e:
        logger.error(f"Failed to generate event: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()


def show_datasets():
    """Display available datasets."""
    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                dataset_id,
                dataset_name,
                current_version,
                row_count,
                status
            FROM dataset_metadata
            ORDER BY dataset_id
            """
        )
        datasets = cursor.fetchall()

        print("\n" + "=" * 70)
        print("                 AVAILABLE DATASETS")
        print("=" * 70)
        print(f"{'ID':<5}{'Dataset':<20}{'Version':<10}{'Rows':<15}{'Status':<10}")
        print("-" * 70)

        for dataset in datasets:
            d_id = dataset['dataset_id'] if isinstance(dataset, dict) else dataset[0]
            d_name = dataset['dataset_name'] if isinstance(dataset, dict) else dataset[1]
            d_ver = dataset['current_version'] if isinstance(dataset, dict) else dataset[2]
            d_rows = dataset['row_count'] if isinstance(dataset, dict) else dataset[3]
            d_status = dataset['status'] if isinstance(dataset, dict) else dataset[4]

            print(f"{d_id:<5}{d_name:<20}{d_ver:<10}{d_rows:<15}{d_status:<10}")

        print("=" * 70)

    except Exception as e:
        logger.error(f"Failed to fetch datasets: {e}")
    finally:
        if connection:
            connection.close()


def main():
    print("\n" + "=" * 70)
    print("          SUPPLYSENSE AI - EVENT GENERATOR")
    print("=" * 70)

    show_datasets()

    print("\nEnter the dataset you want to update.")
    print("\nExamples:")
    print("  Orders")
    print("  Products")
    print("  Sellers")
    print("  Order Items")

    try:
        dataset_name = input("\nDataset name: ").strip()
        if not dataset_name:
            return
        rows_changed = int(input("Number of new rows: "))

        if rows_changed <= 0:
            logger.warning("Number of rows must be greater than 0.")
        else:
            generate_event(dataset_name, rows_changed)
    except ValueError:
        logger.error("Please enter a valid number.")
    except KeyboardInterrupt:
        print("\nExiting...")


if __name__ == "__main__":
    main()
