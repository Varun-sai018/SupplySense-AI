"""
Unit tests for the chronological dataset split logic.

These tests use small synthetic dataframes and do NOT require MySQL.
"""
import unittest
import os
import sys
import tempfile
import csv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from ml.data.split_dataset import chronological_split, validate_split, load_dataset, save_split


def make_rows(weeks, categories=None):
    """Helper: generate synthetic demand rows for given weeks."""
    if categories is None:
        categories = ['cat_a', 'cat_b']
    rows = []
    for wk in weeks:
        for cat in categories:
            rows.append({
                'product_category': cat,
                'year_week': wk.replace('-', '') if '-' in wk else wk,
                'week_start_date': wk,
                'demand': 10
            })
    return rows


class TestChronologicalSplit(unittest.TestCase):

    def test_basic_split_ordering(self):
        """Train dates must precede validation, which must precede test."""
        weeks = [f'2024-01-{d:02d}' for d in range(1, 22)]  # 21 weeks
        rows = make_rows(weeks)
        train, val, test = chronological_split(rows)

        train_max = max(r['week_start_date'] for r in train)
        val_min = min(r['week_start_date'] for r in val)
        val_max = max(r['week_start_date'] for r in val)
        test_min = min(r['week_start_date'] for r in test)

        self.assertLess(train_max, val_min)
        self.assertLess(val_max, test_min)

    def test_no_temporal_overlap(self):
        """No shared dates between any split pair."""
        weeks = [f'2024-01-{d:02d}' for d in range(1, 22)]
        rows = make_rows(weeks)
        train, val, test = chronological_split(rows)

        train_dates = set(r['week_start_date'] for r in train)
        val_dates = set(r['week_start_date'] for r in val)
        test_dates = set(r['week_start_date'] for r in test)

        self.assertEqual(train_dates & val_dates, set())
        self.assertEqual(val_dates & test_dates, set())
        self.assertEqual(train_dates & test_dates, set())

    def test_all_rows_preserved(self):
        """All input rows appear in exactly one split."""
        weeks = [f'2024-01-{d:02d}' for d in range(1, 22)]
        rows = make_rows(weeks)
        train, val, test = chronological_split(rows)
        self.assertEqual(len(train) + len(val) + len(test), len(rows))

    def test_approximate_70_15_15(self):
        """Split proportions should roughly match 70/15/15 by week count."""
        weeks = [f'2024-{m:02d}-01' for m in range(1, 13)]
        weeks += [f'2024-{m:02d}-15' for m in range(1, 9)]  # 20 total weeks
        weeks = sorted(set(weeks))
        rows = make_rows(weeks)
        train, val, test = chronological_split(rows)

        n_weeks = len(weeks)
        train_weeks = len(set(r['week_start_date'] for r in train))
        # Train should get roughly 70% of weeks
        self.assertGreaterEqual(train_weeks / n_weeks, 0.5)
        self.assertLessEqual(train_weeks / n_weeks, 0.85)

    def test_required_columns(self):
        """Output rows should contain the required columns."""
        weeks = [f'2024-01-{d:02d}' for d in range(1, 11)]
        rows = make_rows(weeks)
        train, val, test = chronological_split(rows)

        required = {'product_category', 'year_week', 'week_start_date', 'demand'}
        for dataset in [train, val, test]:
            self.assertTrue(required.issubset(set(dataset[0].keys())))

    def test_non_empty_splits(self):
        """All three splits must be non-empty."""
        weeks = [f'2024-01-{d:02d}' for d in range(1, 11)]
        rows = make_rows(weeks)
        train, val, test = chronological_split(rows)
        self.assertGreater(len(train), 0)
        self.assertGreater(len(val), 0)
        self.assertGreater(len(test), 0)

    def test_too_few_weeks_raises(self):
        """Less than 3 weeks should raise ValueError."""
        rows = make_rows(['2024-01-01', '2024-01-08'])
        with self.assertRaises(ValueError):
            chronological_split(rows)

    def test_validate_split_catches_overlap(self):
        """validate_split should raise if there is temporal overlap."""
        train = [{'product_category': 'a', 'year_week': '2024W01',
                  'week_start_date': '2024-01-01', 'demand': 5}]
        val = [{'product_category': 'a', 'year_week': '2024W01',
                'week_start_date': '2024-01-01', 'demand': 3}]  # same date!
        test = [{'product_category': 'a', 'year_week': '2024W02',
                 'week_start_date': '2024-01-08', 'demand': 7}]
        with self.assertRaises(ValueError):
            validate_split(train, val, test)

    def test_csv_round_trip(self):
        """Save and reload should produce identical data."""
        weeks = [f'2024-01-{d:02d}' for d in range(1, 11)]
        rows = make_rows(weeks)
        train, val, test = chronological_split(rows)

        with tempfile.TemporaryDirectory() as tmpdir:
            save_split(train, val, test, output_dir=tmpdir)

            for name, original in [('train', train), ('validation', val), ('test', test)]:
                loaded = load_dataset(os.path.join(tmpdir, f'{name}.csv'))
                self.assertEqual(len(loaded), len(original))
                for orig, load in zip(original, loaded):
                    self.assertEqual(orig['product_category'], load['product_category'])
                    self.assertEqual(orig['demand'], load['demand'])


if __name__ == '__main__':
    unittest.main()
