#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from copetech_sec.form4_processor import Form4Processor


async def _unused_fetch(*args, **kwargs):
    return []


class DummyDocumentHandler:
    pass


class Form4SignalTests(unittest.TestCase):
    def setUp(self):
        self.processor = Form4Processor(DummyDocumentHandler(), _unused_fetch)

    def _make_tx(self, **overrides):
        tx = {
            'issuer_cik': '0000000001',
            'issuer_name': 'Acme Corp',
            'owner_cik': '0000001000',
            'owner_name': 'Jane Doe',
            'owner_position': 'Officer (CEO)',
            'transaction_date': '2026-03-01',
            'transaction_code': 'P',
            'transaction_type': 'Purchase',
            'shares': 1000.0,
            'price_per_share': 10.0,
            'value': 10000.0,
            'is_derivative': False,
            'is_acquisition': True,
            'is_disposition': False,
            'direct_indirect': 'D',
            'security_title': 'Common Stock',
        }
        tx.update(overrides)
        return tx

    def _make_meta(self, **overrides):
        meta = {
            'accession_no': '0000000000-26-000001',
            'filing_date': '2026-03-02',
            'form': '4',
            'url': 'https://example.com/filing',
            'primary_document': 'form4.xml',
            'primary_document_description': 'Form 4',
        }
        meta.update(overrides)
        return meta

    def test_classification_matrix(self):
        cases = [
            ('P', False, 10.0, 'open_market_buy'),
            ('S', False, 11.0, 'open_market_sell'),
            ('F', False, 0.0, 'tax_sale'),
            ('M', True, 0.0, 'option_exercise'),
            ('A', False, 0.0, 'award_or_grant'),
            ('G', False, 0.0, 'gift'),
            ('C', True, 0.0, 'derivative_conversion'),
        ]
        for code, is_derivative, price, expected in cases:
            tx = self._make_tx(
                transaction_code=code,
                is_derivative=is_derivative,
                price_per_share=price,
                transaction_type=code,
            )
            event = self.processor._normalize_signal_event(tx, self._make_meta(), 'ACME')
            self.assertEqual(event['signal_class'], expected)

    def test_amendment_replaces_prior_event(self):
        original = self.processor._normalize_signal_event(self._make_tx(value=10000.0), self._make_meta(), 'ACME')
        amended = self.processor._normalize_signal_event(
            self._make_tx(value=12500.0, shares=1250.0),
            self._make_meta(accession_no='0000000000-26-000002', form='4/A'),
            'ACME',
        )
        effective = self.processor._dedupe_and_apply_amendments([original, amended])
        self.assertEqual(len(effective), 1)
        self.assertTrue(effective[0]['is_amendment'])
        self.assertEqual(effective[0]['gross_value'], 12500.0)

    def test_same_day_events_collapse_into_single_aggregate(self):
        buy = self.processor._normalize_signal_event(self._make_tx(owner_cik='1', owner_name='A'), self._make_meta(), 'ACME')
        sell = self.processor._normalize_signal_event(
            self._make_tx(owner_cik='2', owner_name='B', transaction_code='S', transaction_type='Sale', is_acquisition=False, is_disposition=True, value=3000.0),
            self._make_meta(accession_no='0000000000-26-000003'),
            'ACME',
        )
        aggregates = self.processor._build_daily_aggregates('ACME', [buy, sell])
        self.assertEqual(len(aggregates), 1)
        aggregate = aggregates[0]
        self.assertEqual(aggregate['open_market_buy_count'], 1)
        self.assertEqual(aggregate['open_market_sell_count'], 1)
        self.assertEqual(aggregate['total_event_count'], 2)
        self.assertIn('signal_strength_score', aggregate)

    def test_tax_only_day_is_dampened(self):
        tax_sale = self.processor._normalize_signal_event(
            self._make_tx(transaction_code='F', transaction_type='Tax', value=0.0, price_per_share=0.0, is_acquisition=False, is_disposition=True),
            self._make_meta(),
            'ACME',
        )
        aggregates = self.processor._build_daily_aggregates('ACME', [tax_sale])
        self.assertEqual(len(aggregates), 1)
        self.assertLessEqual(aggregates[0]['signal_strength_score'], 0.0)
        self.assertIn('tax_only', aggregates[0]['signal_strength_reason'])

    def test_llm_digest_shape(self):
        buy = self.processor._normalize_signal_event(self._make_tx(), self._make_meta(), 'ACME')
        aggregates = self.processor._build_daily_aggregates('ACME', [buy])
        digest = self.processor._build_llm_digest('ACME', [buy], aggregates, 'filing_date')
        self.assertEqual(sorted(digest.keys()), ['anomalies', 'caveats', 'key_events', 'summary'])
        self.assertIn('total_filings', digest['summary'])
        self.assertTrue(isinstance(digest['key_events'], list))

    def _build_buy_event(self, *, owner_cik: str, owner_name: str, transaction_date: str,
                         value: float = 50_000.0, role: str = 'Officer (CEO)') -> dict:
        tx = self._make_tx(
            owner_cik=owner_cik,
            owner_name=owner_name,
            owner_position=role,
            transaction_date=transaction_date,
            value=value,
        )
        meta = self._make_meta(accession_no=f'0000000000-26-{owner_cik}', filing_date=transaction_date)
        return self.processor._normalize_signal_event(tx, meta, 'ACME')

    def test_cluster_detector_returns_empty_below_threshold(self):
        events = [
            self._build_buy_event(owner_cik='001', owner_name='A', transaction_date='2026-03-01'),
            self._build_buy_event(owner_cik='002', owner_name='B', transaction_date='2026-03-02'),
        ]
        clusters = self.processor.detect_cluster_buys(events, window_days=14, min_unique_insiders=3)
        self.assertEqual(clusters, [])

    def test_cluster_detector_finds_cluster_when_three_distinct_insiders_within_window(self):
        events = [
            self._build_buy_event(owner_cik='001', owner_name='A', transaction_date='2026-03-01', value=10_000),
            self._build_buy_event(owner_cik='002', owner_name='B', transaction_date='2026-03-05', value=20_000),
            self._build_buy_event(owner_cik='003', owner_name='C', transaction_date='2026-03-10', value=30_000),
        ]
        clusters = self.processor.detect_cluster_buys(events, window_days=14, min_unique_insiders=3)
        self.assertEqual(len(clusters), 1)
        cluster = clusters[0]
        self.assertEqual(cluster['unique_insiders'], 3)
        self.assertEqual(cluster['event_count'], 3)
        self.assertEqual(cluster['window_start'], '2026-03-01')
        self.assertEqual(cluster['window_end'], '2026-03-10')
        self.assertEqual(cluster['total_value'], 60_000.0)
        self.assertEqual(len(cluster['insiders']), 3)
        # Insiders ranked by gross_value desc
        self.assertEqual(cluster['insiders'][0]['owner_name'], 'C')

    def test_cluster_detector_excludes_buys_outside_window(self):
        events = [
            self._build_buy_event(owner_cik='001', owner_name='A', transaction_date='2026-03-01'),
            self._build_buy_event(owner_cik='002', owner_name='B', transaction_date='2026-03-02'),
            # Outside the 14-day window
            self._build_buy_event(owner_cik='003', owner_name='C', transaction_date='2026-04-15'),
        ]
        clusters = self.processor.detect_cluster_buys(events, window_days=14, min_unique_insiders=3)
        self.assertEqual(clusters, [])

    def test_cluster_detector_ignores_non_open_market_buys(self):
        buy = self._build_buy_event(owner_cik='001', owner_name='A', transaction_date='2026-03-01')
        sell_tx = self._make_tx(
            owner_cik='002', owner_name='B', transaction_code='S',
            transaction_type='Sale', is_acquisition=False, is_disposition=True,
            transaction_date='2026-03-02', value=30_000,
        )
        sell = self.processor._normalize_signal_event(sell_tx, self._make_meta(accession_no='X-2'), 'ACME')
        award_tx = self._make_tx(
            owner_cik='003', owner_name='C', transaction_code='A',
            transaction_type='Award', price_per_share=0.0,
            transaction_date='2026-03-03', value=0.0,
        )
        award = self.processor._normalize_signal_event(award_tx, self._make_meta(accession_no='X-3'), 'ACME')

        clusters = self.processor.detect_cluster_buys([buy, sell, award], window_days=14, min_unique_insiders=2)
        # Only one open_market_buy → cannot form a cluster
        self.assertEqual(clusters, [])

    def test_cluster_detector_merges_overlapping_windows(self):
        # Two overlapping anchors each see 3 insiders; merging extends the cluster end.
        events = [
            self._build_buy_event(owner_cik='001', owner_name='A', transaction_date='2026-03-01', value=10_000),
            self._build_buy_event(owner_cik='002', owner_name='B', transaction_date='2026-03-04', value=20_000),
            self._build_buy_event(owner_cik='003', owner_name='C', transaction_date='2026-03-07', value=30_000),
            self._build_buy_event(owner_cik='004', owner_name='D', transaction_date='2026-03-12', value=40_000),
        ]
        clusters = self.processor.detect_cluster_buys(events, window_days=10, min_unique_insiders=3)
        self.assertEqual(len(clusters), 1)
        merged = clusters[0]
        self.assertEqual(merged['unique_insiders'], 4)
        self.assertEqual(merged['event_count'], 4)
        self.assertEqual(merged['window_start'], '2026-03-01')
        self.assertEqual(merged['window_end'], '2026-03-12')

    def test_cluster_detector_isolates_non_overlapping_clusters(self):
        events = [
            self._build_buy_event(owner_cik='001', owner_name='A', transaction_date='2026-01-01', value=5_000),
            self._build_buy_event(owner_cik='002', owner_name='B', transaction_date='2026-01-03', value=6_000),
            self._build_buy_event(owner_cik='003', owner_name='C', transaction_date='2026-01-05', value=7_000),
            # Big gap, then a second cluster
            self._build_buy_event(owner_cik='004', owner_name='D', transaction_date='2026-06-01', value=80_000),
            self._build_buy_event(owner_cik='005', owner_name='E', transaction_date='2026-06-04', value=90_000),
            self._build_buy_event(owner_cik='006', owner_name='F', transaction_date='2026-06-07', value=100_000),
        ]
        clusters = self.processor.detect_cluster_buys(events, window_days=14, min_unique_insiders=3)
        self.assertEqual(len(clusters), 2)
        # Sorted by total_value desc → the June cluster should come first
        self.assertEqual(clusters[0]['window_start'], '2026-06-01')
        self.assertEqual(clusters[1]['window_start'], '2026-01-01')


if __name__ == '__main__':
    unittest.main()
