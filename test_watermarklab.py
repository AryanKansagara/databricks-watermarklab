import unittest
from watermarklab import replay,compare,corpus

def e(id,t,amount=10):return {'id':id,'time':t,'amount':amount}

class Watermarks(unittest.TestCase):
    def test_in_order_and_within_threshold_duplicates_match_oracle(self):
        r=replay([[e('a',1),e('a',1)],[e('b',2)],[e('c',11)]],delay=20,flush=True)
        self.assertEqual(r['metrics']['absolute_aggregate_error'],0)
        self.assertEqual(r['metrics']['duplicate_leakage'],0)
    def test_previous_batch_watermark_not_current_batch_order(self):
        for batch in ([e('future',100),e('old',1)],[e('old',1),e('future',100)]):
            r=replay([batch],delay=5,flush=True)
            self.assertEqual(r['metrics']['lost_distinct_ids'],0)
    def test_future_timestamp_causes_next_batch_loss(self):
        r=replay([[e('future',100)],[e('late',1)]],delay=5,flush=True)
        self.assertEqual(r['dropped_ids'],['late']);self.assertEqual(r['metrics']['lost_distinct_ids'],1)
    def test_expired_id_refreshed_timestamp_duplicate_leaks(self):
        r=replay([[e('a',1)],[e('b',40)],[e('a',45)]],delay=5,flush=True)
        self.assertEqual(r['metrics']['duplicate_leakage'],1)
        self.assertGreater(r['metrics']['absolute_aggregate_error'],0)
    def test_idle_input_does_not_create_new_event_time(self):
        r=replay([[e('a',2)],[],[]],delay=5)
        self.assertEqual(r['output'],{})
        self.assertEqual(r['trace'][1]['watermark'],r['trace'][2]['watermark'])
        self.assertEqual(r['open_aggregates'],{10:10})
    def test_boundary_equality_is_explicit_model_drop(self):
        r=replay([[e('a',10)],[e('b',5)]],delay=5,flush=True)
        self.assertEqual(r['dropped_ids'],['b'])
    def test_large_delay_preserves_authored_corpus_at_state_cost(self):
        rows=compare(corpus())['policies'];small,big=rows[0],rows[-1]
        self.assertGreater(small['metrics']['absolute_aggregate_error'],0)
        self.assertEqual(big['metrics']['absolute_aggregate_error'],0)
        self.assertGreater(big['metrics']['peak_state_rows'],small['metrics']['peak_state_rows'])
    def test_conflicting_id_money_and_malformed_input_rejected(self):
        for batches in ([[e('a',1),e('a',2,99)]],[[e('a',True)]],[[dict(e('a',1),secret=1)]]):
            with self.assertRaises(ValueError):replay(batches,delay=10)
        with self.assertRaises(ValueError):replay([],delay=True)
    def test_pending_latency_is_censored_not_zero(self):
        r=replay([[e('a',1)],[e('future',20)],[],[]],delay=100,flush=False)
        self.assertEqual(r['output'],{})
        self.assertIsNone(r['metrics']['max_finalize_delay_batches'])
        self.assertEqual(r['metrics']['finalized_window_count'],0)
        self.assertEqual(r['metrics']['pending_window_count'],2)
        self.assertTrue(r['metrics']['latency_censored'])
        self.assertEqual(r['metrics']['oldest_pending_age_batches'],3)
        complete=replay([[e('a',1)]],delay=100,flush=True)
        self.assertFalse(complete['metrics']['latency_censored'])
        self.assertEqual(complete['metrics']['pending_window_count'],0)
        self.assertEqual(complete['metrics']['finalized_window_count'],1)
        empty=replay([],delay=1)
        self.assertIsNone(empty['metrics']['max_finalize_delay_batches'])
        self.assertFalse(empty['metrics']['latency_censored'])
    def test_flush_requires_actual_boolean(self):
        for invalid in ('false',1,None,[]):
            with self.subTest(flush=invalid),self.assertRaises(ValueError):
                replay([[e('a',1)]],delay=10,flush=invalid)
    def test_determinism_empty_and_no_flush_visibility(self):
        self.assertEqual(compare(corpus(42)),compare(corpus(42)))
        self.assertEqual(replay([],delay=1)['metrics']['peak_state_rows'],0)
        self.assertGreater(replay([[e('a',1)]],delay=10)['metrics']['absolute_aggregate_error'],0)

if __name__=='__main__':unittest.main()
