# WatermarkLab — Databricks-inspired late-data policy laboratory

Databricks provides data engineering, analytics, and AI infrastructure. WatermarkLab explores the accuracy/state tradeoff when a stream's late-data policy encounters duplicates, delayed arrivals, and a future clock timestamp.

**Implemented:** a deterministic Python micro-batch **operator model**, complete-data batch oracle, reproducible arrival corpus, per-batch state traces, and paired policy comparison. **Integration unverified:** no Spark, state store, checkpoint, Databricks runtime, or managed cluster executes here. Python is appropriate for an explicit mathematical policy model; this is not a C++ inference workload. Java is available but PySpark/compiler support was absent, so engine fidelity is a future stage rather than an implied feature.

```sh
python -m unittest discover -v
python watermarklab.py --seed 7 --output demo.json
# Optional authored input: JSON array of batches, each an array of {id,time,amount}.
python watermarklab.py --input events.json --output comparison.json
```

Python 3.11+; standard library only. `demo.json` includes the exact event batches and every policy's trace. The checked-in seed-7 run gives:

| Delay units | Aggregate absolute error | Lost distinct IDs | Duplicate leakage | Peak modeled state rows | Peak state JSON bytes |
|---|---:|---:|---:|---:|---:|
| 5 | 298 | 22 | 1 | 12 | 173 |
| 30 | 233 | 17 | 1 | 32 | 436 |
| 300 | 0 | 0 | 0 | 52 | 742 |

These are measured outputs of the local model, **not Spark performance, state-store memory, cluster costs, or a recommendation for a real watermark**. JSON bytes measure the encoded model state only.

## Exact operator contract

1. Each batch starts with watermark `maximum_event_time_from_previous_batches - delay`; before the first batch it is −1. All events in a batch use the same cutoff, independent of within-batch ordering.
2. Before processing, evict ID state whose expiry is ≤ watermark and finalize aggregation windows whose end is ≤ watermark.
3. Drop deliveries whose timestamp is ≤ watermark. Accept the first delivery of an ID still absent from state; remember it until `accepted_event_time + delay`. A refreshed timestamp after ID eviction can therefore leak a duplicate.
4. Add accepted money to a tumbling window with end `(time // window + 1) * window`. Update maximum observed event time after the whole batch, including dropped events.
5. An empty batch creates no new event time. `flush=True` explicitly finalizes remaining model windows at end of input. This synthetic flush is **not** Spark idle-input behavior or an implicit guarantee of eventual output.

The oracle takes the first arriving event for each stable ID and aggregates its original timestamp/money. Duplicate timestamps may differ to model skew; conflicting money for one ID is rejected because the intended oracle would be ambiguous. Amounts and time are integers; all policies replay identical batch boundaries with fresh in-memory state. No checkpoint reuse occurs.

Absolute error is summed across the union of oracle/output windows; without flush it includes still-open output. Lost IDs count IDs never accepted. Duplicate leakage counts extra accepted deliveries of an ID. Finalization delay is measured in **batch indices**, not milliseconds. The maximum is `null` when no window finalized. Finalized and pending window counts, a latency-censored flag, and the oldest pending window age prevent pending output from appearing to have zero delay; when some windows remain pending, the maximum describes only finalized windows. `flush` accepts only a boolean. Future timestamps can advance the cutoff and destroy later-arriving old data; a large threshold retains more state in this corpus.

## Validation and limits

Eleven tests cover in-order controls, within-threshold duplicates, refreshed-timestamp duplicates after expiry, future-clock loss, previous-batch boundaries, equality at the cutoff, idle batches, no-flush visibility, malformed/conflicting data, determinism, censored pending latency, strict boolean flush, and the accuracy/state tradeoff.

This deliberately chosen strict operator model is **not an exact implementation of Spark `dropDuplicatesWithinWatermark` or every aggregation operator**. Databricks documents operator/version-specific guarantees and that beyond-threshold input may still be processed in some situations. We do not infer that all real operators drop it deterministically. Multi-input watermark policies, joins, recovery, skew correction, and throughput are unsupported. Next stage: pinned PySpark execution against these batches, fresh per-policy checkpoints, and differential tests that retain rather than conceal model/engine disagreements. That requires a local Spark installation; managed confirmation additionally needs a workspace.

## Primary research

Databricks already supplies watermarks, streaming progress, and stateful deduplication. The opportunity explored here is comparing the business consequence of policies with an explicit batch oracle.

- [Watermark guarantees and latency/state tradeoffs](https://docs.databricks.com/aws/en/structured-streaming/watermarks)
- [Within-watermark deduplication semantics](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/dropDuplicatesWithinWatermark)
- [withWatermark API](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/withWatermark)
