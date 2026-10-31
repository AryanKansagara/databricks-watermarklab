"""Deterministic operator policy model, NOT Spark or Databricks execution."""
import argparse
from collections import Counter
import json
from pathlib import Path
import random


def validate(batches, delay, window):
    if type(delay) is not int or delay < 0 or type(window) is not int or window <= 0:
        raise ValueError('nonnegative integer delay and positive window required')
    if type(batches) is not list or any(type(b) is not list for b in batches):
        raise ValueError('explicit list of micro-batches required')
    amounts = {}
    for batch in batches:
        for e in batch:
            if (type(e) is not dict or set(e) != {'id','time','amount'}
                or type(e['id']) is not str or not e['id'] or type(e['time']) is not int
                or e['time'] < 0 or type(e['amount']) is not int):
                raise ValueError('event requires id string, nonnegative integer time, integer amount')
            if e['id'] in amounts and amounts[e['id']] != e['amount']:
                raise ValueError('conflicting money for same id makes batch oracle ambiguous')
            amounts[e['id']] = e['amount']


def end(time, window):
    return (time//window+1)*window


def batch_oracle(batches, window):
    """First arrival per stable ID defines canonical time/money for this authored corpus."""
    seen, totals = set(), Counter()
    for batch in batches:
        for e in batch:
            if e['id'] not in seen:
                totals[end(e['time'],window)] += e['amount']
                seen.add(e['id'])
    return dict(totals)


def replay(batches, *, delay, window=10, flush=False):
    validate(batches,delay,window)
    if type(flush) is not bool:
        raise ValueError('flush must be a boolean')
    dedup, aggregates, output = {}, Counter(), {}
    max_seen, watermark = None, -1
    accepted, dropped, trace, finalized = [], [], [], {}
    first_batch = {}
    for i, batch in enumerate(batches):
        for e in batch:
            first_batch.setdefault(end(e['time'],window),i)
        # Watermark from PREVIOUS batch; no event in this batch moves its own cutoff.
        watermark = max_seen-delay if max_seen is not None else -1
        for key in list(dedup):
            if dedup[key] <= watermark:
                del dedup[key]
        for bucket in list(aggregates):
            if bucket <= watermark:
                output[bucket] = aggregates.pop(bucket)
                finalized[bucket] = i
        for e in batch:
            if e['time'] <= watermark:
                dropped.append(e['id'])
            elif e['id'] not in dedup:
                dedup[e['id']] = e['time']+delay
                aggregates[end(e['time'],window)] += e['amount']
                accepted.append(e['id'])
        if batch:
            maximum = max(e['time'] for e in batch)
            max_seen = maximum if max_seen is None else max(max_seen,maximum)
        state = {'dedup':dedup,'aggregates':dict(aggregates)}
        trace.append({'batch':i,'events':len(batch),'watermark':watermark,
                      'state_rows':len(dedup)+len(aggregates),
                      'state_json_bytes':len(json.dumps(state,sort_keys=True,separators=(',',':')).encode()),
                      'finalized_windows':dict(output)})
    if flush:
        # Explicit end-of-input model operation; NOT an idle batch or real Spark behavior.
        for bucket, total in aggregates.items():
            output[bucket] = total
            finalized[bucket] = len(batches)
        aggregates.clear(); dedup.clear()
    oracle = batch_oracle(batches,window)
    keys = set(output)|set(oracle)
    counts = Counter(accepted)
    ids = {e['id'] for b in batches for e in b}
    return {'scope':'deterministic authored operator model; not Spark execution',
            'policy':{'delay':delay,'window':window,'terminal_flush':flush},
            'oracle':oracle,'output':output,'open_aggregates':dict(aggregates),
            'metrics':{'absolute_aggregate_error':sum(abs(output.get(k,0)-oracle.get(k,0)) for k in keys),
                       'lost_distinct_ids':len(ids-set(accepted)), 'duplicate_leakage':sum(max(0,c-1) for c in counts.values()),
                       'dropped_late_deliveries':len(dropped),'peak_state_rows':max((r['state_rows'] for r in trace),default=0),
                       'peak_state_json_bytes':max((r['state_json_bytes'] for r in trace),default=0),
                       'max_finalize_delay_batches':max((i-first_batch[k] for k,i in finalized.items()),default=None),
                       'finalized_window_count':len(finalized),
                       'pending_window_count':len(aggregates),
                       'latency_censored':bool(aggregates),
                       'oldest_pending_age_batches':max((len(batches)-1-first_batch[k] for k in aggregates),default=None)},
            'trace':trace,'accepted_ids':accepted,'dropped_ids':dropped}


def corpus(seed=7):
    rng=random.Random(seed)
    batches=[[] for _ in range(12)]
    for i in range(40):
        e={'id':f'order-{i}','time':i*2,'amount':rng.randrange(1,20)}
        arrival=min(i//5+rng.choice([0,0,1,3]),10)
        batches[arrival].append(e)
        if i%7==0:
            batches[min(arrival+1,11)].append(dict(e))
    # Delayed old original + duplicate with refreshed event time + bad future clock.
    batches[0].append({'id':'late-original','time':1,'amount':20})
    batches[5].append({'id':'clock-skew','time':250,'amount':1})
    batches[10].append({'id':'late-original','time':260,'amount':20})
    batches[11].append({'id':'very-late','time':3,'amount':30})
    return batches


def compare(batches, delays=(5,30,300), window=10):
    return {'schema_version':1,'scope':'local deterministic model; no managed-runtime cost/throughput claims',
            'batch_boundaries':batches,'policies':[replay(batches,delay=d,window=window,flush=True) for d in delays]}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--seed',type=int,default=7);p.add_argument('--input',type=Path);p.add_argument('--output',type=Path);a=p.parse_args()
    batches=json.loads(a.input.read_text()) if a.input else corpus(a.seed)
    result=compare(batches);result['seed']=None if a.input else a.seed
    text=json.dumps(result,indent=2)+'\n'
    if a.output:a.output.write_text(text)
    else:print(text,end='')
