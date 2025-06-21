import json
import math
import pandas as pd
from kafka import KafkaConsumer

#allocation configuration
ORDER_SIZE = 5000
CHUNK_SIZE = 10

#Cost and Allocation
def compute_cost(split, venues, order_size, lambda_over, lambda_under, theta_queue):
    executed = 0
    cash_spent = 0.0
    for i, s in enumerate(split):
        exe = min(s, venues[i]['ask_size'])
        executed += exe
        cash_spent += exe * (venues[i]['ask'] + venues[i]['fee'])
        maker_rebate = max(s - exe, 0) * venues[i]['rebate']
        cash_spent -= maker_rebate

    underfill = max(order_size - executed, 0)
    overfill  = max(executed - order_size, 0)
    risk_pen  = theta_queue * (underfill + overfill)
    cost_pen  = lambda_under * underfill + lambda_over * overfill
    return cash_spent + risk_pen + cost_pen


def allocate(order_size, venues, lambda_over, lambda_under, theta_queue):
    splits = [[]]
    for v in range(len(venues)):
        new_splits = []
        for alloc in splits:
            used = sum(alloc)
            max_v = min(order_size - used, venues[v]['ask_size'])
            for q in range(0, max_v + 1, CHUNK_SIZE):
                new_splits.append(alloc + [q])
        splits = new_splits

    best_cost = math.inf
    best_split = None
    for alloc in splits:
        if sum(alloc) != order_size:
            continue
        cost = compute_cost(alloc, venues, order_size, lambda_over, lambda_under, theta_queue)
        if cost < best_cost:
            best_cost  = cost
            best_split = alloc
    return best_split, best_cost

#Baselines
def run_backtest(lambda_over, lambda_under, theta_queue):
    consumer = KafkaConsumer(
        'mock_l1_stream',
        bootstrap_servers='localhost:9092',
        group_id='backtest-group',
        auto_offset_reset='earliest',   # start from the beginning of the topic
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("Waiting for new market data...")

    unfilled = ORDER_SIZE
    cumulative_cash = 0.0
    current_ts = None
    venues = []
    snapshots = []

    for msg in consumer:
        data = msg.value
        ts = pd.to_datetime(data['ts_event'])

        if current_ts is None:
            current_ts = ts

        # when time changes, execute one snapshot
        if ts != current_ts:
            snapshots.append(list(venues))
            avail = sum(v['ask_size'] for v in venues)
            to_alloc = min(unfilled, avail)

            split, cost_est = allocate(to_alloc, venues, lambda_over, lambda_under, theta_queue)
            if split is None:
                split = [v['ask_size'] for v in venues]
                cost_est = compute_cost(split, venues, to_alloc, lambda_over, lambda_under, theta_queue)

            filled = sum(min(split[i], venues[i]['ask_size']) for i in range(len(venues)))
            print(f"Snapshot {current_ts} → allocate {to_alloc}, filled {filled}, cost {cost_est:.2f}")
            cumulative_cash += cost_est
            unfilled = max(unfilled - filled, 0)

            if unfilled == 0:
                break

            venues = []
            current_ts = ts

        # collect venue snapshot
        venues.append({
            'ask': data['ask_px_00'],
            'ask_size': data['ask_sz_00'],
            'fee': data.get('fee', 0.0),
            'rebate': data.get('rebate', 0.0),
        })

    optimized = {'total_cash': cumulative_cash, 'avg_fill_px': cumulative_cash / ORDER_SIZE}
    return optimized, snapshots


def run_best_ask(snapshots):
    rem  = ORDER_SIZE
    cash = 0.0
    for venues in snapshots:
        if rem <= 0:
            break
        asks = sorted([(v['ask'], v['ask_size']) for v in venues], key=lambda x: x[0])
        for price, size in asks:
            exe = min(size, rem)
            cash += exe * price
            rem  -= exe
            if rem == 0:
                break
    executed = ORDER_SIZE - rem
    return {'total_cash': cash, 'avg_fill_px': cash / ORDER_SIZE, 'executed': executed}


def run_twap(snapshots):
    intervals = len(snapshots)
    if intervals == 0:
        return {'total_cash': 0, 'avg_fill_px': 0, 'executed': 0}
    per_slice = math.ceil(ORDER_SIZE / intervals)
    rem = ORDER_SIZE
    cash = 0.0
    for venues in snapshots:
        if rem <= 0:
            break
        share = min(per_slice, rem)
        filled = 0
        asks = sorted([(v['ask'], v['ask_size']) for v in venues], key=lambda x: x[0])
        for price, size in asks:
            exe = min(size, share - filled)
            cash += exe * price
            rem  -= exe
            filled += exe
            if filled == share:
                break
    executed = ORDER_SIZE - rem
    return {'total_cash': cash, 'avg_fill_px': cash / ORDER_SIZE, 'executed': executed}


def run_vwap(snapshots):
    volumes   = [sum(v['ask_size'] for v in venues) for venues in snapshots]
    total_vol = sum(volumes)
    rem = ORDER_SIZE
    cash = 0.0
    for venues, vol in zip(snapshots, volumes):
        if rem <= 0 or total_vol == 0:
            break
        share = min(math.ceil((vol / total_vol) * ORDER_SIZE), rem)
        filled = 0
        asks = sorted([(v['ask'], v['ask_size']) for v in venues], key=lambda x: x[0])
        for price, size in asks:
            exe = min(size, share - filled)
            cash += exe * price
            rem  -= exe
            filled += exe
            if filled == share:
                break
    executed = ORDER_SIZE - rem
    return {'total_cash': cash, 'avg_fill_px': cash / ORDER_SIZE, 'executed': executed}

#optimization
def optimize_parameters():
    coarse_vals = [0.0, 0.5, 1.0]
    best_params = None
    best_cash   = math.inf
    for lo in coarse_vals:
        for lu in coarse_vals:
            for th in coarse_vals:
                opt, _ = run_backtest(lo, lu, th)
                if opt['total_cash'] < best_cash:
                    best_cash   = opt['total_cash']
                    best_params = (lo, lu, th)

    lo0, lu0, th0 = best_params
    deltas = [-0.25, 0.0, 0.25]
    best2, best_cash2 = best_params, best_cash
    for dlo in deltas:
        for dlu in deltas:
            for dth in deltas:
                lo = max(0, lo0 + dlo)
                lu = max(0, lu0 + dlu)
                th = max(0, th0 + dth)
                opt, _ = run_backtest(lo, lu, th)
                if opt['total_cash'] < best_cash2:
                    best_cash2  = opt['total_cash']
                    best2       = (lo, lu, th)
    return best2

#main func
if __name__ == "__main__":
    #find best parameters
    lambda_over, lambda_under, theta_queue = optimize_parameters()
    print(f"Best params → λ_over={lambda_over}, λ_under={lambda_under}, θ_queue={theta_queue}")

    optimized, snapshots = run_backtest(lambda_over, lambda_under, theta_queue)
    baselines = {
        'best_ask': run_best_ask(snapshots),
        'twap': run_twap(snapshots),
        'vwap': run_vwap(snapshots),
    }
    savings = {}
    for key, base in baselines.items():
        savings[key] = ((base['avg_fill_px'] - optimized['avg_fill_px']) / base['avg_fill_px']) * 10000

    final = {
        'best_parameters': {'lambda_over': lambda_over, 'lambda_under': lambda_under, 'theta_queue': theta_queue},
        'optimized': optimized,
        'baselines': baselines,
        'savings_vs_baselines_bps': savings,
    }
    print(json.dumps(final, indent=2))
