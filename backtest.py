import json
from kafka import KafkaConsumer
import pandas as pd
import math
import itertools

ORDER_SIZE = 5000
CHUNK_SIZE = 100  # shares per step (adjust if needed)


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
    overfill = max(executed - order_size, 0)
    risk_pen = theta_queue * (underfill + overfill)
    cost_pen = lambda_under * underfill + lambda_over * overfill
    return cash_spent + risk_pen + cost_pen


def allocate(order_size, venues, lambda_over, lambda_under, theta_queue):
    splits = [[]]
    for v in range(len(venues)):
        new_splits = []
        for alloc in splits:
            used = sum(alloc)
            max_v = min(order_size - used, venues[v]['ask_size'])
            # step by CHUNK_SIZE
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
            best_cost = cost
            best_split = alloc
    return best_split, best_cost


def run_best_ask(snapshots):
    rem = ORDER_SIZE
    cash = 0.0
    for venues in snapshots:
        if rem <= 0:
            break
        # flatten asks by price
        asks = sorted([(v['ask'], v['ask_size']) for v in venues], key=lambda x: x[0])
        for price, size in asks:
            exe = min(size, rem)
            cash += exe * price
            rem -= exe
            if rem == 0:
                break
    avg = cash / ORDER_SIZE if ORDER_SIZE else 0
    return {'total_cash': cash, 'avg_fill_px': avg}


def run_twap(snapshots):
    intervals = len(snapshots)
    if intervals == 0:
        return {'total_cash': 0, 'avg_fill_px': 0}
    per_slice = math.ceil(ORDER_SIZE / intervals)
    rem = ORDER_SIZE
    cash = 0.0
    for venues in snapshots:
        if rem <= 0:
            break
        share = min(per_slice, rem)
        asks = sorted([(v['ask'], v['ask_size']) for v in venues], key=lambda x: x[0])
        filled = 0
        for price, size in asks:
            exe = min(size, share - filled)
            cash += exe * price
            filled += exe
            rem -= exe
            if filled == share:
                break
    avg = cash / ORDER_SIZE if ORDER_SIZE else 0
    return {'total_cash': cash, 'avg_fill_px': avg}


def run_vwap(snapshots):
    volumes = [sum(v['ask_size'] for v in venues) for venues in snapshots]
    total_vol = sum(volumes)
    rem = ORDER_SIZE
    cash = 0.0
    for venues, vol in zip(snapshots, volumes):
        if rem <= 0 or total_vol == 0:
            break
        share = min(math.ceil((vol / total_vol) * ORDER_SIZE), rem)
        asks = sorted([(v['ask'], v['ask_size']) for v in venues], key=lambda x: x[0])
        filled = 0
        for price, size in asks:
            exe = min(size, share - filled)
            cash += exe * price
            filled += exe
            rem -= exe
            if filled == share:
                break
    avg = cash / ORDER_SIZE if ORDER_SIZE else 0
    return {'total_cash': cash, 'avg_fill_px': avg}


def run_backtest_for_params(lambda_over, lambda_under, theta_queue):
    """
    Run the backtest once with given parameters.
    Returns:
      optimized: {'total_cash': ..., 'avg_fill_px': ...}
      baselines: {'best_ask': {...}, 'twap': {...}, 'vwap': {...}}
      savings: {'best_ask': bps, 'twap': bps, 'vwap': bps}
    """
    consumer = KafkaConsumer(
        'mock_l1_stream',
        bootstrap_servers='localhost:9092',
        group_id=None,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"Running backtest for params λ_over={lambda_over}, λ_under={lambda_under}, θ_queue={theta_queue} ...")
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

        #When timestamp changes, process snapshot
        if ts != current_ts:
            # Save this snapshot’s venues
            snapshots.append(list(venues))
            avail = sum(v['ask_size'] for v in venues)
            to_alloc = min(unfilled, avail)

            split, cost_est = allocate(to_alloc, venues, lambda_over, lambda_under, theta_queue)
            if split is None:
                #if no exact split, fall back to taking all available
                split = [v['ask_size'] for v in venues]
                cost_est = compute_cost(split, venues, to_alloc, lambda_over, lambda_under, theta_queue)

            filled = sum(min(split[i], venues[i]['ask_size']) for i in range(len(venues)))
            cumulative_cash += cost_est
            unfilled = max(unfilled - filled, 0)

            if unfilled == 0:
                break

            #reset for next timestamp
            venues = []
            current_ts = ts

        #accumulate venue info for this timestamp
        venues.append({
            'ask': data['ask_px_00'],
            'ask_size': data['ask_sz_00'],
            'fee': 0.0,
            'rebate': 0.0,
        })

    consumer.close()

    optimized = {
        'total_cash': cumulative_cash,
        'avg_fill_px': (cumulative_cash / ORDER_SIZE) if ORDER_SIZE else 0
    }
    #run baselines on the collected snapshots
    baselines = {
        'best_ask': run_best_ask(snapshots),
        'twap': run_twap(snapshots),
        'vwap': run_vwap(snapshots),
    }
    savings = {}
    for key, base in baselines.items():
        base_avg = base['avg_fill_px']
        if base_avg and optimized['avg_fill_px'] is not None:
            savings[key] = ((base_avg - optimized['avg_fill_px']) / base_avg) * 10000
        else:
            savings[key] = 0.0
    return optimized, baselines, savings


def grid_search():
    lambda_over_list = [0.1, 0.2, 0.3, 0.4, 0.5]
    lambda_under_list = [0.1, 0.2, 0.3, 0.4, 0.5]
    theta_queue_list = [0.0, 0.1, 0.2, 0.3, 0.4]

    best_result = None
    best_params = None
    #track by best savings vs best_ask
    best_savings = -math.inf

    for lambda_over, lambda_under, theta_queue in itertools.product(
            lambda_over_list, lambda_under_list, theta_queue_list):
        optimized, baselines, savings = run_backtest_for_params(lambda_over, lambda_under, theta_queue)
        #pick metric: savings vs best_ask
        sav = savings.get('best_ask', 0.0)
        print(f"Params {(lambda_over, lambda_under, theta_queue)} -> savings vs best_ask = {sav:.2f} bps")
        if sav > best_savings:
            best_savings = sav
            best_result = (optimized, baselines, savings)
            best_params = {'lambda_over': lambda_over, 'lambda_under': lambda_under, 'theta_queue': theta_queue}

    return best_params, best_result


if __name__ == "__main__":
    #Run grid search
    best_params, best_result = grid_search()
    optimized, baselines, savings = best_result

    final = {
        'best_parameters': best_params,
        'optimized': optimized,
        'baselines': baselines,
        'savings_vs_baselines_bps': savings
    }
    print("\nFinal result:")
    print(json.dumps(final, indent=2))
