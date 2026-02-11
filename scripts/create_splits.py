"""
Create reproducible stratified train/dev/test splits from data/all.json.
Usage:
  python scripts/create_splits.py
This writes train.json, dev.json, test.json and split_verification.json in the project root.
"""
import json, os, math, random, collections

WD = os.path.dirname(os.path.dirname(__file__))
ALL_PATH = os.path.join(WD, 'data', 'all.json')
TRAIN_OUT = os.path.join(WD, 'data', 'train.json')
DEV_OUT = os.path.join(WD, 'data', 'dev.json')
TEST_OUT = os.path.join(WD, 'data', 'test.json')
VER_OUT = os.path.join(WD, 'data', 'split_verification.json')

SEED = 42
TRAIN_FRAC = 0.70
DEV_FRAC = 0.15
TEST_FRAC = 0.15

if __name__ == '__main__':
    with open(ALL_PATH, 'r', encoding='utf-8') as f:
        items = json.load(f)

    n = len(items)
    id_to_item = {it['id']: it for it in items}
    all_ids = [it['id'] for it in items]

    # group by (difficulty, table)
    groups = collections.defaultdict(list)
    for it in items:
        key = (it['difficulty_level'], it['table'])
        groups[key].append(it['id'])

    target_train = int(round(n * TRAIN_FRAC))
    target_dev = int(round(n * DEV_FRAC))
    target_test = n - target_train - target_dev

    group_alloc = {}
    for k, ids in groups.items():
        g = len(ids)
        ideal_train = g * TRAIN_FRAC
        ideal_dev = g * DEV_FRAC
        f_train = math.floor(ideal_train)
        f_dev = math.floor(ideal_dev)
        f_test = g - f_train - f_dev
        group_alloc[k] = {
            'size': g,
            'ideal_train': ideal_train,
            'ideal_dev': ideal_dev,
            'train': f_train,
            'dev': f_dev,
            'test': f_test,
        }

    def adjust_for_target(split_key, target_total):
        current_total = sum(v[split_key] for v in group_alloc.values())
        deficit = target_total - current_total
        if deficit == 0:
            return
        fracs = []
        for k, v in group_alloc.items():
            frac = v['ideal_' + split_key] - v[split_key]
            capacity = v['size'] - v['train'] - v['dev']
            fracs.append((frac, k, capacity))
        fracs.sort(key=lambda x: x[0], reverse=True)
        i = 0
        while deficit > 0 and i < len(fracs):
            frac, k, cap = fracs[i]
            if cap > 0:
                group_alloc[k][split_key] += 1
                group_alloc[k]['test'] = group_alloc[k]['size'] - group_alloc[k]['train'] - group_alloc[k]['dev']
                deficit -= 1
            i += 1
        if deficit > 0:
            keys = list(group_alloc.keys())
            idx = 0
            while deficit > 0:
                k = keys[idx % len(keys)]
                if group_alloc[k][split_key] < group_alloc[k]['size']:
                    group_alloc[k][split_key] += 1
                    group_alloc[k]['test'] = group_alloc[k]['size'] - group_alloc[k]['train'] - group_alloc[k]['dev']
                    deficit -= 1
                idx += 1

    adjust_for_target('train', target_train)
    adjust_for_target('dev', target_dev)

    # Final reconciliation
    sum_train = sum(v['train'] for v in group_alloc.values())
    sum_dev = sum(v['dev'] for v in group_alloc.values())
    if sum_train != target_train:
        diff = target_train - sum_train
        for k, v in group_alloc.items():
            if diff == 0:
                break
            can = v['test'] if diff > 0 else v['train']
            change = min(abs(diff), can)
            if change <= 0:
                continue
            if diff > 0:
                group_alloc[k]['train'] += change
            else:
                group_alloc[k]['train'] -= change
            group_alloc[k]['test'] = group_alloc[k]['size'] - group_alloc[k]['train'] - group_alloc[k]['dev']
            diff = target_train - sum(v['train'] for v in group_alloc.values())

    if sum_dev != target_dev:
        diff = target_dev - sum_dev
        for k, v in group_alloc.items():
            if diff == 0:
                break
            can = v['test'] if diff > 0 else v['dev']
            change = min(abs(diff), can)
            if change <= 0:
                continue
            if diff > 0:
                group_alloc[k]['dev'] += change
            else:
                group_alloc[k]['dev'] -= change
            group_alloc[k]['test'] = group_alloc[k]['size'] - group_alloc[k]['train'] - group_alloc[k]['dev']
            diff = target_dev - sum(v['dev'] for v in group_alloc.values())

    # Assign ids deterministically per group
    rnd = random.Random(SEED)
    train_ids = []
    dev_ids = []
    test_ids = []
    for k, ids in sorted(groups.items()):
        ids_sorted = list(ids)
        rnd.shuffle(ids_sorted)
        alloc = group_alloc[k]
        tcount = alloc['train']
        dcount = alloc['dev']
        train_chunk = ids_sorted[:tcount]
        dev_chunk = ids_sorted[tcount:tcount + dcount]
        test_chunk = ids_sorted[tcount + dcount:]
        train_ids.extend(train_chunk)
        dev_ids.extend(dev_chunk)
        test_ids.extend(test_chunk)

    # Verify uniqueness
    all_assigned = set(train_ids) | set(dev_ids) | set(test_ids)
    assert len(all_assigned) == n, f'assigned {len(all_assigned)} != total {n}'

    # Preserve original order
    train_list = [id_to_item[i] for i in all_ids if i in set(train_ids)]
    dev_list = [id_to_item[i] for i in all_ids if i in set(dev_ids)]
    test_list = [id_to_item[i] for i in all_ids if i in set(test_ids)]

    # Write outputs
    with open(TRAIN_OUT, 'w', encoding='utf-8') as f:
        json.dump(train_list, f, ensure_ascii=False, indent=2)
    with open(DEV_OUT, 'w', encoding='utf-8') as f:
        json.dump(dev_list, f, ensure_ascii=False, indent=2)
    with open(TEST_OUT, 'w', encoding='utf-8') as f:
        json.dump(test_list, f, ensure_ascii=False, indent=2)

    # Verification report
    from collections import Counter

    def breakdown(lst):
        diff = Counter([it['difficulty_level'] for it in lst])
        tab = Counter([it['table'] for it in lst])
        return diff, tab

    train_diff, train_tab = breakdown(train_list)
    dev_diff, dev_tab = breakdown(dev_list)
    test_diff, test_tab = breakdown(test_list)

    ver_report = {
        'total': n,
        'targets': {'train': target_train, 'dev': target_dev, 'test': target_test},
        'actual': {'train': len(train_list), 'dev': len(dev_list), 'test': len(test_list)},
        'train_diff': dict(train_diff),
        'dev_diff': dict(dev_diff),
        'test_diff': dict(test_diff),
        'train_tab': dict(train_tab),
        'dev_tab': dict(dev_tab),
        'test_tab': dict(test_tab),
    }
    with open(VER_OUT, 'w', encoding='utf-8') as f:
        json.dump(ver_report, f, ensure_ascii=False, indent=2)

    print('WROTE', TRAIN_OUT, DEV_OUT, TEST_OUT, VER_OUT)
