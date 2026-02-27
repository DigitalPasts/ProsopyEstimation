"""
generate_data.py

Generate JSON data files for the GitHub Pages visualization.
Run from the docs/ directory:

    python generate_data.py

Prerequisites (place these files before running):
  ../data/output/estimation_results.csv
  ../data/output/validation_results_after_correction.csv
  ../data/input/preprocessed_whole_data.csv  (from ProsopyBase)
"""

import json
import os
import pandas as pd

ESTIMATION_PATH    = '../data/output/estimation_results.csv'
VALIDATION_PATH    = '../data/output/validation_results_after_correction.csv'
OUTPUT_DIR         = 'data'

# Preprocessed data: try ProsopyBase sibling repo first, then local data/input
_prosopybase = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'ProsopyBase', 'data', 'processed', 'preprocessed_whole_data.csv'))
_local       = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'input', 'preprocessed_whole_data.csv'))
PREPROCESSED_PATH  = _prosopybase if os.path.exists(_prosopybase) else _local

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
print("Loading CSVs...")
est    = pd.read_csv(ESTIMATION_PATH)
val    = pd.read_csv(VALIDATION_PATH)
pre    = pd.read_csv(PREPROCESSED_PATH, low_memory=False)

est['julian_date']    = pd.to_numeric(est['julian_date'],    errors='coerce')
est['estimated_from'] = pd.to_numeric(est['estimated_from'], errors='coerce')
est['estimated_to']   = pd.to_numeric(est['estimated_to'],   errors='coerce')

pre['Tablet ID'] = pd.to_numeric(pre['Tablet ID'], errors='coerce')
pre = pre.dropna(subset=['Tablet ID'])
pre['Tablet ID'] = pre['Tablet ID'].astype(int)

# Normalise hit column (CSV stores Python booleans as strings)
val['hit'] = val['hit'].map({True: True, False: False, 'True': True, 'False': False})

total = len(est)
n_pre  = int((est['status'] == 'pre_dated').sum())
n_new  = int((est['status'] == 'newly_estimated').sum())
n_une  = int((est['status'] == 'unestimatable').sum())
n_hit  = int(val['hit'].sum())
n_est  = int(val['hit'].notna().sum())

print(f"  Tablets: {total}  |  pre-dated: {n_pre}  |  newly-estimated: {n_new}  |  unestimatable: {n_une}")
print(f"  Validation: {n_hit}/{n_est} hits ({n_hit/n_est:.1%})")

# ---------------------------------------------------------------------------
# 1. Timeline gap-filling
# ---------------------------------------------------------------------------
print("Building timeline data...")

def to_bin(year, width=5):
    return int((year // width) * width)

# Pre-dated: julian_date is stored as positive BCE (e.g. 490 = 490 BCE)
pre_years = est[est['status'] == 'pre_dated']['julian_date'].dropna().astype(int)

# Newly-estimated: estimated_from/to are stored as NEGATIVE (Yellow Pages convention)
# Convert midpoint to positive BCE
ne = est[est['status'] == 'newly_estimated'].copy()
ne['midpoint_bce'] = -((ne['estimated_from'] + ne['estimated_to']) / 2)
new_years = ne['midpoint_bce'].dropna()

bins = list(range(300, 725, 5))

pre_counts = pre_years.apply(to_bin).value_counts().reindex(bins, fill_value=0).tolist()
new_counts = new_years.apply(to_bin).value_counts().reindex(bins, fill_value=0).tolist()

timeline_data = {
    'bins': bins,
    'pre_dated': pre_counts,
    'newly_estimated': new_counts,
    'summary': {
        'total_pre_dated':       n_pre,
        'total_newly_estimated': n_new,
        'total_unestimatable':   n_une,
    }
}

with open(f'{OUTPUT_DIR}/timeline.json', 'w', encoding='utf-8') as f:
    json.dump(timeline_data, f)
print(f"  -> timeline.json")

# ---------------------------------------------------------------------------
# 2. Archive benefit analysis
# ---------------------------------------------------------------------------
print("Building archive benefit data...")

# One archive per tablet (avoid double-counting from multiple attestation rows)
tablet_archive = pre.drop_duplicates('Tablet ID')[['Tablet ID', 'Archive']]
merged = est.merge(tablet_archive, on='Tablet ID', how='left')
merged['Archive'] = merged['Archive'].fillna('Unknown')

archive_stats = (
    merged
    .groupby('Archive')['status']
    .value_counts()
    .unstack(fill_value=0)
)
for col in ['pre_dated', 'newly_estimated', 'unestimatable']:
    if col not in archive_stats.columns:
        archive_stats[col] = 0

archive_stats['total'] = (
    archive_stats['pre_dated'] + archive_stats['newly_estimated'] + archive_stats['unestimatable']
)
archive_stats['pct_newly_dated'] = (
    archive_stats['newly_estimated'] / archive_stats['total'] * 100
).round(1)

# Keep only archives with ≥ 10 tablets
archive_stats = archive_stats[archive_stats['total'] >= 10].copy()
archive_stats = archive_stats.sort_values('pct_newly_dated', ascending=True)

archive_data = {
    'archives':        archive_stats.index.tolist(),
    'pre_dated':       [int(x) for x in archive_stats['pre_dated']],
    'newly_estimated': [int(x) for x in archive_stats['newly_estimated']],
    'unestimatable':   [int(x) for x in archive_stats['unestimatable']],
    'pct_newly_dated': archive_stats['pct_newly_dated'].tolist(),
    'total':           [int(x) for x in archive_stats['total']],
}

with open(f'{OUTPUT_DIR}/archive_benefit.json', 'w', encoding='utf-8') as f:
    json.dump(archive_data, f, ensure_ascii=False)
print(f"  -> archive_benefit.json  ({len(archive_stats)} archives)")

# ---------------------------------------------------------------------------
# 3. Most productive people
# ---------------------------------------------------------------------------
print("Building top-people data...")

pre_tablet_ids = set(est[est['status'] == 'pre_dated']['Tablet ID'])
new_tablet_ids = set(est[est['status'] == 'newly_estimated']['Tablet ID'])

# PIDs that appear in at least one pre-dated tablet (Yellow Pages anchors)
anchors = set(pre[pre['Tablet ID'].isin(pre_tablet_ids)]['PID'].unique())

# Count newly-estimated tablets each anchor appears in
new_rows = pre[pre['Tablet ID'].isin(new_tablet_ids) & pre['PID'].isin(anchors)]
pid_counts = new_rows.groupby('PID')['Tablet ID'].nunique().sort_values(ascending=False).head(20)

# Resolve names from the preprocessed data
pid_info = (
    pre.drop_duplicates('PID')
    .set_index('PID')[['ind.Name', 'ind.Patronym', 'ind.Family name']]
)

# Identify king PIDs (role contains "king in")
king_pids = set(pre[pre['Role'].str.contains('king in', na=False)]['PID'].unique())

people_list = []
for pid, count in pid_counts.items():
    info = pid_info.loc[pid] if pid in pid_info.index else {}
    name     = str(info.get('ind.Name',          pid) if hasattr(info, 'get') else pid)
    patronym = str(info.get('ind.Patronym',       '-') if hasattr(info, 'get') else '-')
    family   = str(info.get('ind.Family name',    '-') if hasattr(info, 'get') else '-')
    is_king  = int(pid) in king_pids

    # Build display label
    parts = [name]
    if patronym not in ('-', 'nan', '', 'None'):
        parts.append(f"s/o {patronym}")
    if family not in ('-', 'nan', '', '[...]', 'None'):
        parts.append(f"({family})")
    if is_king:
        parts.append("👑")
    display_name = ' '.join(parts)

    people_list.append({
        'pid':                  int(pid),
        'display_name':         display_name,
        'name':                 name,
        'patronym':             patronym,
        'family':               family,
        'is_king':              is_king,
        'newly_estimated_count': int(count),
    })

with open(f'{OUTPUT_DIR}/top_people.json', 'w', encoding='utf-8') as f:
    json.dump(people_list, f, ensure_ascii=True)
print(f"  -> top_people.json  (top {len(people_list)} people)")

# ---------------------------------------------------------------------------
# 4. Validation accuracy
# ---------------------------------------------------------------------------
print("Building validation data...")

val_est = val.dropna(subset=['coe']).copy()

# Convert negative years to positive BCE for display
val_est['actual_bce'] = (-val_est['actual_year']).round(0).astype(int)
val_est['coe_bce']    = (-val_est['coe']).round(1)
val_est['dev']        = val_est['deviation'].round(1)

hits   = val_est[val_est['hit'] == True]
misses = val_est[val_est['hit'] == False]

validation_data = {
    'scatter': {
        'hits': {
            'actual': hits['actual_bce'].tolist(),
            'coe':    hits['coe_bce'].tolist(),
        },
        'misses': {
            'actual': misses['actual_bce'].tolist(),
            'coe':    misses['coe_bce'].tolist(),
        },
    },
    'deviations': {
        'hits':   hits['dev'].tolist(),
        'misses': misses['dev'].tolist(),
    },
    'summary': {
        'total_dated':      int(len(val)),
        'total_estimated':  int(len(val_est)),
        'not_estimated':    int(val['hit'].isna().sum()),
        'hits':             int(len(hits)),
        'misses':           int(len(misses)),
        'hit_rate':         round(len(hits) / len(val_est) * 100, 1),
        'median_dev_hits':  round(float(hits['dev'].median()),   1) if len(hits)   else None,
        'median_dev_misses':round(float(misses['dev'].median()), 1) if len(misses) else None,
    }
}

with open(f'{OUTPUT_DIR}/validation.json', 'w', encoding='utf-8') as f:
    json.dump(validation_data, f)
print(f"  -> validation.json")

# ---------------------------------------------------------------------------
# 5. Threshold sweep (max_active_years 1–30)
# ---------------------------------------------------------------------------
SWEEP_PATH = '../data/output/threshold_sweep_results.csv'
if os.path.exists(SWEEP_PATH):
    print("Building threshold sweep data...")
    sweep = pd.read_csv(SWEEP_PATH)
    sweep_data = {
        'max_active_years':  sweep['max_active_years'].tolist(),
        'newly_estimated':   sweep['newly_estimated'].tolist(),
        'unestimatable':     sweep['unestimatable'].tolist(),
        'coverage_pct':      sweep['coverage_pct'].tolist(),
        'avg_range_yrs':     sweep['avg_range_yrs'].tolist(),
        'median_range_yrs':  sweep['median_range_yrs'].tolist(),
        'std_range_yrs':     sweep['std_range_yrs'].tolist(),
        'pre_dated':         int(sweep['pre_dated'].iloc[0]),
        'total':             int(sweep['total_estimated'].iloc[0] + sweep['unestimatable'].iloc[0]),
    }
    with open(f'{OUTPUT_DIR}/threshold_sweep.json', 'w', encoding='utf-8') as f:
        json.dump(sweep_data, f)
    print(f"  -> threshold_sweep.json  ({len(sweep)} thresholds)")
else:
    print(f"  Skipping threshold_sweep.json (file not found: {SWEEP_PATH})")

print("\nDone — all JSON files written to docs/data/")
