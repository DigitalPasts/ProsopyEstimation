"""
threshold_sweep.py
------------------
Runs iterative date estimation for max_active_years = 1..30
using the corrected preprocessed data, capturing per-threshold stats:
  - newly_estimated   : number of undated tablets successfully estimated
  - unestimatable     : tablets that couldn't be reached
  - coverage_pct      : (pre_dated + newly_estimated) / total  * 100
  - avg_range_yrs     : mean width of the estimated interval
  - median_range_yrs  : median width
  - std_range_yrs     : std dev of widths

Note: estimate_date() uses tiered fallback internally
(threshold → 2×threshold → 3×threshold), so a threshold of N
means "prefer people active ≤N years, fall back to ≤2N, then ≤3N".

Output: ../data/output/threshold_sweep_results.csv
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import pandas as pd
import numpy as np

# ── path setup ──────────────────────────────────────────────────────────────
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPTS_DIR)

PREPROCESSED_PATH = os.path.abspath(os.path.join(
    SCRIPTS_DIR, '..', '..', 'ProsopyBase',
    'data', 'processed', 'preprocessed_whole_data.csv'
))
if not os.path.exists(PREPROCESSED_PATH):
    PREPROCESSED_PATH = os.path.abspath(os.path.join(
        SCRIPTS_DIR, '..', 'data', 'input', 'preprocessed_whole_data.csv'
    ))
assert os.path.exists(PREPROCESSED_PATH), f"Preprocessed data not found: {PREPROCESSED_PATH}"
print(f"Using: {PREPROCESSED_PATH}\n")

from yellow_pages import YellowPages
from estimation import estimate_date
from helpers import get_fully_dated_rows_by_julian, get_dateless_rows

# ── load data once ──────────────────────────────────────────────────────────
df_base = pd.read_csv(PREPROCESSED_PATH).copy()
df_base['Split_Julian_dates'] = df_base['Split_Julian_dates'].apply(
    lambda x: str(int(x)) if pd.notna(x) else None
)

pre_dated_mask    = df_base['Split_Julian_dates'].str.match(r'^\d{3,4}$', na=False)
pre_dated_tablets = set(df_base.loc[pre_dated_mask, 'Tablet ID'].unique())
all_tablets       = set(df_base['Tablet ID'].unique())

n_pre   = len(pre_dated_tablets)
n_total = len(all_tablets)

print(f"Total tablets : {n_total}")
print(f"Pre-dated     : {n_pre}")
print(f"Undated (pool): {n_total - n_pre}")
print()
print(f"{'Threshold':>9}  {'Newly est.':>10}  {'Unest.':>7}  {'Coverage%':>9}  {'AvgRange':>9}  {'MedRange':>9}  {'StdRange':>9}")
print(f"  {'-'*70}")

results = []

for threshold in range(1, 31):
    yp           = YellowPages(df_base.copy())
    dateless     = get_dateless_rows(df_base.copy()).copy()
    dateless['estimated_from_year'] = None
    dateless['estimated_to_year']   = None

    estimated    = set()
    est_ranges   = []

    while True:
        new_estimates = 0
        for tab_id, tablet in dateless.copy().groupby('Tablet ID'):
            if tab_id in estimated:
                continue
            est_from, est_to = estimate_date(tablet, yp, max_active_years=threshold)
            if est_from is None:
                continue
            dateless.loc[tablet.index, 'estimated_from_year'] = est_from
            dateless.loc[tablet.index, 'estimated_to_year']   = est_to
            est_ranges.append(abs(est_to - est_from))
            for idx in tablet.index:
                pid  = tablet.at[idx, 'PID']
                role = tablet.at[idx, 'Role']
                pdata = yp.get_person_data(pid)
                if pdata is None:
                    yp.add_person(pid, tablet.at[idx, 'ind.Name'], [role], est_from, est_to)
                else:
                    yp.update_person(pid, role, est_from, est_to)
            estimated.add(tab_id)
            new_estimates += 1
        if new_estimates == 0:
            break

    newly_estimated = len(estimated)
    unestimatable   = n_total - n_pre - newly_estimated
    coverage        = (n_pre + newly_estimated) / n_total * 100
    avg_r           = float(np.mean(est_ranges))   if est_ranges else 0.0
    med_r           = float(np.median(est_ranges)) if est_ranges else 0.0
    std_r           = float(np.std(est_ranges))    if est_ranges else 0.0

    results.append({
        'max_active_years':  threshold,
        'pre_dated':         n_pre,
        'newly_estimated':   newly_estimated,
        'unestimatable':     unestimatable,
        'total_estimated':   n_pre + newly_estimated,
        'coverage_pct':      round(coverage, 2),
        'avg_range_yrs':     round(avg_r, 2),
        'median_range_yrs':  round(med_r, 2),
        'std_range_yrs':     round(std_r, 2),
    })

    print(f"  {threshold:>9}  {newly_estimated:>10}  {unestimatable:>7}  {coverage:>8.2f}%  "
          f"{avg_r:>9.2f}  {med_r:>9.2f}  {std_r:>9.2f}")

out_csv = os.path.abspath(os.path.join(SCRIPTS_DIR, '..', 'data', 'output', 'threshold_sweep_results.csv'))
os.makedirs(os.path.dirname(out_csv), exist_ok=True)
pd.DataFrame(results).to_csv(out_csv, index=False)
print(f"\nSaved to: {out_csv}")
