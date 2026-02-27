"""
rerun_pipeline.py
-----------------
Re-runs estimation + validation from scratch using the corrected
preprocessed data (all 25 error corrections applied).

Run from the scripts/ directory:
    python rerun_pipeline.py

Output files (relative to scripts/):
    ../data/output/estimation_results.csv
    ../data/output/validation_results_after_correction.csv   (max_active_years=30)
    ../data/output/validation_results_after_correction_17.csv (max_active_years=17)
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')
import os
import time
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

PREPROCESSED_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__),
    '..', '..', '..', 'prosopy', 'ProsopyBase',
    'data', 'processed', 'preprocessed_whole_data.csv'
))

if not os.path.exists(PREPROCESSED_PATH):
    PREPROCESSED_PATH = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'data', 'input', 'preprocessed_whole_data.csv'
    ))

print(f"Using preprocessed data: {PREPROCESSED_PATH}")
assert os.path.exists(PREPROCESSED_PATH), f"File not found: {PREPROCESSED_PATH}"

# Patch module-level path variables before importing
import estimation as _est_mod
import validation as _val_mod
_est_mod.preprocessed_prosobab_data = PREPROCESSED_PATH
_val_mod.preprocessed_prosobab_data = PREPROCESSED_PATH

from estimation import estimate
from validation import validate_known_tablets

OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'output'))
os.makedirs(OUTPUT_DIR, exist_ok=True)

DIVIDER = "=" * 70

def print_section(title):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)

def summarize_validation(val_df, label):
    """Print a detailed breakdown of validation results."""
    total = len(val_df)
    estimated_df = val_df.dropna(subset=['estimated_from', 'estimated_to'])
    total_est = len(estimated_df)
    not_est = total - total_est

    eval_df = estimated_df.dropna(subset=['hit']).copy()
    eval_df['hit'] = eval_df['hit'].astype(bool)

    hits   = eval_df['hit'].sum()
    misses = len(eval_df) - hits

    devs        = eval_df['deviation'].dropna()
    hit_devs    = eval_df.loc[ eval_df['hit'], 'deviation'].dropna()
    miss_devs   = eval_df.loc[~eval_df['hit'], 'deviation'].dropna()

    print(f"\n  [{label}] VALIDATION SUMMARY")
    print(f"  {'─'*50}")
    print(f"  Total pre-dated tablets:          {total:>6}")
    print(f"  ├─ Got an estimate:               {total_est:>6}  ({total_est/total*100:.1f}%)")
    print(f"  └─ Could NOT be estimated:        {not_est:>6}  ({not_est/total*100:.1f}%)  (isolated nodes — excluded from hit/miss)")
    print()
    print(f"  Of the {total_est} estimated tablets:")
    print(f"  ├─ ✅ Hits:                        {hits:>6}  ({hits/total_est*100:.2f}%)")
    print(f"  └─ ❌ Misses:                      {misses:>6}  ({misses/total_est*100:.2f}%)")
    print()
    print(f"  Deviation stats (all estimated):")
    print(f"  ├─ Mean:       {devs.mean():>8.2f} years")
    print(f"  ├─ Median:     {devs.median():>8.2f} years")
    print(f"  └─ Std dev:    {devs.std():>8.2f} years")
    print()
    if len(hit_devs) > 0:
        print(f"  Deviation stats (hits only):")
        print(f"  ├─ Mean:       {hit_devs.mean():>8.2f} years")
        print(f"  ├─ Median:     {hit_devs.median():>8.2f} years")
        print(f"  └─ Max:        {hit_devs.max():>8.2f} years")
    if len(miss_devs) > 0:
        print()
        print(f"  Deviation stats (misses only):")
        print(f"  ├─ Mean:       {miss_devs.mean():>8.2f} years")
        print(f"  ├─ Median:     {miss_devs.median():>8.2f} years")
        print(f"  └─ Max:        {miss_devs.max():>8.2f} years")
    print()
    # Show worst misses (largest deviations)
    print(f"  Top 5 largest deviations (worst misses):")
    worst = eval_df.nlargest(5, 'deviation')[['Tablet ID', 'actual_year', 'coe', 'deviation', 'hit']]
    for _, row in worst.iterrows():
        flag = "✅" if row['hit'] else "❌"
        print(f"    {flag} Tablet {int(row['Tablet ID']):>5}  actual={int(row['actual_year']):>6} BCE  "
              f"coe={row['coe']:>9.1f}  dev={row['deviation']:>7.1f} yrs")

# ---------------------------------------------------------------------------
# STEP 1: Estimation
# ---------------------------------------------------------------------------
print_section("STEP 1: DATE ESTIMATION")
t0 = time.time()
est_output = os.path.join(OUTPUT_DIR, 'estimation_results.csv')
estimate(output_path=est_output)
est_elapsed = time.time() - t0
print(f"\n  Estimation completed in {est_elapsed:.1f}s")

# Report estimation results
est_df = pd.read_csv(est_output)
total   = len(est_df)
n_pre   = (est_df['status'] == 'pre_dated').sum()
n_new   = (est_df['status'] == 'newly_estimated').sum()
n_une   = (est_df['status'] == 'unestimatable').sum()
print(f"\n  ESTIMATION RESULTS")
print(f"  {'─'*50}")
print(f"  Total tablets:                    {total:>6}")
print(f"  ├─ Pre-dated:                     {n_pre:>6}  ({n_pre/total*100:.1f}%)")
print(f"  ├─ Newly estimated:               {n_new:>6}  ({n_new/total*100:.1f}%)")
print(f"  └─ Unestimatable:                 {n_une:>6}  ({n_une/total*100:.1f}%)")
print(f"\n  Saved to: {est_output}")

# ---------------------------------------------------------------------------
# Load preprocessed data for validation (shared)
# ---------------------------------------------------------------------------
print_section("Loading preprocessed data for validation")
df_raw = pd.read_csv(PREPROCESSED_PATH).copy()
# Use the error-corrected Split_Julian_dates from the preprocessed CSV directly.
df_raw['Split_Julian_dates'] = df_raw['Split_Julian_dates'].apply(
    lambda x: str(int(x)) if pd.notna(x) else None
)
from helpers import get_fully_dated_rows_by_julian
fully_dated = get_fully_dated_rows_by_julian(df_raw)
print(f"  Fully-dated tablets available for leave-one-out: {fully_dated['Tablet ID'].nunique()}")

# ---------------------------------------------------------------------------
# STEP 2a: Validation — max_active_years=30  (reproduces after_correction.csv)
# ---------------------------------------------------------------------------
print_section("STEP 2a: VALIDATION  (max_active_years=30)")
print("  This reproduces validation_results_after_correction.csv")
print("  Tiered fallback thresholds: 30 → 60 → 90 years")
t1 = time.time()
val_30 = validate_known_tablets(df_raw.copy(), max_active_years=30)
val30_elapsed = time.time() - t1
out_30 = os.path.join(OUTPUT_DIR, 'validation_results_after_correction.csv')
val_30.to_csv(out_30, index=False)
print(f"\n  Validation (30) completed in {val30_elapsed/60:.1f} min")
print(f"  Saved to: {out_30}")
summarize_validation(val_30, "max_active_years=30")

# ---------------------------------------------------------------------------
# STEP 2b: Validation — max_active_years=17  (reproduces _17.csv)
# ---------------------------------------------------------------------------
print_section("STEP 2b: VALIDATION  (max_active_years=17)")
print("  This reproduces validation_results_after_correction_17.csv")
print("  Tiered fallback thresholds: 17 → 34 → 51 years")
t2 = time.time()
val_17 = validate_known_tablets(df_raw.copy(), max_active_years=17)
val17_elapsed = time.time() - t2
out_17 = os.path.join(OUTPUT_DIR, 'validation_results_after_correction_17.csv')
val_17.to_csv(out_17, index=False)
print(f"\n  Validation (17) completed in {val17_elapsed/60:.1f} min")
print(f"  Saved to: {out_17}")
summarize_validation(val_17, "max_active_years=17")

# ---------------------------------------------------------------------------
# Final comparison
# ---------------------------------------------------------------------------
print_section("COMPARISON SUMMARY")

def quick_stats(df):
    est = df.dropna(subset=['coe'])
    hits = est[est['hit'].map({True: True, False: False, 'True': True, 'False': False}) == True]
    return len(df), len(est), len(hits), round(len(hits)/len(est)*100, 2) if len(est) else 0

t_30, e_30, h_30, r_30 = quick_stats(val_30)
t_17, e_17, h_17, r_17 = quick_stats(val_17)

print(f"\n  {'Variant':<35} {'Total':>6} {'Estimated':>10} {'Hits':>8} {'Hit Rate':>10}")
print(f"  {'─'*70}")
print(f"  {'after_correction (max_active=30)':<35} {t_30:>6} {e_30:>10} {h_30:>8} {r_30:>9.2f}%")
print(f"  {'after_correction_17 (max_active=17)':<35} {t_17:>6} {e_17:>10} {h_17:>8} {r_17:>9.2f}%")

total_time = time.time() - t0
print(f"\n  Total pipeline time: {total_time/60:.1f} minutes")
print(f"\n  Next step: regenerate the visualization JSON files by running")
print(f"  docs/generate_data.py (or _run_generate.py with the ProsopyBase path)")
print(f"\n{DIVIDER}")
print("  PIPELINE COMPLETE")
print(DIVIDER)
