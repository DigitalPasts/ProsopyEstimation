# Where Every Number Comes From

This document traces every statistic shown on the visualization page back to its source file and calculation.

---

## Header Stats Bar

| Statistic | Value | Source | Calculation |
|-----------|-------|--------|-------------|
| Total tablets | 4,589 | `data/output/estimation_results.csv` | Row count (one row per unique tablet) |
| Pre-dated | 3,415 | `data/output/estimation_results.csv` | `status == 'pre_dated'` count. Also confirmed by: unique tablets with a numeric `Split_Julian_dates` in `preprocessed_whole_data.csv` = 3,415 |
| Newly estimated | 1,001 | `data/output/estimation_results.csv` | `status == 'newly_estimated'` count |
| Unestimatable | 173 | `data/output/estimation_results.csv` | `status == 'unestimatable'` count |
| Check | 3,415 + 1,001 + 173 = **4,589** ✓ | | |
| Validation accuracy | 77.8% | `data/output/validation_results_after_correction.csv` | Hits / estimated: 2,619 / 3,365 = 77.83% |

---

## Chart 1 — Timeline (Gap-Filling)

**Source:** `data/output/estimation_results.csv`

- **Pre-dated bars**: `julian_date` column of rows where `status == 'pre_dated'`, binned into 5-year intervals (`300–720 BCE`)
- **Newly-estimated bars**: midpoint of (`estimated_from` + `estimated_to`) / 2 for `status == 'newly_estimated'` rows, negated (since the Yellow Pages convention stores years as negative values), then binned
- **Peak insight**: bin with the maximum `newly_estimated` count

---

## Chart 2 — Archive Benefit

**Source:** `data/output/estimation_results.csv` joined with `preprocessed_whole_data.csv`

- Each tablet's archive is taken from the preprocessed data (one archive per unique Tablet ID, using `drop_duplicates('Tablet ID')` to avoid double-counting from multiple attestation rows)
- Archives with fewer than 10 tablets are excluded
- `% newly dated` = `newly_estimated` / (`pre_dated` + `newly_estimated` + `unestimatable`) × 100

---

## Chart 3 — Most Productive Individuals

**Source:** `data/output/estimation_results.csv` + `preprocessed_whole_data.csv`

- **Anchor pool**: all PIDs that appear in at least one pre-dated tablet (these are the people in the "Yellow Pages")
- **Count**: for each anchor PID, the number of *newly-estimated* tablets they appear in (i.e., how many new datings they enabled)
- **King flag**: PID's `Role` field contains `"king in"` in the preprocessed data
- Top 20 by count are shown

---

## Chart 4 — Validation Accuracy

**Source:** `data/output/validation_results_after_correction.csv`

This is leave-one-out cross-validation: each of the 3,415 pre-dated tablets is removed from the corpus, its date re-estimated from the remaining data, then compared to the known date.

| Figure | Value | Calculation |
|--------|-------|-------------|
| Tablets in validation | 3,415 | Row count of `validation_results_after_correction.csv` |
| Got an estimate | 3,365 | Rows where `coe` is not NaN |
| Could not be estimated | 50 | Isolated nodes — no co-attested person with a known date range when that tablet is removed |
| Hits | 2,619 | `hit == True` among estimated rows |
| Misses | 746 | `hit == False` among estimated rows |
| **Hit rate** | **77.8%** | 2,619 / 3,365 × 100 |
| Median deviation (hits) | 3.0 years | Median of `deviation` column for hits |
| Median deviation (misses) | 9.2 years | Median of `deviation` column for misses |

**Hit definition:** the true Julian date falls within [`estimated_from`, `estimated_to`] ±1 year tolerance.

**Which validation file is used:** `validation_results_after_correction.csv` (no `max_active_years` constraint on the Yellow Pages). The file `validation_results_after_correction_17.csv` applies a stricter `max_active_years=17` filter and gives 65.1% — this is **not** the number reported in the paper.

---

## Known Data Note — Tablet 4873

Tablet 4873 has `Split_Julian_dates = 531` in the preprocessed data and is treated as pre-dated in the validation file. In the original `estimation_results.csv`, it was erroneously classified as `newly_estimated` (with `julian_date = NaN`) due to a data issue in that pipeline run. This has been corrected in the current `estimation_results.csv`: tablet 4873 is now `pre_dated` with `julian_date = 531`.
