# ProsopyEstimation

Date estimation for undated Neo-Babylonian tablets using prosopographical co-occurrence networks.

This repository implements an iterative algorithm that estimates the date of undated tablets by identifying co-attested individuals whose active years are known, then using a weighted average of those ranges. It also provides cross-validation and the U_iter theoretical upper-bound measurement.

Part of the [Prosobab](https://prosobab.leidenuniv.nl/) analysis framework. Takes as input the preprocessed dataset from [ProsopyBase](https://github.com/DigitalPasts/ProsopyBase).

---

## Repository Structure

```
ProsopyEstimation/
├── data/
│   ├── input/
│   │   └── README.md                                     # How to get preprocessed_whole_data.csv
│   └── output/
│       ├── estimation_results.csv                        # All tablets with date status
│       ├── validation_results.csv                        # Initial validation run
│       ├── validation_results_after_correction.csv       # Post-PID-correction validation
│       └── validation_results_after_correction_17.csv    # Optimized (max_active_years=17)
└── scripts/
    ├── helpers.py          # Shared utility functions
    ├── yellow_pages.py     # Person reference database (YellowPages class)
    ├── estimation.py       # Core estimation algorithm
    ├── validation.py       # Leave-one-out cross-validation
    ├── u-iter.py           # U_iter theoretical upper-bound calculation
    └── main.py             # Pipeline runner + optional Gephi export
```

---

## Algorithm Overview

### Estimation (`estimation.py`)

1. Build a **Yellow Pages** — a dictionary of all people with known (attested) date ranges, weighted by document count.
2. For each undated tablet, find co-attested people with known ranges within a `max_active_years` threshold (default: 17 years). Falls back to 2× and 3× thresholds if no match found.
3. Estimate the tablet's date as a weighted average of those people's attested ranges.
4. **Iteratively**: newly estimated tablets update the Yellow Pages, enabling further tablets to be estimated in subsequent rounds. Repeats until no new estimates are made.

Output statuses:
- `pre_dated` — tablet had a date from the start
- `newly_estimated` — dated by the algorithm
- `unestimatable` — no path through the co-occurrence network

### Validation (`validation.py`)

Leave-one-out cross-validation on the fully dated subset:
- Each dated tablet is temporarily removed
- Its date is estimated from the remaining data
- Success = true date falls within the estimated range (±1 year tolerance)
- Reports: success rate, mean deviation, standard deviation

### U_iter (`u-iter.py`)

Computes the theoretical upper bound for structurally dateable tablets:
- A tablet is *structurally dateable* if it shares at least one person with a dated tablet (transitively, across the co-occurrence graph)
- Reports U_iter for both undated tablets (full dataset) and dated tablets (validation regime)

---

## Prerequisites

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Get the preprocessed data from [ProsopyBase](https://github.com/DigitalPasts/ProsopyBase) and place it in `data/input/`:
```bash
# See data/input/README.md for full instructions
cp /path/to/ProsopyBase/data/processed/preprocessed_whole_data.csv data/input/
```

---

## Usage

### Run the full pipeline (estimate + validate)
```bash
cd scripts
python main.py
```

Outputs:
- `data/output/estimation_results.csv`
- `data/output/validation_results_after_correction_17.csv`

### Run estimation only
```bash
cd scripts
python estimation.py
```

### Run validation only
```bash
cd scripts
python validation.py
```

### Compute U_iter
```bash
cd scripts
python u-iter.py
```

### Export tablet co-occurrence network for Gephi
```python
# From scripts/
from main import export_tablet_network_to_gephi
export_tablet_network_to_gephi()
# Output: data/output/tablet_network.gexf (not committed — regenerate as needed)
```

---

## Output Description

### `estimation_results.csv`

| Column | Description |
|---|---|
| `Tablet ID` | Unique tablet identifier |
| `status` | `pre_dated`, `newly_estimated`, or `unestimatable` |
| `julian_date` | Known Julian year (pre-dated tablets only) |
| `estimated_from` | Start of estimated range (newly estimated only) |
| `estimated_to` | End of estimated range (newly estimated only) |

### `validation_results_after_correction_17.csv`

| Column | Description |
|---|---|
| `Tablet ID` | Tablet identifier |
| `actual_year` | True Julian year (BCE, stored as positive int) |
| `estimated_from` / `estimated_to` | Estimated range |
| `coe` | Center of estimate |
| `deviation` | \|actual_year − coe\| |
| `hit` | True if actual year falls within estimated range ±1 |

---

## Data Source

Waerzeggers, C., Groß, M., et al. (2019). Prosobab: Prosopography of Babylonia (c. 620–330 BCE). Leiden University. https://prosobab.leidenuniv.nl.

Waerzeggers, C., & Groß, M. (2022). Prosobab (version 1.0). DANS Data Station Archaeology. https://doi.org/10.17026/dans-zvn-eece.
