# Input Data

This folder should contain `preprocessed_whole_data.csv` from the
[ProsopyBase](https://github.com/DigitalPasts/ProsopyBase) repository before running the pipeline.

## How to get it

**Option 1 — Clone ProsopyBase (requires Git LFS):**
```bash
git clone https://github.com/DigitalPasts/ProsopyBase.git
cd ProsopyBase
git lfs pull
cp data/processed/preprocessed_whole_data.csv /path/to/ProsopyEstimation/data/input/
```

**Option 2 — Download directly from GitHub:**
Go to https://github.com/DigitalPasts/ProsopyBase and download `data/processed/preprocessed_whole_data.csv`.

## What the file contains

~15,000 attestations of ~11,500 individuals across ~4,000 Neo-Babylonian tablets,
cleaned and corrected by the ProsopyBase preprocessing pipeline.
