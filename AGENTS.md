# Agent Instructions — NL Chemicals Analysis

See [README.md](README.md) for full project overview.

## Running locally

```bash
pip install -r requirements.txt

# Step 1 — pull data (writes data/)
python scripts/pull_nl_chem_data.py --outdir data --start 2015-01-01

# Step 2 — build dashboard (writes site/)
python scripts/build_nl_chem_analysis.py \
  --input data/nl_chem_inputs_monthly.csv \
  --metadata data/pull_metadata.json \
  --template web/dashboard_template.html \
  --site-dir site

# Step 3 — preview dashboard
python -m http.server 8000 --directory site
```

The CI workflow at [.github/workflows/nl-chem-analysis.yml](.github/workflows/nl-chem-analysis.yml) runs the same two commands on a monthly schedule and deploys to GitHub Pages.

## Architecture

```
pull_nl_chem_data.py  →  data/nl_chem_inputs_monthly.csv
                               ↓
                    build_nl_chem_analysis.py
                               ↓
                site/index.html  (payload embedded at build time)
```

- All data sources are **public, no API keys required**.
- `site/index.html` is fully static; no backend is needed.
- Dashboard rendering: `build_nl_chem_analysis.py` replaces the literal string `__ANALYSIS_PAYLOAD__` in `web/dashboard_template.html` with the serialized JSON payload.

## Key conventions

| Topic | Convention |
|-------|-----------|
| CSV column names | `chem_output_idx`, `chem_ppi_idx`, `brent_usd_per_bbl`, `ttf_gas_usd_per_mmbtu`, `capacity_util_pct` — case-sensitive |
| Dates | ISO 8601 `YYYY-MM-DD`, normalized to month-end; `parse_period_to_month_end()` handles quarters and mixed formats |
| Lag regressors | Named `{driver}_l{lag}`, e.g. `brent_l0`, `brent_l3`; max lag = 6 (burns 6 rows) |
| Capacity utilization | **Optional** column; if <24 non-NA values it is silently excluded from the model |
| Output directory | CI writes to `data/` and `site/`; `testdata/` holds checked-in test fixtures |

## Gotchas

- **Minimum observations**: the build script raises `AnalysisError` if fewer than 48 rows remain after lag construction.
- **Hardcoded data in `_update_testdata.ps1`**: Brent prices and capacity utilization are manually maintained in that script and need updating after the last hardcoded date.
- **NaN/inf in JSON**: `_safe_float()` coerces non-finite values to `null`; never write raw `float('nan')` into the payload.
- **Eurostat dataset IDs** (`sts_inpr_m`, `sts_inpp_m`, `teibs070`) are hardcoded; if Eurostat renames a dataset the pull script will fail.
- **Bootstrap P10/P90** are residual-bootstrap bands around the model point estimate — not structural forecasts.
