# NL chemicals analysis on GitHub Actions

This bundle gives you a no-key pull layer, a distributed-lag analysis builder, and a static HTML dashboard for the Netherlands chemicals use case.

## What it does

1. Pulls monthly data for:
   - Netherlands chemicals output index
   - Netherlands chemicals PPI
   - Brent crude
   - Europe gas / TTF
   - Netherlands manufacturing capacity utilization proxy
2. Runs lagged correlations and a distributed-lag regression.
3. Produces a standalone static dashboard in `site/index.html`.
4. Publishes the dashboard with GitHub Pages and uploads the full output as a workflow artifact.

## Repo layout

```text
.
├── .github/workflows/nl-chem-analysis.yml
├── requirements.txt
├── scripts/
│   ├── pull_nl_chem_data.py
│   └── build_nl_chem_analysis.py
└── web/
    └── dashboard_template.html
```

## How to use it

1. Drop these files into a repository.
2. In GitHub, go to **Settings → Pages** and set the publishing source to **GitHub Actions**.
3. Push to the default branch.
4. Run the workflow manually from **Actions**, or wait for the monthly schedule.
5. The workflow will:
   - build `data/`
   - build `site/`
   - upload both as an artifact
   - deploy `site/` to GitHub Pages

## What the model is doing

- Dependent variable: monthly log growth in Dutch chemicals output.
- Drivers: monthly log changes in Brent, TTF gas, chemicals PPI, plus capacity utilization if available.
- Lag window: 0-6 months.
- Estimator: OLS with HAC robust standard errors.
- Scenario engine: applies user-defined shocks through the lag coefficients and overlays residual-bootstrap ranges.

## Files produced by the workflow

- `data/nl_chem_inputs_monthly.csv`
- `data/pull_metadata.json`
- `site/index.html`
- `site/analysis_payload.json`
- `site/lag_correlations.csv`
- `site/regression_coefficients.csv`
- `site/model_summary.txt`

## Notes

- Capacity utilization is a manufacturing-wide proxy and is repeated from quarterly to monthly.
- The dashboard is static: it does not need a backend.
- The scenario tool reports modeled output impact, not a structural forecast.
