#!/usr/bin/env python3
"""
Build an end-to-end Netherlands chemicals analysis site from the pulled data.

Outputs:
- site/index.html              standalone dashboard with embedded payload
- site/analysis_payload.json   machine-readable payload for debugging/reuse
- site/lag_correlations.csv
- site/regression_coefficients.csv
- site/model_summary.txt

Usage:
    python scripts/build_nl_chem_analysis.py \
      --input data/nl_chem_inputs_monthly.csv \
      --metadata data/pull_metadata.json \
      --template web/dashboard_template.html \
      --site-dir site
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.stattools import durbin_watson

DEFAULT_MAX_LAG = 6
DEFAULT_BOOTSTRAP_DRAWS = 3000
RNG_SEED = 42

DRIVER_META = {
    "brent": {
        "level_col": "brent_usd_per_bbl",
        "diff_col": "d_brent",
        "label": "Brent",
        "unit": "USD/bbl",
    },
    "gas": {
        "level_col": "ttf_gas_usd_per_mmbtu",
        "diff_col": "d_gas",
        "label": "TTF gas",
        "unit": "USD/mmbtu",
    },
    "ppi": {
        "level_col": "chem_ppi_idx",
        "diff_col": "d_ppi",
        "label": "Chemicals PPI",
        "unit": "Index",
    },
}

DEFAULT_SCENARIOS = {
    "Oil shock": {
        "description": "Brent spike with modest gas and price pass-through",
        "horizon_months": 3,
        "brent_pct": 20.0,
        "gas_pct": 8.0,
        "ppi_pct": 4.0,
        "capacity_pts": 0.0,
    },
    "Gas squeeze": {
        "description": "TTF shock with partial output-price offset",
        "horizon_months": 3,
        "brent_pct": 5.0,
        "gas_pct": 35.0,
        "ppi_pct": 8.0,
        "capacity_pts": -1.0,
    },
    "Margin squeeze": {
        "description": "Input inflation with weak pricing and lower utilization",
        "horizon_months": 4,
        "brent_pct": 12.0,
        "gas_pct": 28.0,
        "ppi_pct": 3.0,
        "capacity_pts": -2.0,
    },
    "Demand rebound": {
        "description": "Input costs ease while pricing and utilization improve",
        "horizon_months": 3,
        "brent_pct": -6.0,
        "gas_pct": -12.0,
        "ppi_pct": 4.0,
        "capacity_pts": 1.5,
    },
}


@dataclass
class ModelArtifacts:
    model: Any
    model_df: pd.DataFrame
    y_col: str
    x_cols: List[str]
    coefficients: pd.DataFrame
    lag_correlations: pd.DataFrame
    cumulative_betas: pd.DataFrame
    residuals: np.ndarray
    residual_quantiles: Dict[str, float]


class AnalysisError(RuntimeError):
    pass


# -------------------------
# Generic helpers
# -------------------------
def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (float, int, np.floating, np.integer)):
        if np.isfinite(value):
            return float(value)
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if np.isfinite(num):
        return float(num)
    return None



def _clean_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _clean_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean_json(v) for v in value]
    if isinstance(value, tuple):
        return [_clean_json(v) for v in value]
    if isinstance(value, (pd.Timestamp, pd.Period)):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float, int)):
        return _safe_float(value)
    return value



def pct_from_log_change(log_change: float) -> float:
    return float(np.exp(log_change) - 1.0)



def log_change_from_pct(pct_value: float) -> float:
    return float(np.log1p(pct_value / 100.0))



def maybe_read_json(path: Optional[Path]) -> Optional[dict]:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


# -------------------------
# Data prep
# -------------------------
def load_input_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise AnalysisError(f"Input file not found: {path}")

    df = pd.read_csv(path)
    if "date" not in df.columns:
        raise AnalysisError("Input CSV must contain a date column.")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    required = ["chem_output_idx", "chem_ppi_idx", "brent_usd_per_bbl", "ttf_gas_usd_per_mmbtu"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise AnalysisError(f"Input CSV is missing required columns: {missing}")

    for col in required + ["capacity_util_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in required:
        if (df[col] <= 0).any():
            bad = int((df[col] <= 0).sum())
            raise AnalysisError(f"Column {col} contains {bad} non-positive values; log transforms require positive inputs.")

    return df



def prepare_model_dataframe(df: pd.DataFrame, max_lag: int) -> pd.DataFrame:
    work = df.copy()
    work["output_log_growth"] = np.log(work["chem_output_idx"]).diff()
    work["output_yoy_pct"] = np.log(work["chem_output_idx"]).diff(12) * 100.0

    for short, meta in DRIVER_META.items():
        work[meta["diff_col"]] = np.log(work[meta["level_col"]]).diff()
        work[f"{short}_yoy_pct"] = np.log(work[meta["level_col"]]).diff(12) * 100.0

    if "capacity_util_pct" in work.columns:
        work["capacity_util_pct"] = work["capacity_util_pct"].ffill()

    for short in DRIVER_META:
        base_col = DRIVER_META[short]["diff_col"]
        for lag in range(0, max_lag + 1):
            work[f"{short}_l{lag}"] = work[base_col].shift(lag)

    return work


# -------------------------
# Analytics
# -------------------------
def compute_lag_correlations(df: pd.DataFrame, max_lag: int) -> pd.DataFrame:
    rows: List[dict] = []
    y = df["output_log_growth"]
    for short, meta in DRIVER_META.items():
        diff_col = meta["diff_col"]
        for lag in range(0, max_lag + 1):
            corr = y.corr(df[diff_col].shift(lag))
            rows.append({
                "driver": short,
                "driver_label": meta["label"],
                "lag_months": lag,
                "correlation": _safe_float(corr),
            })
    return pd.DataFrame(rows)



def fit_distributed_lag_model(df: pd.DataFrame, max_lag: int) -> ModelArtifacts:
    x_cols: List[str] = []
    for short in DRIVER_META:
        x_cols.extend([f"{short}_l{lag}" for lag in range(0, max_lag + 1)])

    if "capacity_util_pct" in df.columns and df["capacity_util_pct"].notna().sum() >= 24:
        x_cols.append("capacity_util_pct")

    model_df = df.dropna(subset=["output_log_growth"] + x_cols).copy()
    if len(model_df) < 48:
        raise AnalysisError(
            f"Not enough observations after lag construction: {len(model_df)} rows. Need at least 48."
        )

    X = sm.add_constant(model_df[x_cols])
    y = model_df["output_log_growth"]
    model = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": max_lag})

    conf = model.conf_int()
    coeff_rows: List[dict] = []
    for name in model.params.index:
        driver = None
        lag = None
        if name.startswith(("brent_l", "gas_l", "ppi_l")):
            driver, lag_str = name.split("_l")
            lag = int(lag_str)
        elif name == "capacity_util_pct":
            driver = "capacity"
        coeff_rows.append({
            "term": name,
            "driver": driver,
            "lag_months": lag,
            "coef": _safe_float(model.params[name]),
            "std_err": _safe_float(model.bse[name]),
            "p_value": _safe_float(model.pvalues[name]),
            "ci_low": _safe_float(conf.loc[name, 0]),
            "ci_high": _safe_float(conf.loc[name, 1]),
        })
    coefficients = pd.DataFrame(coeff_rows)

    lag_correlations = compute_lag_correlations(df, max_lag)

    cumulative_rows: List[dict] = []
    for short, meta in DRIVER_META.items():
        lag_mask = coefficients["driver"].eq(short)
        lag_coefs = coefficients.loc[lag_mask].sort_values("lag_months")
        coef_sum_0_6 = lag_coefs["coef"].sum()
        coef_sum_0_3 = lag_coefs.loc[lag_coefs["lag_months"] <= 3, "coef"].sum()
        best = lag_correlations[lag_correlations["driver"] == short].copy()
        best = best.loc[best["correlation"].abs().idxmax()] if not best.empty else None
        cumulative_rows.append({
            "driver": short,
            "driver_label": meta["label"],
            "cumulative_beta_0_6": _safe_float(coef_sum_0_6),
            "cumulative_beta_0_3": _safe_float(coef_sum_0_3),
            "best_correlation_lag": int(best["lag_months"]) if best is not None else None,
            "best_correlation": _safe_float(best["correlation"]) if best is not None else None,
        })
    if "capacity_util_pct" in coefficients["term"].values:
        row = coefficients.loc[coefficients["term"] == "capacity_util_pct"].iloc[0]
        cumulative_rows.append({
            "driver": "capacity",
            "driver_label": "Capacity utilization",
            "cumulative_beta_0_6": _safe_float(row["coef"]),
            "cumulative_beta_0_3": _safe_float(row["coef"]),
            "best_correlation_lag": None,
            "best_correlation": None,
        })
    cumulative_betas = pd.DataFrame(cumulative_rows)

    residuals = np.asarray(model.resid)
    residual_quantiles = {
        "p10": _safe_float(np.quantile(residuals, 0.10)),
        "p50": _safe_float(np.quantile(residuals, 0.50)),
        "p90": _safe_float(np.quantile(residuals, 0.90)),
        "std": _safe_float(np.std(residuals, ddof=1)),
    }

    return ModelArtifacts(
        model=model,
        model_df=model_df,
        y_col="output_log_growth",
        x_cols=x_cols,
        coefficients=coefficients,
        lag_correlations=lag_correlations,
        cumulative_betas=cumulative_betas,
        residuals=residuals,
        residual_quantiles=residual_quantiles,
    )


# -------------------------
# Scenario engine
# -------------------------
def _scenario_log_impact(artifacts: ModelArtifacts, scenario: Mapping[str, Any]) -> float:
    impact = 0.0
    horizon = int(scenario.get("horizon_months", 3))

    for short in DRIVER_META:
        pct_value = float(scenario.get(f"{short}_pct", 0.0))
        shock_log = log_change_from_pct(pct_value)
        for lag in range(0, horizon):
            term = f"{short}_l{lag}"
            if term in artifacts.model.params.index:
                impact += float(artifacts.model.params[term]) * shock_log

    capacity_pts = float(scenario.get("capacity_pts", 0.0))
    if "capacity_util_pct" in artifacts.model.params.index:
        impact += float(artifacts.model.params["capacity_util_pct"]) * capacity_pts

    return float(impact)



def simulate_scenario(
    artifacts: ModelArtifacts,
    scenario_name: str,
    scenario: Mapping[str, Any],
    bootstrap_draws: int,
    rng: np.random.Generator,
) -> dict:
    deterministic_log_impact = _scenario_log_impact(artifacts, scenario)
    deterministic_pct = pct_from_log_change(deterministic_log_impact)

    residuals = artifacts.residuals
    sampled_residuals = rng.choice(residuals, size=bootstrap_draws, replace=True)
    simulated_log = deterministic_log_impact + sampled_residuals
    simulated_pct = np.exp(simulated_log) - 1.0

    return {
        "name": scenario_name,
        "description": scenario.get("description"),
        "inputs": {k: _clean_json(v) for k, v in scenario.items()},
        "deterministic_log_impact": _safe_float(deterministic_log_impact),
        "deterministic_pct_impact": _safe_float(deterministic_pct),
        "bootstrap_pct_impact": {
            "p10": _safe_float(np.quantile(simulated_pct, 0.10)),
            "p50": _safe_float(np.quantile(simulated_pct, 0.50)),
            "p90": _safe_float(np.quantile(simulated_pct, 0.90)),
        },
    }


# -------------------------
# Dashboard payload
# -------------------------
def build_payload(
    df: pd.DataFrame,
    artifacts: ModelArtifacts,
    metadata: Optional[dict],
    max_lag: int,
    bootstrap_draws: int,
) -> dict:
    rng = np.random.default_rng(RNG_SEED)
    scenarios = [
        simulate_scenario(artifacts, name, spec, bootstrap_draws=bootstrap_draws, rng=rng)
        for name, spec in DEFAULT_SCENARIOS.items()
    ]

    latest = df.dropna(subset=["chem_output_idx"]).sort_values("date").iloc[-1].to_dict()
    latest_date = pd.Timestamp(latest["date"]).strftime("%Y-%m-%d")

    levels = df[["date", "chem_output_idx", "chem_ppi_idx", "brent_usd_per_bbl", "ttf_gas_usd_per_mmbtu"]].copy()
    if "capacity_util_pct" in df.columns:
        levels["capacity_util_pct"] = df["capacity_util_pct"]

    growth = df[["date", "output_log_growth", "output_yoy_pct", "brent_yoy_pct", "gas_yoy_pct", "ppi_yoy_pct"]].copy()

    strongest_driver = None
    driver_rows = artifacts.cumulative_betas[artifacts.cumulative_betas["driver"].isin(list(DRIVER_META) + ["capacity"])].copy()
    if not driver_rows.empty:
        driver_rows["abs_beta"] = driver_rows["cumulative_beta_0_6"].abs()
        strongest_driver = driver_rows.sort_values("abs_beta", ascending=False).iloc[0]["driver_label"]

    model = artifacts.model
    coeff = artifacts.coefficients.copy()
    coeff["impact_of_10pct_move"] = coeff["coef"].where(coeff["driver"].isin(DRIVER_META), coeff["coef"]) \
        * coeff["driver"].map(lambda d: np.log1p(0.10) if d in DRIVER_META else 1.0)

    source_rows: List[dict] = []
    if metadata and isinstance(metadata.get("series"), dict):
        for key, item in metadata["series"].items():
            row = {"series": key}
            if isinstance(item, dict):
                row.update(item)
            source_rows.append(row)

    payload = {
        "meta": {
            "country": "Netherlands",
            "generated_from": "nl_chem_inputs_monthly.csv",
            "latest_observation_date": latest_date,
            "start_date": pd.Timestamp(df["date"].min()).strftime("%Y-%m-%d"),
            "end_date": pd.Timestamp(df["date"].max()).strftime("%Y-%m-%d"),
            "max_lag_months": max_lag,
            "bootstrap_draws": bootstrap_draws,
            "notes": [
                "The dependent variable is monthly log growth in the Netherlands chemicals production index.",
                "Driver shocks in the simulator are translated through the distributed-lag regression coefficients.",
                "Scenario ranges are residual bootstraps around the model point estimate, not structural forecasts.",
                "Capacity utilization is a manufacturing-wide proxy when available, not a chemicals-only measure.",
            ],
        },
        "headline": {
            "nobs": int(model.nobs),
            "r_squared": _safe_float(model.rsquared),
            "adj_r_squared": _safe_float(model.rsquared_adj),
            "aic": _safe_float(model.aic),
            "bic": _safe_float(model.bic),
            "durbin_watson": _safe_float(durbin_watson(model.resid)),
            "strongest_driver": strongest_driver,
        },
        "latest_values": {
            "date": latest_date,
            "chem_output_idx": _safe_float(latest.get("chem_output_idx")),
            "chem_ppi_idx": _safe_float(latest.get("chem_ppi_idx")),
            "brent_usd_per_bbl": _safe_float(latest.get("brent_usd_per_bbl")),
            "ttf_gas_usd_per_mmbtu": _safe_float(latest.get("ttf_gas_usd_per_mmbtu")),
            "capacity_util_pct": _safe_float(latest.get("capacity_util_pct")),
        },
        "series": {
            "levels": _clean_json(levels.assign(date=levels["date"].dt.strftime("%Y-%m-%d")).to_dict(orient="records")),
            "growth": _clean_json(growth.assign(date=growth["date"].dt.strftime("%Y-%m-%d")).to_dict(orient="records")),
        },
        "lag_correlations": _clean_json(artifacts.lag_correlations.to_dict(orient="records")),
        "coefficients": _clean_json(coeff.to_dict(orient="records")),
        "cumulative_betas": _clean_json(artifacts.cumulative_betas.to_dict(orient="records")),
        "driver_betas_by_lag": {
            short: _clean_json(
                artifacts.coefficients.loc[artifacts.coefficients["driver"] == short, ["lag_months", "coef", "p_value"]]
                .sort_values("lag_months")
                .to_dict(orient="records")
            )
            for short in DRIVER_META
        },
        "capacity_beta": _safe_float(model.params.get("capacity_util_pct")) if "capacity_util_pct" in model.params.index else None,
        "residual_quantiles": artifacts.residual_quantiles,
        "scenarios": _clean_json(scenarios),
        "sources": _clean_json(source_rows),
    }
    return payload


# -------------------------
# File output
# -------------------------
def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")



def render_dashboard(template_path: Path, payload: dict) -> str:
    if not template_path.exists():
        raise AnalysisError(f"Template not found: {template_path}")
    template = template_path.read_text(encoding="utf-8")
    payload_json = json.dumps(_clean_json(payload), ensure_ascii=False)
    payload_json = payload_json.replace("</", "<\\/")
    return template.replace("__ANALYSIS_PAYLOAD__", payload_json)



def main() -> None:
    parser = argparse.ArgumentParser(description="Build the NL chemicals analysis dashboard.")
    parser.add_argument("--input", type=Path, required=True, help="Merged monthly CSV produced by the pull script")
    parser.add_argument("--metadata", type=Path, default=None, help="Optional JSON metadata from the pull script")
    parser.add_argument("--template", type=Path, required=True, help="HTML dashboard template")
    parser.add_argument("--site-dir", type=Path, default=Path("site"), help="Output directory for the static site")
    parser.add_argument("--max-lag", type=int, default=DEFAULT_MAX_LAG, help="Maximum lag in months")
    parser.add_argument("--bootstrap-draws", type=int, default=DEFAULT_BOOTSTRAP_DRAWS, help="Residual bootstrap draws")
    args = parser.parse_args()

    df = load_input_data(args.input)
    prepared = prepare_model_dataframe(df, max_lag=args.max_lag)
    artifacts = fit_distributed_lag_model(prepared, max_lag=args.max_lag)
    metadata = maybe_read_json(args.metadata)
    payload = build_payload(prepared, artifacts, metadata, max_lag=args.max_lag, bootstrap_draws=args.bootstrap_draws)

    site_dir = args.site_dir
    site_dir.mkdir(parents=True, exist_ok=True)

    payload_path = site_dir / "analysis_payload.json"
    payload_path.write_text(json.dumps(_clean_json(payload), indent=2, ensure_ascii=False), encoding="utf-8")

    artifacts.lag_correlations.to_csv(site_dir / "lag_correlations.csv", index=False)
    artifacts.coefficients.to_csv(site_dir / "regression_coefficients.csv", index=False)
    write_text(site_dir / "model_summary.txt", artifacts.model.summary().as_text())
    write_text(site_dir / ".nojekyll", "")

    html = render_dashboard(args.template, payload)
    write_text(site_dir / "index.html", html)

    print(f"[OK] Wrote {site_dir / 'index.html'}")
    print(f"[OK] Wrote {site_dir / 'analysis_payload.json'}")
    print(f"[OK] Wrote {site_dir / 'lag_correlations.csv'}")
    print(f"[OK] Wrote {site_dir / 'regression_coefficients.csv'}")


if __name__ == "__main__":
    main()
