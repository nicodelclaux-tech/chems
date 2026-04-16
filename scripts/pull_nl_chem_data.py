#!/usr/bin/env python3
"""
Pull Netherlands chemicals / energy driver data from official free sources.

Core output series:
- Netherlands chemicals production index (Eurostat sts_inpr_m, NACE C20)
- Netherlands chemicals producer price index (Eurostat sts_inpp_m, NACE C20)
- Brent crude monthly price (EIA public history download)
- Europe natural gas monthly price (World Bank Pink Sheet; Europe series is TTF)
- Netherlands manufacturing capacity utilization (Eurostat teibs070, quarterly; optional proxy)

Dependencies:
    pip install pandas requests openpyxl lxml

Usage:
    python scripts/pull_nl_chem_data.py --outdir data --start 2015-01-01

This script only pulls, normalizes, and saves the data needed for the later
analysis. It does not run regressions.
"""
from __future__ import annotations

import argparse
import io
import itertools
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
import requests

EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EIA_BRENT_XLS = "https://www.eia.gov/dnav/pet/hist_xls/RBRTEm.xls"
EIA_BRENT_HTML = "https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?f=m&n=pet&s=rbrte"
WORLD_BANK_COMMODITY_PAGE = "https://www.worldbank.org/en/research/commodity-markets"
USER_AGENT = "Mozilla/5.0 (compatible; NL-Chem-Pull/2.0)"
TIMEOUT = 60

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


class SeriesSelectionError(RuntimeError):
    pass


def _get_json(url: str, params: Optional[dict] = None) -> dict:
    resp = session.get(url, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _get_text(url: str, params: Optional[dict] = None) -> str:
    resp = session.get(url, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.text


def _get_bytes(url: str, params: Optional[dict] = None) -> bytes:
    resp = session.get(url, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.content


# -------------------------
# JSON-stat -> DataFrame
# -------------------------
def _ordered_codes_and_labels(dim_meta: dict) -> Tuple[List[str], Dict[str, str]]:
    category = dim_meta["category"]
    index_meta = category.get("index", {})
    label_meta = category.get("label", {})

    if isinstance(index_meta, list):
        codes = list(index_meta)
    elif isinstance(index_meta, dict):
        codes = [k for k, _ in sorted(index_meta.items(), key=lambda kv: kv[1])]
    else:
        raise TypeError(f"Unsupported JSON-stat category index type: {type(index_meta)!r}")

    if isinstance(label_meta, dict):
        labels = {code: str(label_meta.get(code, code)) for code in codes}
    else:
        labels = {code: str(code) for code in codes}

    return codes, labels


def jsonstat_to_long(payload: dict) -> pd.DataFrame:
    ids: List[str] = payload["id"]
    size: List[int] = payload["size"]
    dims = payload["dimension"]

    codes_per_dim: List[List[str]] = []
    label_maps: Dict[str, Dict[str, str]] = {}
    for dim_id in ids:
        codes, labels = _ordered_codes_and_labels(dims[dim_id])
        codes_per_dim.append(codes)
        label_maps[dim_id] = labels

    combos = list(itertools.product(*codes_per_dim))
    values = payload.get("value", [])
    rows: List[dict] = []

    if isinstance(values, list):
        iterator = enumerate(values)
    elif isinstance(values, dict):
        iterator = ((int(k), v) for k, v in values.items())
    else:
        raise TypeError(f"Unsupported JSON-stat value type: {type(values)!r}")

    max_positions = 1
    for s in size:
        max_positions *= s

    for pos, val in iterator:
        if pos >= max_positions or val is None:
            continue
        combo = combos[pos]
        row = {dim_id: combo[i] for i, dim_id in enumerate(ids)}
        row["value"] = pd.to_numeric(val, errors="coerce")
        rows.append(row)

    df = pd.DataFrame(rows)
    for dim_id in ids:
        df[f"{dim_id}_label"] = df[dim_id].map(label_maps[dim_id])
    return df


# -------------------------
# Time parsing helpers
# -------------------------
def parse_period_to_month_end(value: object) -> pd.Timestamp:
    if pd.isna(value):
        return pd.NaT

    if isinstance(value, pd.Timestamp):
        return value.to_period("M").to_timestamp("M")

    s = str(value).strip()

    patterns = [
        (r"^\d{4}-\d{2}$", "%Y-%m"),
        (r"^\d{4}/\d{2}$", "%Y/%m"),
        (r"^[A-Za-z]{3}-\d{4}$", "%b-%Y"),
        (r"^[A-Za-z]+-\d{4}$", "%B-%Y"),
        (r"^\d{4}-[A-Za-z]{3}$", "%Y-%b"),
        (r"^\d{4}-[A-Za-z]+$", "%Y-%B"),
    ]
    for regex, fmt in patterns:
        if re.match(regex, s):
            return pd.to_datetime(s, format=fmt, errors="coerce").to_period("M").to_timestamp("M")

    if re.match(r"^\d{4}M\d{2}$", s):
        return pd.Period(s.replace("M", "-"), freq="M").to_timestamp("M")

    if re.match(r"^\d{4}-Q[1-4]$", s):
        return pd.Period(s.replace("-", ""), freq="Q").to_timestamp("Q")

    if re.match(r"^\d{4}Q[1-4]$", s):
        return pd.Period(s, freq="Q").to_timestamp("Q")

    dt = pd.to_datetime(s, errors="coerce")
    if pd.notna(dt):
        return dt.to_period("M").to_timestamp("M")

    return pd.NaT


def quarter_to_all_month_ends(qdate: pd.Timestamp) -> List[pd.Timestamp]:
    q = qdate.to_period("Q")
    return [m.to_timestamp("M") for m in pd.period_range(q.start_time.to_period("M"), q.end_time.to_period("M"), freq="M")]


# -------------------------
# Eurostat helpers
# -------------------------
def fetch_eurostat(dataset: str, filters: dict) -> pd.DataFrame:
    payload = _get_json(f"{EUROSTAT_BASE}/{dataset}", params={"lang": "EN", **filters})
    df = jsonstat_to_long(payload)
    if "time" not in df.columns:
        raise RuntimeError(f"Eurostat dataset {dataset} did not return a time dimension.")
    df["date"] = df["time"].map(parse_period_to_month_end)
    return df



def _pick_code_by_preference(
    df: pd.DataFrame,
    code_col: str,
    label_col: str,
    preferred_codes: Sequence[str] = (),
    label_patterns: Sequence[str] = (),
    allow_all_if_missing: bool = False,
) -> pd.DataFrame:
    if code_col not in df.columns:
        return df

    available = df[[code_col, label_col]].drop_duplicates()
    available_codes = set(available[code_col].astype(str))

    for code in preferred_codes:
        if code in available_codes:
            return df[df[code_col].astype(str) == code].copy()

    if label_patterns:
        labels = available[label_col].astype(str)
        for pat in label_patterns:
            mask = labels.str.contains(pat, case=False, regex=True, na=False)
            if mask.any():
                chosen_code = str(available.loc[mask, code_col].iloc[0])
                return df[df[code_col].astype(str) == chosen_code].copy()

    if allow_all_if_missing:
        return df

    options = available.to_dict(orient="records")
    raise SeriesSelectionError(
        f"Could not select {code_col}. Available options: {json.dumps(options, ensure_ascii=False)}"
    )



def _pick_index_unit(df: pd.DataFrame, code_col: str = "unit", label_col: str = "unit_label") -> pd.DataFrame:
    if code_col not in df.columns:
        return df

    available = df[[code_col, label_col]].drop_duplicates().copy()
    labels = available[label_col].astype(str)
    mask = labels.str.contains(r"index", case=False, regex=True, na=False) & ~labels.str.contains(
        r"%|percentage|change", case=False, regex=True, na=False
    )
    candidates = available.loc[mask].copy()
    if candidates.empty:
        raise SeriesSelectionError(
            f"Could not find an index-level unit. Available units: {available.to_dict(orient='records')}"
        )

    candidates["base_year"] = (
        candidates[label_col].astype(str).str.extract(r"((?:19|20)\d{2})", expand=False).astype(float)
    )
    candidates = candidates.sort_values(["base_year", label_col], ascending=[False, True], na_position="last")
    chosen = str(candidates.iloc[0][code_col])
    return df[df[code_col].astype(str) == chosen].copy()



def _finalize_single_series(
    df: pd.DataFrame,
    value_name: str,
    extra_columns_to_keep: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    out = df.copy()
    out = out[pd.notna(out["date"])].copy()
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["value", "date"])
    keep = ["date", "value"]
    if extra_columns_to_keep:
        keep.extend([c for c in extra_columns_to_keep if c in out.columns])
    out = out[keep].drop_duplicates(subset=["date"]).sort_values("date")
    out = out.rename(columns={"value": value_name})
    return out



def pull_nl_chem_output() -> pd.DataFrame:
    df = fetch_eurostat("sts_inpr_m", {"geo": "NL", "nace_r2": "C20", "freq": "M"})
    df = _pick_code_by_preference(
        df,
        "indic_bt",
        "indic_bt_label",
        preferred_codes=["PROD"],
        label_patterns=[r"production"],
    )
    df = _pick_code_by_preference(
        df,
        "s_adj",
        "s_adj_label",
        preferred_codes=["SCA", "CA", "NSA"],
        label_patterns=[r"seasonally and calendar adjusted", r"calendar adjusted", r"unadjusted"],
    )
    df = _pick_index_unit(df)
    return _finalize_single_series(df, "chem_output_idx")



def pull_nl_chem_ppi() -> pd.DataFrame:
    df = fetch_eurostat("sts_inpp_m", {"geo": "NL", "nace_r2": "C20", "freq": "M"})
    df = _pick_code_by_preference(
        df,
        "indic_bt",
        "indic_bt_label",
        preferred_codes=["PRC_PRR"],
        label_patterns=[r"producer price"],
    )
    df = _pick_code_by_preference(
        df,
        "s_adj",
        "s_adj_label",
        preferred_codes=["SCA", "CA", "NSA"],
        label_patterns=[r"seasonally and calendar adjusted", r"calendar adjusted", r"unadjusted"],
    )
    df = _pick_index_unit(df)
    return _finalize_single_series(df, "chem_ppi_idx")



def pull_nl_capacity_utilization() -> pd.DataFrame:
    df = fetch_eurostat("teibs070", {"geo": "NL", "freq": "Q"})
    indicator_cols = [c for c in ["indic_bs", "indic", "indicator"] if c in df.columns]
    for indicator_col in indicator_cols:
        df = _pick_code_by_preference(
            df,
            indicator_col,
            f"{indicator_col}_label",
            label_patterns=[r"capacity utilization"],
            allow_all_if_missing=True,
        )
    if "s_adj" in df.columns:
        df = _pick_code_by_preference(
            df,
            "s_adj",
            "s_adj_label",
            preferred_codes=["NSA"],
            label_patterns=[r"unadjusted"],
            allow_all_if_missing=True,
        )
    quarterly = _finalize_single_series(df, "capacity_util_pct")

    rows: List[dict] = []
    for _, row in quarterly.iterrows():
        qdate = pd.Timestamp(row["date"])
        for mdate in quarter_to_all_month_ends(qdate):
            rows.append({"date": mdate, "capacity_util_pct": row["capacity_util_pct"]})
    monthly = pd.DataFrame(rows).drop_duplicates(subset=["date"]).sort_values("date")
    return monthly


# -------------------------
# EIA Brent parser
# -------------------------
def _pull_brent_from_eia_xls() -> pd.DataFrame:
    data = _get_bytes(EIA_BRENT_XLS)
    xls = pd.ExcelFile(io.BytesIO(data))
    sheet_name = xls.sheet_names[0]
    df = pd.read_excel(xls, sheet_name=sheet_name)
    df.columns = [str(c).strip() for c in df.columns]

    year_col = df.columns[0]
    month_cols = [
        c for c in df.columns
        if c in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    ]
    if not month_cols:
        raise RuntimeError(f"Could not identify month columns in the EIA Excel file. Columns: {df.columns.tolist()}")

    long = df[[year_col] + month_cols].rename(columns={year_col: "year"}).melt(
        id_vars="year", var_name="month", value_name="brent_usd_per_bbl"
    )
    long["year"] = pd.to_numeric(long["year"], errors="coerce")
    long["brent_usd_per_bbl"] = pd.to_numeric(long["brent_usd_per_bbl"], errors="coerce")
    long = long.dropna(subset=["year", "brent_usd_per_bbl"])

    month_map = {m: i for i, m in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}
    long["month_num"] = long["month"].map(month_map)
    long["date"] = pd.to_datetime(
        dict(year=long["year"].astype(int), month=long["month_num"].astype(int), day=1),
        errors="coerce",
    ).dt.to_period("M").dt.to_timestamp("M")

    return long[["date", "brent_usd_per_bbl"]].sort_values("date").drop_duplicates(subset=["date"])



def _pull_brent_from_eia_html() -> pd.DataFrame:
    html = _get_text(EIA_BRENT_HTML)
    tables = pd.read_html(io.StringIO(html))
    if not tables:
        raise RuntimeError("Could not parse any tables from the EIA Brent page.")

    table = tables[0].copy()
    table.columns = [str(c).strip() for c in table.columns]
    year_col = table.columns[0]
    month_cols = [
        c for c in table.columns
        if c in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    ]
    if not month_cols:
        raise RuntimeError(f"Could not identify month columns on the EIA page. Columns: {table.columns.tolist()}")

    long = table[[year_col] + month_cols].rename(columns={year_col: "year"}).melt(
        id_vars="year", var_name="month", value_name="brent_usd_per_bbl"
    )
    long["year"] = pd.to_numeric(long["year"], errors="coerce")
    long["brent_usd_per_bbl"] = pd.to_numeric(long["brent_usd_per_bbl"], errors="coerce")
    long = long.dropna(subset=["year", "brent_usd_per_bbl"])

    month_map = {m: i for i, m in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}
    long["month_num"] = long["month"].map(month_map)
    long["date"] = pd.to_datetime(
        dict(year=long["year"].astype(int), month=long["month_num"].astype(int), day=1),
        errors="coerce",
    ).dt.to_period("M").dt.to_timestamp("M")

    return long[["date", "brent_usd_per_bbl"]].sort_values("date").drop_duplicates(subset=["date"])



def pull_brent_from_eia() -> Tuple[pd.DataFrame, str]:
    try:
        return _pull_brent_from_eia_xls(), EIA_BRENT_XLS
    except Exception:
        return _pull_brent_from_eia_html(), EIA_BRENT_HTML


# -------------------------
# World Bank TTF gas parser
# -------------------------
def _discover_world_bank_monthly_xlsx_url() -> str:
    html = _get_text(WORLD_BANK_COMMODITY_PAGE)

    patterns = [
        r'href=["\']([^"\']*CMO-Historical-Data-Monthly\.xlsx)["\']',
        r'(https://thedocs\.worldbank\.org[^"\']*CMO-Historical-Data-Monthly\.xlsx)',
        r"href=[\"']([^\"']*Monthly prices[^\"']*\.xls[x]?)[\"']",
    ]
    for pat in patterns:
        match = re.search(pat, html, flags=re.IGNORECASE)
        if match:
            url = match.group(1)
            if url.startswith("/"):
                return f"https://thedocs.worldbank.org{url}"
            if url.startswith("http"):
                return url
            return f"https://www.worldbank.org{url}"

    fallback_candidates = [
        "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-0350012021/related/CMO-Historical-Data-Monthly.xlsx",
        "https://www.worldbank.org/content/dam/Worldbank/GEP/GEPcommodities/CMO-Historical-Data-Monthly.xlsx",
    ]
    for url in fallback_candidates:
        try:
            _ = _get_bytes(url)
            return url
        except Exception:
            continue

    raise RuntimeError("Could not locate the World Bank monthly commodity workbook URL on the commodity markets page.")



def _coerce_header_to_month_end(value: object) -> pd.Timestamp:
    if isinstance(value, pd.Timestamp):
        return value.to_period("M").to_timestamp("M")
    return parse_period_to_month_end(value)



def _extract_wide_row_time_series(workbook_bytes: bytes, row_regex: str) -> Tuple[pd.DataFrame, str]:
    xls = pd.ExcelFile(io.BytesIO(workbook_bytes), engine="openpyxl")

    for sheet_name in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        raw_str = raw.astype(str)

        match_mask = raw_str.apply(lambda col: col.str.contains(row_regex, case=False, regex=True, na=False))
        match_positions = [(r, c) for c in match_mask.columns for r in match_mask.index[match_mask[c]]]
        if not match_positions:
            continue

        for row_idx, _ in match_positions:
            header_scores: List[Tuple[int, int, pd.Series]] = []
            for hdr_idx in range(max(0, row_idx - 12), row_idx):
                header_dates = raw.loc[hdr_idx].map(_coerce_header_to_month_end)
                score = int(header_dates.notna().sum())
                header_scores.append((score, hdr_idx, header_dates))
            if not header_scores:
                continue

            score, _, header_dates = max(header_scores, key=lambda x: x[0])
            if score < 12:
                continue

            valid_cols = header_dates[header_dates.notna()].index.tolist()
            values = pd.to_numeric(raw.loc[row_idx, valid_cols], errors="coerce")
            out = pd.DataFrame({
                "date": pd.to_datetime(header_dates.loc[valid_cols]).to_period("M").to_timestamp("M"),
                "ttf_gas_usd_per_mmbtu": values.values,
            })
            out = out.dropna(subset=["date", "ttf_gas_usd_per_mmbtu"])
            out = out.sort_values("date").drop_duplicates(subset=["date"])
            if len(out) >= 12:
                return out, sheet_name

    raise RuntimeError("Could not locate a wide monthly row for 'Natural gas, Europe' in the World Bank workbook.")



def pull_ttf_gas_from_world_bank() -> Tuple[pd.DataFrame, str, str]:
    xlsx_url = _discover_world_bank_monthly_xlsx_url()
    workbook_bytes = _get_bytes(xlsx_url)
    series, sheet_name = _extract_wide_row_time_series(workbook_bytes, row_regex=r"natural\s+gas.*europe")
    return series, xlsx_url, sheet_name


# -------------------------
# Merge + write
# -------------------------
def merge_monthly(series_frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    merged: Optional[pd.DataFrame] = None
    for df in series_frames:
        if merged is None:
            merged = df.copy()
        else:
            merged = merged.merge(df, on="date", how="outer")
    assert merged is not None
    return merged.sort_values("date").reset_index(drop=True)



def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)



def main() -> None:
    parser = argparse.ArgumentParser(description="Pull Netherlands chemicals / energy inputs from official free sources.")
    parser.add_argument("--outdir", type=Path, default=Path("data"), help="Output directory")
    parser.add_argument("--start", type=str, default="2015-01-01", help="Inclusive start date, e.g. 2015-01-01")
    args = parser.parse_args()

    start_date = pd.to_datetime(args.start).to_period("M").to_timestamp("M")
    outdir: Path = args.outdir
    rawdir = outdir / "raw"
    rawdir.mkdir(parents=True, exist_ok=True)

    chem_output = pull_nl_chem_output()
    chem_ppi = pull_nl_chem_ppi()
    brent, brent_url = pull_brent_from_eia()
    ttf_gas, wb_xlsx_url, wb_sheet_name = pull_ttf_gas_from_world_bank()

    try:
        capacity = pull_nl_capacity_utilization()
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] Could not pull NL manufacturing capacity utilization: {exc}")
        capacity = pd.DataFrame(columns=["date", "capacity_util_pct"])

    frames = [chem_output, chem_ppi, brent, ttf_gas]
    if not capacity.empty:
        frames.append(capacity)

    merged = merge_monthly(frames)
    merged = merged[merged["date"] >= start_date].copy()

    write_csv(chem_output, rawdir / "nl_chem_output.csv")
    write_csv(chem_ppi, rawdir / "nl_chem_ppi.csv")
    write_csv(brent, rawdir / "brent_monthly.csv")
    write_csv(ttf_gas, rawdir / "ttf_gas_monthly.csv")
    if not capacity.empty:
        write_csv(capacity, rawdir / "nl_capacity_util_monthly.csv")

    write_csv(merged, outdir / "nl_chem_inputs_monthly.csv")

    metadata = {
        "pulled_at_utc": datetime.now(timezone.utc).isoformat(),
        "country": "NL",
        "series": {
            "chem_output": {
                "source": "Eurostat",
                "dataset": "sts_inpr_m",
                "filters": {"geo": "NL", "nace_r2": "C20", "freq": "M"},
            },
            "chem_ppi": {
                "source": "Eurostat",
                "dataset": "sts_inpp_m",
                "filters": {"geo": "NL", "nace_r2": "C20", "freq": "M"},
            },
            "capacity_util": {
                "source": "Eurostat",
                "dataset": "teibs070",
                "filters": {"geo": "NL", "freq": "Q"},
                "note": "Manufacturing-wide proxy, expanded from quarterly to monthly by repeating each quarter's value across its three months.",
            },
            "brent": {
                "source": "U.S. Energy Information Administration",
                "url": brent_url,
            },
            "ttf_gas": {
                "source": "World Bank Pink Sheet",
                "commodity_page": WORLD_BANK_COMMODITY_PAGE,
                "resolved_xlsx_url": wb_xlsx_url,
                "sheet_name": wb_sheet_name,
                "note": "The World Bank defines 'Natural gas, Europe' as Netherlands Title Transfer Facility (TTF).",
            },
        },
        "output_files": {
            "merged": str((outdir / "nl_chem_inputs_monthly.csv").as_posix()),
            "raw_dir": str(rawdir.as_posix()),
        },
    }
    with open(outdir / "pull_metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(f"[OK] Wrote {outdir / 'nl_chem_inputs_monthly.csv'}")
    print(f"[OK] Wrote {outdir / 'pull_metadata.json'}")


if __name__ == "__main__":
    main()
