#!/usr/bin/env python3
import argparse, re, random
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

BASE_COLS = ["Ref #","Hits","Search Query","DBs","Default Operator","Plurals","British Equivalents","Time Stamp"]

# ---------------- utilities ----------------
def norm_name(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', '', (s or '').strip().lower())

def read_all_sheets_with_names(xlsx_path: Path) -> pd.DataFrame:
    """
    Read all sheets; keep BASE_COLS; add sheet_name + row_in_sheet.
    """
    book = pd.read_excel(xlsx_path, sheet_name=None, dtype=str)
    frames = []
    base_norm = [norm_name(c) for c in BASE_COLS]
    for sheet, df in book.items():
        if df is None or df.empty: 
            continue
        # map columns to canonical names when possible
        colmap = {}
        for c in df.columns:
            nc = norm_name(c)
            if nc in base_norm:
                target = BASE_COLS[base_norm.index(nc)]
            else:
                target = None
                for bn, base in zip(base_norm, BASE_COLS):
                    if bn and bn in nc:
                        target = base; break
            colmap[c] = target if target else c
        df = df.rename(columns=colmap)
        for bc in BASE_COLS:
            if bc not in df.columns:
                df[bc] = np.nan
        df = df[BASE_COLS].copy()
        df["sheet_name"] = sheet
        df["row_in_sheet"] = np.arange(1, len(df)+1)
        frames.append(df)
    if not frames:
        raise ValueError("No usable sheets/columns found.")
    merged = pd.concat(frames, ignore_index=True)
    # clean common stringy NA
    for c in merged.columns:
        merged[c] = merged[c].astype(str).str.strip()
        merged.loc[merged[c].isin(["nan","None","NaT"]), c] = ""
    return merged

def coerce_datetime(series: pd.Series) -> pd.Series:
    """
    Parse timestamps; drop a trailing bare TZ-ish token (e.g., 'SS', 'PST').
    """
    def parse_one(s):
        s = str(s).strip()
        if not s or s.lower() in {"nan", "none"}:
            return pd.NaT
        s = re.sub(r"\s+[A-Za-z]{1,4}$", "", s)
        return pd.to_datetime(s, errors="coerce")
    return series.apply(parse_one)

def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def fit_lognormal_params_pos(x: np.ndarray):
    """
    Fit lognormal μ,σ for x>0. Returns (mu, sigma) in log-space or None.
    """
    x = np.asarray(x)
    x = x[np.isfinite(x) & (x > 0)]
    if x.size < 20:
        return None
    lx = np.log(x + 1e-9)
    return float(lx.mean()), float(lx.std() + 1e-9)

def sample_session_size_model(real: pd.DataFrame):
    sizes = real.groupby("sheet_name").size().values
    if sizes.size == 0:
        return np.array([20]), np.array([1.0])
    vals, counts = np.unique(sizes, return_counts=True)
    p = counts / counts.sum()
    return vals, p

def start_time_model(real: pd.DataFrame):
    dt = coerce_datetime(real["Time Stamp"])
    first_by_sheet = real.assign(_dt=dt).groupby("sheet_name")["_dt"].min().dropna()
    if first_by_sheet.empty:
        dow_w = np.array([1,1,1,1,1,0.7,0.5], float); dow_w /= dow_w.sum()
        hour_w = np.ones(24, float); hour_w[0:7]*=0.4; hour_w[18:24]*=0.6; hour_w/=hour_w.sum()
    else:
        dow = first_by_sheet.dt.dayofweek.values
        hour = first_by_sheet.dt.hour.values
        dow_w = (np.bincount(dow, minlength=7).astype(float)+1e-3); dow_w/=dow_w.sum()
        hour_w = (np.bincount(hour, minlength=24).astype(float)+1e-3); hour_w/=hour_w.sum()
    return dow_w, hour_w

def gaps_from_real(real: pd.DataFrame):
    """
    Collect positive consecutive gaps (seconds) within each sheet.
    """
    dt = coerce_datetime(real["Time Stamp"])
    gaps = []

    df = real.assign(_dt=dt)
    for _, g in df.groupby("sheet_name", sort=False):
        t = g.sort_values("row_in_sheet")["_dt"]
        s = t.diff().dt.total_seconds()
        s = s[s > 0]
        if not s.empty:
            gaps.append(s.values.astype(float))

    if gaps:
        return np.concatenate(gaps)
    return np.array([20.0, 30.0, 15.0], float)

def align_hits_gaps(real: pd.DataFrame):
    """
    Build aligned arrays (hits_i, gap_i) where gap_i is the time (sec) between
    row i-1 and row i, aligned to row i's Hits, within each sheet.
    """
    dt = coerce_datetime(real["Time Stamp"])
    hits = coerce_numeric(real["Hits"])
    out_h, out_g = [], []

    df = real.assign(_dt=dt, _hits=hits)
    for _, g in df.groupby("sheet_name", sort=False):
        g = g.sort_values("row_in_sheet")
        t = g["_dt"]
        h = g["_hits"]

        gaps_sec = t.diff().dt.total_seconds() # gap aligned to the current (later) row
        mask = gaps_sec.notna() & (gaps_sec > 0) & h.notna()

        if mask.any():
            out_h.extend(h[mask].astype(float).to_list())
            out_g.extend(gaps_sec[mask].astype(float).to_list())

    if not out_h:
        return np.array([]), np.array([])
    return np.array(out_h, float), np.array(out_g, float)


def build_joint_hits_time_model(real: pd.DataFrame):
    """
    Fit lognormal marginals for Hits and gaps & a Gaussian-copula correlation (log-space).
    """
    hits_all = coerce_numeric(real["Hits"]).dropna().to_numpy()
    hits_all = hits_all[hits_all >= 0]
    mu_h, sg_h = fit_lognormal_params_pos(hits_all + 1.0) or (np.log(5.0), 1.0)

    gaps_all = gaps_from_real(real)
    mu_g, sg_g = fit_lognormal_params_pos(gaps_all) or (np.log(20.0), 0.7)

    h_al, g_al = align_hits_gaps(real)
    if h_al.size >= 20:
        X = np.log(h_al + 1.0); Y = np.log(g_al + 1e-9)
        rho = float(np.corrcoef(X, Y)[0,1])
        rho = np.clip(rho, -0.95, 0.95)
    else:
        rho = 0.0
    return (mu_h, sg_h), (mu_g, sg_g), rho

def sample_correlated_lognormals(mu1, s1, mu2, s2, rho, n):
    cov = np.array([[1.0, rho],[rho, 1.0]], float)
    z = np.random.multivariate_normal([0,0], cov, size=n)
    x1 = np.exp(mu1 + s1*z[:,0])
    x2 = np.exp(mu2 + s2*z[:,1])
    return x1, x2

def sample_start_time(dow_w, hour_w):
    base = datetime(2025,1,1) + timedelta(days=int(np.random.randint(0,180)))
    d = int(np.random.choice(range(7), p=dow_w))
    h = int(np.random.choice(range(24), p=hour_w))
    dt0 = base.replace(hour=h, minute=int(np.random.randint(0,60)), second=int(np.random.randint(0,60)))
    while dt0.weekday() != d:
        dt0 += timedelta(days=1)
    return dt0

# --------------- risk (Hits + time only) ---------------
def robust_z(x):
    x = np.asarray(x, float)
    med = np.nanmedian(x)
    mad = np.nanmedian(np.abs(x - med)) + 1e-9
    return 0.6745 * (x - med) / mad

def risk_from_hits_time(hits, gaps_sec):
    h = np.log1p(np.asarray(hits, float))
    g = np.asarray(gaps_sec, float)
    zh = robust_z(h)
    zg = robust_z(g)
    mismatch = zg - zh
    s = 0.7*mismatch + 0.2*zg - 0.1*zh
    risk = 1.0 / (1.0 + np.exp(-s))
    label = np.full_like(risk, 'low', dtype=object)
    label[(mismatch > 0.3) | (zg > 0.5) | (zh < -0.5)] = 'medium'
    label[(mismatch > 1.0) | (zg > 1.25) | (zh < -1.25)] = 'high'
    return np.round(risk, 4), label

# --------------- query corpus (optional) ---------------
BOOL_TOKS = [" AND ", " OR ", " WITH ", " NEAR ", " NOT "]
def build_query_corpus(real: pd.DataFrame):
    q = real["Search Query"].astype(str).str.strip()
    q = q[(q != "") & (q.str.lower() != "nan") & (q.str.lower() != "none")].tolist()
    if not q:
        q = ["(object$1 AND object$2) WITH (EMF NEAR (skin OR tissue))"]
    return q

def perturb_query(q: str, p_digits=0.35, p_ws=0.4, p_bool=0.15) -> str:
    if not q: return q
    s = " " + q.strip() + " "
    def tweak_digits(m):
        digits = list(m.group(0))
        idx = random.randrange(len(digits))
        d = digits[idx]
        if d.isdigit():
            digits[idx] = str((int(d)+random.choice([-1,1])) % 10)
        return "".join(digits)
    if random.random() < p_digits:
        s = re.sub(r"(?<![A-Za-z])\d{2,}(?![A-Za-z])", tweak_digits, s)
    if random.random() < p_ws:
        s = re.sub(r"\s+\)", ")", s); s = re.sub(r"\(\s+", "(", s)
    if random.random() < 0.25:
        s = re.sub(r"\s{2,}", " ", s)
    if random.random() < p_bool:
        tok = random.choice(BOOL_TOKS)
        s = s.replace(tok, random.choice(BOOL_TOKS))
    return s.strip()

# --------------- main generator ---------------
def synth_hits_time_sessions(xlsx_in: Path, out_path: Path, total_rows: int,
                             time_output: str, ts_format: str,
                             risk_flag: str, query_flag: str, seed: int):
    np.random.seed(seed); random.seed(seed)
    real = read_all_sheets_with_names(xlsx_in)

    # models
    size_vals, size_p = sample_session_size_model(real)
    dow_w, hour_w = start_time_model(real)
    (mu_h, sg_h), (mu_g, sg_g), rho = build_joint_hits_time_model(real)
    query_pool = build_query_corpus(real)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    first = True
    written = 0
    sess_id = 1

    while written < total_rows:
        # session size
        m = int(np.random.choice(size_vals, p=size_p))
        m = min(m, total_rows - written)
        session_id = f"S{sess_id:07d}"
        sess_id += 1

        # correlated Hits & time gaps
        hits_f, gaps_f = sample_correlated_lognormals(mu_h, sg_h, mu_g, sg_g, rho, m)
        hits = np.maximum(0, np.round(hits_f - 1.0, 0).astype(int))  # undo +1 used for log
        gaps_sec = np.maximum(1.0, gaps_f)

        # build time axis
        start = sample_start_time(dow_w, hour_w)
        t = [start]
        for g in gaps_sec[:-1]:
            t.append(t[-1] + timedelta(seconds=float(g)))
        t = pd.to_datetime(pd.Series(t), errors="coerce")
        ts_str = t.dt.strftime(ts_format).fillna("")
        minutes = np.round(gaps_sec / 60.0, 2)

        # assemble row dict
        data = {
            "session_id": session_id,
            "session_index": np.arange(1, m+1, dtype=int),
            "Ref #": [f"L{i}" for i in range(1, m+1)],
            "Hits": hits,
        }
        if time_output in ("timestamp","both"):
            data["Time Stamp"] = ts_str
        if time_output in ("minutes","both"):
            data["Search Minutes"] = minutes

        if query_flag == "on":
            # sample & perturb queries
            qs = []
            for _ in range(m):
                base = random.choice(query_pool)
                qs.append(perturb_query(base) if random.random()<0.30 else base)
            data["Search Query"] = qs

        if risk_flag == "on":
            risk_score, risk_label = risk_from_hits_time(hits, gaps_sec)
            data["risk_score"] = risk_score
            data["risk_label"] = risk_label

        df = pd.DataFrame(data)
        df.to_csv(out_path, index=False, mode="w" if first else "a", header=first)
        first = False
        written += m

    print(f"Done. Wrote {written} rows → {out_path}")

# ---------------- CLI ----------------
def main():
    ap = argparse.ArgumentParser(description="Synthesize sessioned search log data (Hits + Time), optionally risk and query text.")
    ap.add_argument("--in", dest="infile", required=True, type=Path, help="Input Excel (.xlsx); each sheet = one session.")
    ap.add_argument("--out", dest="outfile", required=True, type=Path, help="Output CSV path.")
    ap.add_argument("--rows", type=int, default=500000, help="Total synthetic rows to generate across many sessions.")
    ap.add_argument("--time-output", choices=["timestamp","minutes","both"], default="timestamp",
                    help="Emit 'Time Stamp' like your sheets, 'Search Minutes', or both.")
    ap.add_argument(
        "--ts-format",
        default="%Y/%m/%d %I:%M %p",
        help="strftime for 'Time Stamp' (e.g., '%%m/%%d/%%Y %%I:%%M %%p')."
    )
    ap.add_argument("--risk", choices=["on","off"], default="on",
                    help="Include risk_score/risk_label based only on Hits & time.")
    ap.add_argument("--query", choices=["on","off"], default="off",
                    help="Include a synthetic 'Search Query' column sampled/perturbed from the workbook.")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    synth_hits_time_sessions(
        xlsx_in=args.infile,
        out_path=args.outfile,
        total_rows=args.rows,
        time_output=args.time_output,
        ts_format=args.ts_format,
        risk_flag=args.risk,
        query_flag=args.query,
        seed=args.seed
    )

if __name__ == "__main__":
    main()


'''
# run – keep timestamp like your sheets, include risk, NO query text
python output.py \
  --in "/Users/helloworld/Downloads/pe2e_all.xlsx" \
  --out "/Users/helloworld/Downloads/synth_500k.csv" \
  --rows 1000 \
  --time-output timestamp \
  --risk on \
  --query off
'''