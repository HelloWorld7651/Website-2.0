import pandas as pd
import sys

# Usage: python count_citations.py input.tsv output.tsv
inp = sys.argv[1]
out = sys.argv[2] if len(sys.argv) > 2 else "citation_counts.tsv"

# Read TSV; keep as strings to preserve any leading zeros
df = pd.read_csv(inp, sep="\t", dtype=str)

# Adjust these names if yours differ:
# Expect columns like: patent_id, citation_patent_id (or cited_patent_id)
if "citation_patent_id" not in df.columns and "cited_patent_id" in df.columns:
    df = df.rename(columns={"cited_patent_id": "citation_patent_id"})

required = {"patent_id", "citation_patent_id"}
missing = required - set(df.columns)
if missing:
    raise SystemExit(f"Missing required columns: {missing}. Found: {list(df.columns)}")

# Clean whitespace & drop rows with no citation
df["patent_id"] = df["patent_id"].astype(str).str.strip()
df["citation_patent_id"] = df["citation_patent_id"].astype(str).str.strip()
df = df[df["citation_patent_id"] != ""]

# Compute counts (total & unique)
grp = df.groupby("patent_id", dropna=False)
out_df = grp.agg(
citation_total=("citation_patent_id", "size"),
citation_unique=("citation_patent_id", pd.Series.nunique),
).reset_index()

# Write TSV
out_df.to_csv(out, sep="\t", index=False)

# Optional: print a quick check for 10000000 if present
row = out_df.loc[out_df["patent_id"] == "10000000"]
if not row.empty:
    tot = int(row["citation_total"].iloc[0])
uniq = int(row["citation_unique"].iloc[0])
print(f"patent_id 10000000 → {tot} citations ({uniq} unique)")
else:
    print("patent_id 10000000 not found.")
    print(f"Wrote: {out}")