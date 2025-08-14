# cities_txt_to_csv_enhanced.py
# Clean up the GeoNames cities1000.txt file and save an enhanced CSV version

import pandas as pd
import unicodedata
from pathlib import Path

RAW = "../../data/interim/cities1000.txt"
OUT = "../../data/processed/cities_clean_enhanced_csv_version.csv"

COLS = [
    "geonameid",
    "name",
    "asciiname",
    "alternatenames",
    "latitude",
    "longitude",
    "feature_class",
    "feature_code",
    "country_code",
    "cc2",
    "admin1_code",
    "admin2_code",
    "admin3_code",
    "admin4_code",
    "population",
    "elevation",
    "dem",
    "timezone",
    "modification_date",
]

# Keep only real populated places
PLACE_CODES = {"PPL", "PPLA", "PPLA2", "PPLA3", "PPLA4", "PPLC"}


def norm(s: str) -> str:
    """Normalize to lowercase ASCII."""
    if pd.isna(s):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    return s.lower().strip()


def top_alts(alts: str, limit=20):
    """Return a sorted list of top alternate names (normalized)."""
    if pd.isna(alts):
        return []
    bag = {norm(t) for t in alts.split(",") if len(t) > 2}
    return sorted(list(bag))[:limit]


def main():
    print("=" * 50)
    print("CITIES TXT → ENHANCED CSV")
    print("=" * 50)
    print(f"Reading: {RAW}")

    # Read raw file
    df = pd.read_csv(RAW, sep="\t", names=COLS, usecols=COLS, low_memory=False)

    # Filter to populated places only
    df = df[(df["feature_class"] == "P") & (df["feature_code"].isin(PLACE_CODES))]

    # Drop rows with missing essential data
    df = df.dropna(subset=["name", "latitude", "longitude", "country_code"])

    # Convert population to int
    df["population"] = (
        pd.to_numeric(df["population"], errors="coerce").fillna(0).astype("int64")
    )

    # Add normalized columns
    df["city_norm"] = df["name"].apply(norm)
    df["alt_names_norm"] = df["alternatenames"].apply(top_alts)

    # De-dup within (country_code, admin1_code, city_norm) by population
    df = df.sort_values("population", ascending=False).drop_duplicates(
        subset=["country_code", "admin1_code", "city_norm"], keep="first"
    )

    # Keep only the columns we want in the CSV
    out_cols = [
        "geonameid",
        "name",
        "asciiname",
        "city_norm",
        "alt_names_norm",
        "latitude",
        "longitude",
        "country_code",
        "admin1_code",
        "population",
        "feature_code",
        "timezone",
    ]
    out_df = df[out_cols].reset_index(drop=True)

    # Flatten alt_names_norm for CSV
    out_df["alt_names_norm"] = out_df["alt_names_norm"].apply(
        lambda xs: ";".join(xs) if isinstance(xs, (list, tuple)) else ""
    )

    # Save as CSV
    Path(OUT).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT, index=False)

    print(f"✅ Saved {len(out_df):,} rows to {OUT}")


if __name__ == "__main__":
    main()
