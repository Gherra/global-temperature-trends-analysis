# src/processing/city_station_matcher.py


from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

# Configuration
WEEKLY_IN = Path("../../data/processed/weather_weekly_clean.parquet")
CITIES_IN = Path("../../data/processed/cities_clean_enhanced.parquet")
MAP_OUT = Path(
    "../../data/processed/station_to_city_map_calculated_with_the_week_dates_data.parquet"
)
ENRICHED_DIR = Path("../../data/processed/weather_weekly_with_city_dated")

MAX_KM = 100.0
REVIEW_KM = 30.0
MIN_YEARS = 10
MIN_PAIR_FRAC = 0.90

EARTH_RADIUS_KM = 6371.0088


def to_radians(df_lat_lon):

    return np.deg2rad(
        df_lat_lon[["latitude", "longitude"]].to_numpy(dtype=float, copy=False)
    )


def build_station_table(weekly):

    has_pair = weekly[["tmax_c", "tmin_c"]].notna().all(axis=1)

    iso_years = pd.to_datetime(weekly["week_start"]).dt.isocalendar().year

    station_tab = (
        weekly.assign(_pair=has_pair, _iso_year=iso_years)
        .groupby("station", as_index=False)
        .agg(
            {
                "name": "first",
                "latitude": "median",
                "longitude": "median",
                "elevation": "median",
                "_iso_year": "nunique",
                "_pair": "mean",
                "week": "size",
                "week_start": ["min", "max"],
            }
        )
    )

    station_tab.columns = [
        "station",
        "station_name",
        "latitude",
        "longitude",
        "elevation",
        "years_count",
        "pair_fraction",
        "total_weeks",
        "first_week",
        "last_week",
    ]

    good = station_tab[
        (station_tab["years_count"] >= MIN_YEARS)
        & (station_tab["pair_fraction"] >= MIN_PAIR_FRAC)
    ].copy()

    print("Total stations in weekly file:", station_tab.shape[0])
    print(
        "Kept (>=",
        MIN_YEARS,
        "years AND pair_fraction >=",
        MIN_PAIR_FRAC,
        "):",
        good.shape[0],
    )
    if good.shape[0] < station_tab.shape[0]:
        print(
            "dropped:",
            station_tab.shape[0] - good.shape[0],
            "(insufficient coverage/completeness)",
        )

    return good


def match_stations_to_cities(stations, cities):

    # adapted form https://stackoverflow.com/a/58942469

    cities_rad = to_radians(
        cities.rename(columns={"latitude": "latitude", "longitude": "longitude"})
    )
    tree = BallTree(cities_rad, metric="haversine")

    stations_rad = to_radians(stations)
    dist_rad, idx = tree.query(stations_rad, k=3)
    dist_km = dist_rad * EARTH_RADIUS_KM

    rows = []
    for i in range(stations.shape[0]):
        st = stations.iloc[i]

        picked = None
        for j, ci in enumerate(idx[i]):
            if dist_km[i, j] <= MAX_KM:
                c = cities.iloc[ci]
                picked = (c, dist_km[i, j])
                break

        if picked is None:

            rows.append(
                {
                    "station": st["station"],
                    "city_name": None,
                    "country_code": None,
                    "city_lat": np.nan,
                    "city_lon": np.nan,
                    "distance_km": np.nan,
                    "review_flag": True,
                }
            )
        else:
            c, dk = picked
            rows.append(
                {
                    "station": st["station"],
                    "city_name": c["name"],
                    "country_code": c["country_code"],
                    "city_lat": float(c["latitude"]),
                    "city_lon": float(c["longitude"]),
                    "distance_km": float(dk),
                    "review_flag": bool(dk > REVIEW_KM),
                }
            )

    mapping = pd.DataFrame(rows)

    # Print summary
    matched = mapping["city_name"].notna().sum()
    print()

    print("Stations input:", stations.shape[0])
    print("Matched within", MAX_KM, "km:", matched)
    print("Unmatched:", stations.shape[0] - matched)
    if mapping["review_flag"].any():
        print(
            "Matches >",
            REVIEW_KM,
            "km:",
            int(mapping["review_flag"].sum()),
            "(worth reviewing)",
        )

    return mapping


def write_mapping(mapping, path):

    path.parent.mkdir(parents=True, exist_ok=True)
    mapping.to_parquet(path, index=False)
    print()
    print("Saved station-city map:", path)


def write_enriched_weekly(weekly, mapping, out_dir):

    out_dir.mkdir(parents=True, exist_ok=True)

    enriched = weekly.merge(mapping, on="station", how="left")
    enriched = enriched.rename(
        columns={"latitude": "station_lat", "longitude": "station_lon"}
    )

    cols_order = [
        "station",
        "station_lat",
        "station_lon",
        "elevation",
        "name",
        "year",
        "week",
        "week_start",
        "tmax_c",
        "tmin_c",
        "prcp_mm",
        "city_name",
        "country_code",
        "city_lat",
        "city_lon",
        "distance_km",
    ]
    keep = [c for c in cols_order if c in enriched.columns]
    enriched = enriched[keep].copy()

    years = pd.to_datetime(enriched["week_start"]).dt.isocalendar().year.astype(int)
    enriched = enriched.assign(year=years)

    for yr in sorted(enriched["year"].unique()):
        sub = enriched[enriched["year"] == yr]
        year_dir = out_dir / str(yr)
        year_dir.mkdir(parents=True, exist_ok=True)
        sub.to_parquet(year_dir / "part-0.parquet", index=False)

    print("Saved enriched weekly dataset to:", out_dir, "(one folder per year)")


def main():

    # Load input data
    weekly = pd.read_parquet(WEEKLY_IN)
    cities = pd.read_parquet(CITIES_IN)

    print("Weekly rows:", len(weekly), "  stations:", weekly["station"].nunique())
    print("Cities rows:", len(cities), "  countries:", cities["country_code"].nunique())

    stations = build_station_table(weekly)

    mapping = match_stations_to_cities(stations, cities)

    write_mapping(mapping, MAP_OUT)
    write_enriched_weekly(weekly, mapping, ENRICHED_DIR)

    print()
    print("Sample mapping:")
    print(mapping.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
