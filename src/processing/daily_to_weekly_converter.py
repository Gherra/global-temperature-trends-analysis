# src/processing/daily_to_weekly_converter.py

# Daily to Weekly Weather Data Converter
# Reads daily data and creates weekly averages


from pathlib import Path
import sys
import gc
import pandas as pd
import numpy as np

# colums i need from daily data
REQUIRED_DAILY_COLS = [
    "station",
    "date",
    "tmax_c",
    "tmin_c",
    "prcp_mm",
    "latitude",
    "longitude",
    "elevation",
    "name",
]


def list_daily_files(input_root):

    files = list(input_root.glob("year=*/part-*.parquet"))
    # getting the year form the file name and sorting by it
    files = sorted(files, key=lambda p: int(p.parent.name.split("=")[1]))
    return files


def convert_daily_to_weekly(daily_df):

    print("Converting to weekly data:")
    daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")
    before_count = len(daily_df)
    daily_df = daily_df.dropna(subset=["date"]).copy()
    dropped = before_count - len(daily_df)
    if dropped > 0:
        print("Dropped", dropped, "rows with bad dates")

    iso_data = daily_df["date"].dt.isocalendar()
    daily_df["year"] = iso_data.year.astype("int16")
    daily_df["week"] = iso_data.week.astype("int16")

    daily_df["week_start"] = pd.to_datetime(
        daily_df["year"].astype(str) + "-W" + daily_df["week"].astype(str) + "-1",
        format="%G-W%V-%u",
        errors="coerce",
    )

    daily_df["prcp_mm"] = daily_df["prcp_mm"].fillna(0)

    group_cols = [
        "station",
        "name",
        "latitude",
        "longitude",
        "elevation",
        "year",
        "week",
        "week_start",
    ]

    weekly = daily_df.groupby(group_cols, as_index=False).agg(
        {"tmax_c": "mean", "tmin_c": "mean", "prcp_mm": "sum"}
    )

    weekly = weekly.sort_values(["station", "year", "week"]).reset_index(drop=True)

    print("Generated", len(weekly), "weekly records")
    return weekly


def load_and_process_daily_data(input_root):

    files = list_daily_files(input_root)

    print("found", len(files), "yearly files to process")

    weekly_parts = []
    for i, path in enumerate(files, start=1):
        year = path.parent.name.split("=")[1]
        print("prorocessing year", year, "(" + str(i) + "/" + str(len(files)) + ")")

        df = pd.read_parquet(path, columns=REQUIRED_DAILY_COLS)
        print("Loaded", len(df), "daily records")

        missing = [c for c in REQUIRED_DAILY_COLS if c not in df.columns]
        if missing:
            raise KeyError("Missing columns in " + str(path) + ": " + str(missing))

        weekly = convert_daily_to_weekly(df)
        weekly_parts.append(weekly)

        del df, weekly
        gc.collect()

    print("Combining all yearsnow : ")
    combined = pd.concat(weekly_parts, ignore_index=True)
    print("Total weekly recrods:", len(combined))
    return combined


def validate_weekly_data(weekly_df):

    print("Validating weekly data:")

    required = [
        "station",
        "name",
        "latitude",
        "longitude",
        "elevation",
        "year",
        "week",
        "week_start",
        "tmax_c",
        "tmin_c",
        "prcp_mm",
    ]
    missing = [c for c in required if c not in weekly_df.columns]
    if missing:
        print("WARNING: missing columns:", missing)

    print("Year range:", weekly_df["year"].min(), "to", weekly_df["year"].max())
    print("Week range:", weekly_df["week"].min(), "to", weekly_df["week"].max())
    print("Unique stations:", weekly_df["station"].nunique())

    ws = pd.to_datetime(weekly_df["week_start"], errors="coerce")
    iso_check = ws.dt.isocalendar()
    year_mismatches = (
        weekly_df["year"].astype(int) != iso_check.year.astype(int)
    ).sum()
    week_mismatches = (
        weekly_df["week"].astype(int) != iso_check.week.astype(int)
    ).sum()
    print(
        "ISO consistency - year mismatches:",
        year_mismatches,
        "week mismatches:",
        week_mismatches,
    )

    temp_complete = weekly_df[["tmax_c", "tmin_c"]].notna().all(axis=1).mean()
    print("Temperature completeness:", round(temp_complete * 100, 1), "%")

    bad_weeks = weekly_df[(weekly_df["week"] < 1) | (weekly_df["week"] > 53)]
    if not bad_weeks.empty:
        print("WARNING:", len(bad_weeks), "records with invalid ISO week numbers")


def save_weekly_data(weekly_df, output_path):

    output_path.parent.mkdir(parents=True, exist_ok=True)
    weekly_df.to_parquet(output_path, index=False)
    print("Saved weekly data to", output_path)
    print("Columns:", list(weekly_df.columns))
    print("Records:", len(weekly_df))


def main():

    input_root = Path("../../data/interim/weather-monthly")
    output_path = Path("../../data/processed/weather_weekly_clean.parquet")

    try:

        weekly_df = load_and_process_daily_data(input_root)

        weekly_df["year"] = weekly_df["year"].astype("int16")
        weekly_df["week"] = weekly_df["week"].astype("int16")

        validate_weekly_data(weekly_df)

        save_weekly_data(weekly_df, output_path)

        print("Processing complete")

    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
