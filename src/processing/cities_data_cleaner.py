# src/processing/cities_data_cleaner.py

# Cities Data Cleaner
# Clean GeoNames cities data for weather station matching


import pandas as pd
import unicodedata
from pathlib import Path


def normalize_text(text):

    if pd.isna(text):
        return ""

    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_text.lower().strip()


def extract_alternate_names(alternates_str, limit=20):

    if pd.isna(alternates_str):
        return []

    names = alternates_str.split(",")
    normalized_names = set()
    for name in names:
        norm_name = normalize_text(name)
        if len(norm_name) > 2:
            normalized_names.add(norm_name)

    return sorted(list(normalized_names))[:limit]


def load_geonames_data(input_path):

    print("loadd geonames cities data:")

    column_names = [
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

    use_columns = [
        "geonameid",
        "name",
        "asciiname",
        "alternatenames",
        "latitude",
        "longitude",
        "feature_class",
        "feature_code",
        "country_code",
        "admin1_code",
        "population",
        "timezone",
    ]

    try:
        df = pd.read_csv(
            input_path,
            sep="\t",
            names=column_names,
            usecols=use_columns,
            low_memory=False,
            dtype={"geonameid": "int64", "population": "str"},
        )
        print("loaded", len(df), "records from geonames")
        return df

    except Exception as e:
        print("Error loading GeoNames data:", str(e))
        raise


def filter_populated_places(df):

    print("filtering to populated places...")

    populated_codes = {"PPL", "PPLA", "PPLA2", "PPLA3", "PPLA4", "PPLC"}

    filtered_df = df[
        (df["feature_class"] == "P") & (df["feature_code"].isin(populated_codes))
    ].copy()

    print("filtered", len(df), "to", len(filtered_df), "populated places")
    return filtered_df


def clean_and_normalize_data(df):

    print("Cleaning and normalizing data:")

    before_count = len(df)
    df = df.dropna(subset=["name", "latitude", "longitude", "country_code"]).copy()
    print("removed", before_count - len(df), "records with missing essential data")

    df["population"] = (
        pd.to_numeric(df["population"], errors="coerce").fillna(0).astype("int64")
    )

    df["city_norm"] = df["name"].apply(normalize_text)

    print("Processing alternate names:")
    df["alt_names_norm"] = df["alternatenames"].apply(extract_alternate_names)

    print("Data cleaning completed")
    return df


def deduplicate_cities(df):
    print("Removing duplicate cities:")

    before_count = len(df)

    deduped_df = (
        df.sort_values("population", ascending=False)
        .drop_duplicates(
            subset=["country_code", "admin1_code", "city_norm"], keep="first"
        )
        .reset_index(drop=True)
    )

    removed_count = before_count - len(deduped_df)
    print("removed", removed_count, "duplicate cities")
    print("fiinal dataset:", len(deduped_df), "unique cities")

    return deduped_df


def select_final_columns(df):
    final_columns = [
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
    return df[final_columns].copy()


def save_processed_data(df, output_path):

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    print("Saving processed data:")
    df.to_parquet(output_file, index=False)
    print("Saved", len(df), "cities to", output_file)


def generate_summary_stats(df):
    print()
    print("Dataset Summary:")

    print("Total cities:", len(df))
    print("Countries:", df["country_code"].nunique())

    print()
    print("Feature code distribution:")
    feature_counts = df["feature_code"].value_counts()
    for code, count in feature_counts.items():
        print(" ", code + ":", count)

    pop_stats = df["population"].describe()
    print()
    print("Population statistics:")
    print("Mean:", round(pop_stats["mean"], 0))
    print("Median:", round(pop_stats["50%"], 0))
    print("Max:", round(pop_stats["max"], 0))

    # Top countries
    print()
    print("Top 10 countries by city count:")
    top_countries = df["country_code"].value_counts().head(10)
    for country, count in top_countries.items():
        print(" ", country + ":", count)


def main():

    input_file = "../../data/interim/cities1000.txt"
    output_file = "../../data/processed/cities_clean_enhanced.parquet"

    print("Input:", input_file)
    print("Output:", output_file)
    print()

    try:

        df = load_geonames_data(input_file)
        df = filter_populated_places(df)
        df = clean_and_normalize_data(df)
        df = deduplicate_cities(df)
        df = select_final_columns(df)

        generate_summary_stats(df)

        save_processed_data(df, output_file)

        print()
        print("Processing completed successfully!")

    except Exception as e:
        print("Error during processing:", str(e))
        raise


if __name__ == "__main__":
    main()
