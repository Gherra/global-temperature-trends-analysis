import pandas as pd

data = pd.read_parquet(
    "data/processed/weather_weekly_with_city_dated/2020/part-0.parquet"
)
sample = data.head(200000)
sample.to_parquet("data/samples/weather_sation_with_city_sample.parquet", index=False)
