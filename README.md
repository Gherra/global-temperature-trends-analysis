# Global City Temperature Trends Analysis (2000–2024)

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?logo=pandas&logoColor=white)
![SciPy](https://img.shields.io/badge/SciPy-8CAAE6?logo=scipy&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-F7931E?logo=scikitlearn&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-F37626?logo=jupyter&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

Analyzing 25 years of daily weather observations across 92 cities worldwide to measure urban-scale warming rates using robust statistical methods. Processes 4.5GB of raw GHCN-Daily data through a PySpark → pandas pipeline with spatial indexing, seasonal bias correction, and non-parametric hypothesis testing.

**77 out of 92 cities (83.7%) show positive warming trends, with a median rate of +0.29°C per decade.** Of those, 39 reach statistical significance at p < 0.05. A binomial test confirms this pattern is extremely unlikely under a null hypothesis of no warming (p < 0.0001).

![Temperature Trends Overview](outputs/figures/temperature_trends_overview.png)

---

## Motivation

Most global warming analyses operate at continental or planetary scale. I wanted to ask a more specific question: can you detect warming trends at the scale of individual cities using publicly available station data? And if so, how do you handle the data quality problems that come with 25 years of fragmented, unevenly distributed weather observations?

This project tackles both the data engineering and the statistical methodology needed to answer that question rigorously.

---

## The hard problems (and how I solved them)

### Seasonal data gaps inflate cold-climate trends

This was the biggest methodological issue I ran into. Weather stations in cold climates (especially Arctic and subarctic) often have missing winter observations. If you just average whatever data exists for each year, you end up with a warm-season bias that makes trends look steeper than they really are.

Yellowknife, for example, initially showed **+2.61°C/decade** — physically unrealistic for a 25-year window. The root cause: years with sparse winter data produced artificially high annual averages, and those years clustered in the early 2000s.

I fixed this by adding seasonal balance requirements before a city-year is included in the trend calculation:
- ≥47 weeks of data per year (90% temporal coverage)
- ≥11 distinct months represented
- ≥8 weeks in each meteorological season (DJF, MAM, JJA, SON)

After applying these filters, Yellowknife dropped to **+1.3°C/decade** — still high (consistent with Arctic amplification), but within the range of published estimates.

### Geographic imbalance in station coverage

GHCN has far more stations in North America and Europe than in Africa, South America, or central Asia. Without correction, the analysis would just be measuring warming in the developed world and calling it "global."

I used quota-based geographic sampling across latitude bands and continents: after quality filtering, I selected cities to ensure no single region dominated the dataset. The final 92 cities span 6 continents and latitudes from -47°S to +78°N.

### Choosing the right regression method

I used **Theil-Sen** as the primary trend estimator instead of OLS. Theil-Sen computes the median of all pairwise slopes, which makes it resistant to outliers and doesn't assume normally distributed residuals — both important properties for noisy climate data. It's the standard choice in published climate trend analyses for this reason.

OLS is still in the pipeline, but only for computing R² and p-values. The actual trend numbers come from Theil-Sen.

### Confirming results with non-parametric tests

Shapiro-Wilk testing confirmed the trend distribution is non-normal (p = 0.035), so I used non-parametric methods throughout the validation:

- **Binomial test** — Is the proportion of warming cities significantly greater than 50%? (Yes, p ≈ 10⁻¹¹)
- **Wilcoxon signed-rank** — Is the median trend significantly different from zero? (Yes, p ≈ 10⁻¹⁶)
- **Kruskal-Wallis** — Do warming rates differ across climate zones? (No significant difference — warming is global)
- **Spearman correlation** — Does latitude predict trend magnitude? (ρ = 0.17, p = 0.11 — no significant relationship)

---

## Data pipeline

```
Raw GHCN-Daily observations (4.5GB, ~1B rows)
    ↓  src/spark/ghcn_extract.py
    ↓  PySpark on computing cluster: filter to 2000-2024, pivot elements
    ↓  to wide format, join station metadata, partition by year
    ↓
Daily station data (parquet, partitioned)
    ↓  src/processing/daily_to_weekly_converter.py
    ↓  Weekly mean temperatures per station with ISO week alignment
    ↓  Validation: date consistency, completeness checks
    ↓
Weekly station averages
    ↓  src/processing/city_station_matcher.py
    ↓  BallTree spatial index (haversine metric) matches stations to
    ↓  nearest city within 50km. Requires ≥10 years data, ≥90% temp pairs.
    ↓
Station-city matched dataset
    ↓  notebooks/01_temperature_trends_analysis.ipynb
    ↓  Quality filtering → geographic sampling → seasonal balance →
    ↓  Theil-Sen regression per city → visualization
    ↓
Per-city warming trends (92 cities)
    ↓  notebooks/02_advanced_statistics.ipynb
    ↓  Normality testing, climate zone analysis, outlier identification
    ↓
Final validated results → outputs/
```

### Data sources
- **Weather observations:** [GHCN-Daily](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily) (NOAA) — the largest global archive of daily surface observations
- **City coordinates:** [GeoNames](http://www.geonames.org/) cities1000.txt — all cities with population > 1,000

---

## Results

### Global overview

The overview figure (top of README) shows four views of the results: trend vs. latitude with 10° bin medians, the distribution of trend magnitudes (positive-skewed, centered around +0.3°C/decade), trend vs. model fit quality, and a world map of warming/cooling cities.

### Temperature evolution by latitude band

![Latitude band temperature evolution](outputs/figures/latband_evolution_loess.png)

LOESS-smoothed temperature anomalies over time, split by latitude band. Each thin line is an individual city; the thick line with shading is the smoothed band average ± uncertainty. Warming is visible across all latitude groups, with Arctic cities showing the steepest trends (consistent with polar amplification).

### Statistical validation

![Advanced statistical analysis](outputs/figures/advanced_stats.png)

Normality assessment (QQ-plot + Shapiro-Wilk), climate zone trend comparisons via Kruskal-Wallis, and outlier analysis. Utqiaġvik (Barrow, Alaska) shows up as an extreme positive outlier at +1.25°C/decade — this is a real signal from Arctic amplification, not a data artifact. The analysis flags it but retains it.

---

## Repository structure

```
src/
  spark/
    ghcn_extract.py                   # PySpark ETL: raw GHCN → pivoted parquet
  processing/
    daily_to_weekly_converter.py      # Daily → weekly aggregation + validation
    city_station_matcher.py           # BallTree spatial matching (haversine, ≤50km)
    cities_data_cleaner.py            # GeoNames cleaning, dedup, normalization
    cities_to_csv.py                  # CSV export for cleaned city data

notebooks/
  01_temperature_trends_analysis.ipynb    # Main pipeline: selection → trends → plots
  02_advanced_statistics.ipynb            # Hypothesis testing + outlier analysis

outputs/
  figures/                            # Publication-quality visualizations
  tables/                             # CSV results: trends, annual data, summaries
    final_temperature_trends.csv      # Per-city: trend, R², p-value, significance
    temperature_trends_results.csv    # Full results with all computed fields
    annual_temperature_data.csv       # City-year level data used for regression
  final_analysis_stats.json           # Aggregate statistics (92 cities summary)

data/
  processed/                          # Station-city mapping (parquet)
  samples/                            # Bundled sample data for demo mode
```

---

## Running it yourself

The notebooks auto-detect whether the full dataset is available. If not, they fall back to **demo mode** using bundled sample data (~25 cities). You get the full methodology on a smaller dataset — no cluster access needed.

```bash
pip install pandas numpy matplotlib seaborn scipy scikit-learn statsmodels pyarrow

cd notebooks/
jupyter notebook 01_temperature_trends_analysis.ipynb
```

> **Note:** Figures generated in demo mode use the sample subset. The figures in `outputs/figures/` are from the full 92-city run on the computing cluster.

For the full pipeline (4.5GB GHCN data), you'll need access to the raw dataset and a Spark-capable environment. The PySpark extraction script (`src/spark/ghcn_extract.py`) expects the GHCN-repartitioned format.

---

## Key technical details

| Component | Implementation |
|---|---|
| Distributed ETL | PySpark — pivot, filter, join, partition by year |
| Spatial matching | scikit-learn BallTree with haversine metric |
| Weekly aggregation | pandas groupby with ISO week alignment |
| Trend estimation | Theil-Sen (primary), OLS (R²/p-values only) |
| Smoothing | LOESS via statsmodels |
| Hypothesis testing | Binomial, Wilcoxon, Kruskal-Wallis, Spearman |
| Data I/O | PyArrow parquet throughout |
| Visualization | matplotlib + seaborn |

---

## What I'd improve next

- **Station-level decomposition** — separate urbanization heat island effects from background warming using rural vs. urban station pairs
- **Automated updates** — build the pipeline to ingest new GHCN releases and recompute trends incrementally
- **Interactive explorer** — a web dashboard where you can click a city and see its trend, time series, and contributing stations

---

## Author

**Raman Kumar** — [GitHub](https://github.com/Gherra) · [Portfolio](https://gherra.github.io/)