# Global City Temperature Trends Analysis (2000-2024)

**CMPT 353 Final Project**  
**Author:** Raman Kumar (rka138)

## Project Overview

This project analyzes global temperature trends across 92 cities from 2000-2024 using robust statistical methods. The analysis reveals widespread warming with a median rate of 0.29°C per decade.

### Key Results (Full Analysis)
- 92 cities analyzed with balanced geographic coverage
- 83.7% show warming trends (77/92 cities)
- Median warming rate: 0.29°C/decade  
- No significant latitude dependence: Global warming pattern
- Statistically significant: p < 0.0001 vs random chance

---
```
## Repository Structure

│       
├──data
│   ├── interim
│   │  
│   ├── processed
│   │   ├── station_to_city_map_calculated_with_the_week_dates_data.parquet
│   └── samples                                                              # Sample input data
│       ├── cities_sample.txt
│       ├── station_to_city_map_calculated_with_the_week_dates_data.parquet
│       └── weather_sation_with_city_sample.parquet
├── notebooks
│   ├── 01_temperature_trends_analysis.ipynb                               # Main analysis notebook
│   └── 02_advanced_statistics.ipynb                                       # Statistical validation
├── outputs
│   ├── figures                                                           # Analysis figures (PNG)
│   │   ├── advanced_stats.png
│   │   ├── latband_evolution_loess.png
│   │   └── temperature_trends_overview.png
│   ├── final_analysis_stats.json
│   └── tables                                                             # Results tables (CSV)
│       ├── advanced_statistical_tests.csv
│       ├── annual_temperature_data.csv
│       ├── city_data_summary.csv
│       ├── city_seasonal_coverage.csv
│       ├── final_temperature_trends.csv
│       ├── outlier_cities.csv
│       └── temperature_trends_results.csv
├── README.md
├── sample_data.py
└── src
    ├── processing
    │   ├── cities_data_cleaner.py                                       # Clean GeoNames cities data
    │   ├── city_station_matcher.py
    │   └── daily_to_weekly_converter.py
    └── spark
        └── ghcn_extract.py                                                # Extract data from cluster

```

## Quick Start for TAs/Instructors

**Ready to run immediately with included sample data:**

```bash
# Install required libraries
pip install pandas numpy matplotlib seaborn scipy scikit-learn statsmodels pyarrow

# Navigate to notebooks  
cd notebooks/

# Run main analysis
jupyter notebook 01_temperature_trends_analysis.ipynb

# Run statistical validation
jupyter notebook 02_advanced_statistics.ipynb
```

**What you'll see:** Complete analysis methodology with sample data demonstrating:
- Geographic city selection (reduced sample size)
- Seasonal balance controls  
- Robust trend estimation (Theil-Sen + OLS)
- Statistical validation and visualization

Please note the sample data is very small so it won't show a lot of detail.

---

## Sample Data Included

**For immediate testing/demonstration:**

- weather_sation_with_city_sample.parquet - 200K weather records from 2020
- station_to_city_map_calculated_with_the_week_dates_data.parquet - Complete station-city mapping (554 KB)
- cities_sample.txt - First 10K cities from GeoNames database

**Output from full analysis included:**
- All figures (PNG) showing complete results
- All data tables (CSV) with 92-city analysis
- Statistical test results and validation

---

## Required Libraries

```bash
pip install pandas numpy matplotlib seaborn scipy scikit-learn statsmodels pyarrow pathlib
```

**For cluster processing only:**
- PySpark (available on SFU cluster)

---

## Complete Data Pipeline (Full Analysis)

### 1. Raw Data Extraction (SFU Cluster)
```bash
# Extract GHCN-Daily data (2000-2024)
python src/spark/ghcn_extract.py output_directory
```
- **Input:** GHCN-Daily dataset from `/courses/datasets/ghcn-repartitioned/`
- **Output:** Daily weather observations (~4.5GB partitioned Parquet)

### 2. Data Processing Pipeline
```bash
# Convert daily to weekly aggregates  
python src/processing/daily_to_weekly_converter.py

# Clean cities database
python src/processing/cities_data_cleaner.py

# Match weather stations to cities
python src/processing/city_station_matcher.py
```

### 3. Analysis Execution
```bash
# Main analysis (city selection + trends + figures)
jupyter notebook notebooks/01_temperature_trends_analysis.ipynb

# Statistical validation + advanced plots  
jupyter notebook notebooks/02_advanced_statistics.ipynb
```

---

## Key Methodology

### Seasonal Balance Requirements
**Prevents bias from missing winter/summer data in extreme climates:**
- ≥47 weeks per year (90% coverage)
- ≥11 distinct months per year
- ≥8 weeks in each meteorological season (DJF, MAM, JJA, SON)
- Meteorological year: December assigned to following year

### Geographic Sampling  
**Ensures global representativeness:**
- Quality filters: ≥15 years data, ≥80% completeness, ≤50km station distance
- Balanced quotas across latitude×longitude×hemisphere grid
- Final selection: 92 cities across all continents and climate zones

### Robust Trend Analysis
**Handles noisy temperature data:**
- **Primary method:** Theil-Sen robust regression (resistant to outliers)
- **Secondary method:** OLS regression (for R^2 and p-values only)  
- **Validation:** Non-parametric tests due to non-normal distribution

---

## Output Files

### Figures (All Included)
- **`temperature_trends_overview.png`** - 2×2 overview with global map
- **`latband_evolution_loess.png`** - Time series by latitude bands with LOESS smoothing
- **`advanced_stats.png`** - Statistical validation plots (Q-Q, distributions, outliers)

### Data Tables (All Included)
- **`temperature_trends_results.csv`** - Per-city trend estimates and statistics
- **`annual_temperature_data.csv`** 
- **`city_data_summary.csv`** 
- **`city_seasonal_coverage.csv`** - Data quality diagnostics  
- **`final_temperature_trends.csv`** - Summary results table
- **`outlier_cities.csv`** - Outlier analysis (Utqiagvik flagged)
- **`final_analysis_stats.json`** -



---
