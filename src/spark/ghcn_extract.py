import sys
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("GHCN pleasant extract").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

GHCN_PATH = "/courses/datasets/ghcn-repartitioned"
STATIONS_PATH = "/courses/datasets/ghcn-more/ghcnd-stations.txt"

# shcemas
obs_schema = T.StructType(
    [
        T.StructField("station", T.StringType()),
        T.StructField("date", T.StringType()),  # yyyyMMdd (string)
        T.StructField("element", T.StringType()),  # TMAX/TMIN/PRCP/SNOW/SNWD/...
        T.StructField("value", T.IntegerType()),
        T.StructField("mflag", T.StringType()),
        T.StructField("qflag", T.StringType()),
        T.StructField("sflag", T.StringType()),
        T.StructField("obstime", T.StringType()),
        T.StructField("year", T.IntegerType(), nullable=True),
    ]
)

station_schema = T.StructType(
    [
        T.StructField("station", T.StringType()),
        T.StructField("latitude", T.FloatType()),
        T.StructField("longitude", T.FloatType()),
        T.StructField("elevation", T.FloatType()),
        T.StructField("name", T.StringType()),
    ]
)


def parse_station(line):

    return [
        line[0:11].strip(),
        float(line[12:20]),
        float(line[21:30]),
        float(line[31:37]),
        line[41:71].strip(),
    ]


def main(outdir, start_year=2000, end_year=2024):

    stations_rdd = spark.sparkContext.textFile(STATIONS_PATH).map(parse_station)
    stations = spark.createDataFrame(stations_rdd, station_schema).hint("broadcast")

    obs = spark.read.csv(GHCN_PATH, header=None, schema=obs_schema)

    obs = obs.filter((F.col("year") >= start_year) & (F.col("year") <= end_year))
    obs = obs.filter(F.col("qflag").isNull())

    keep = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"]
    obs = obs.filter(F.col("element").isin(keep))

    obs = obs.withColumn("date", F.to_date("date", "yyyyMMdd"))

    wide = obs.groupBy("station", "date").pivot("element", keep).agg(F.first("value"))

    wide = (
        wide.withColumn("tmax_c", F.col("TMAX") / 10.0)
        .withColumn("tmin_c", F.col("TMIN") / 10.0)
        .withColumn("prcp_mm", F.col("PRCP") / 10.0)
        .withColumn("snow_mm", F.col("SNOW").cast("double"))
        .withColumn("snwd_mm", F.col("SNWD").cast("double"))
    )

    wide = wide.join(stations, on="station", how="left")

    tidy = wide.select(
        "station",
        "date",
        "tmax_c",
        "tmin_c",
        "prcp_mm",
        "snow_mm",
        "snwd_mm",
        "latitude",
        "longitude",
        "elevation",
        "name",
    )

    tidy = tidy.withColumn("year", F.year("date"))
    (
        tidy.repartition("year")
        .write.mode("overwrite")
        .partitionBy("year")
        .parquet(outdir)
    )


if __name__ == "__main__":
    outdir = sys.argv[1] if len(sys.argv) > 1 else "ghcn_tidy.parquet"
    main(outdir, start_year=2000, end_year=2024)
