from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, to_date
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
import matplotlib.pyplot as plt
import sys
import geopandas as gpd
import os

# Accept start and end dates as command-line arguments
start_date = sys.argv[1]  # Example: "01/01/2018"
end_date = sys.argv[2]    # Example: "12/31/2018"

# Initialize Spark Session with Sedona for geospatial data processing
spark = SparkSession.builder \
    .appName("ChicagoCrimeAnalysis") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.serializer", KryoSerializer.getName) \
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
    .config("spark.jars", "geotools-wrapper-1.7.0-28.5.jar,sedona-spark-shaded-3.5_2.12-1.7.0.jar") \
    .getOrCreate()

# Register Sedona functions
SedonaRegistrator.registerAll(spark)

# -----------------------------
# Task 1: Data Preparation
# -----------------------------

# Load crime dataset as a Spark DataFrame
crime_df = spark.read.csv("file:///root/BigData/workspace/project/Chicago_Crimes_10k.csv", header=True, inferSchema=True)

# Rename columns for easier handling
crime_df = crime_df.withColumnRenamed("Case Number", "CaseNumber") \
                   .withColumnRenamed("Primary Type", "PrimaryType") \
                   .withColumnRenamed("Location Description", "LocationDescription") \
                   .withColumnRenamed("Community Area", "CommunityArea") \
                   .withColumnRenamed("FBI Code", "FBICode")
crime_df.createOrReplaceTempView("crime")  # Register as SQL view for querying

# Load ZIP Code dataset (shapefile) as a Spark DataFrame
zip_code_df = spark.read.format("shapefile").load("file:///root/BigData/workspace/project/tl_2018_us_zcta510/tl_2018_us_zcta510.shp")
zip_code_df.createOrReplaceTempView("zip")

# Perform a spatial join to link crimes to ZIP codes
crime_with_zip_df = spark.sql("""
    SELECT c.*, z.ZCTA5CE10 AS ZIPCode
    FROM crime c, zip z
    WHERE ST_Contains(z.geometry, ST_Point(c.x, c.y))
""")

# Save the resulting DataFrame as Parquet for faster access in future tasks
crime_with_zip_df.write.parquet("file:///root/BigData/workspace/project/output/Chicago_Crimes_ZIP")

# -----------------------------
# Task 2: Spatial Analysis
# -----------------------------

# Load the parquet dataset for spatial analysis
crime_zip_df = spark.read.parquet("file:///root/BigData/workspace/project/output/Chicago_Crimes_ZIP/")
crime_zip_df.createOrReplaceTempView("crime_zip")

# Aggregate the number of crimes per ZIP code
crime_count_by_zip_df = spark.sql("""
    SELECT ZIPCode, COUNT(*) AS CrimeCount
    FROM crime_zip
    GROUP BY ZIPCode
""")

# Register the aggregated data as a temporary SQL view
crime_count_by_zip_df.createOrReplaceTempView("crime_count_by_zip_df")

# Join aggregated data with ZIP code geometries for visualization
zip_with_crime_count_df = spark.sql("""
    SELECT z.geometry, c.ZIPCode, c.CrimeCount
    FROM zip z
    JOIN crime_count_by_zip_df c
    ON z.ZCTA5CE10 = c.ZIPCode
""")

# Reduce to a single partition for saving as a single GeoJSON file
zip_with_crime_count_df = zip_with_crime_count_df.coalesce(1)

# Save the output as GeoJSON for QGIS
shapefile_output_path = "file:///root/BigData/workspace/project/output/ZIPCodeCrimeCount"
zip_with_crime_count_df.write.format("geojson").save(shapefile_output_path)
print(f"GeoJSON file written successfully to {shapefile_output_path}")

# -----------------------------
# Task 3: Temporal Analysis
# -----------------------------

# Query crimes within the specified date range and group by crime type
crime_type_count_df = spark.sql(f"""
    SELECT PrimaryType, COUNT(*) AS CrimeCount
    FROM crime_zip
    WHERE to_date(Date, 'MM/dd/yyyy hh:mm:ss a') BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY PrimaryType
    ORDER BY CrimeCount DESC
""")

# Reduce to a single partition for saving as a single CSV file
crime_type_count_df = crime_type_count_df.coalesce(1)

# Save the temporal analysis output as CSV
crime_type_count_output_path = "file:///root/BigData/workspace/project/output/CrimeTypeCount"
crime_type_count_df.write.csv(crime_type_count_output_path, mode="overwrite")
print(f"Crime type count data written successfully to {crime_type_count_output_path}")

# Convert Spark DataFrame to Pandas for bar chart plotting
crime_type_count_pd = crime_type_count_df.toPandas()

# Plot the bar chart
plt.figure(figsize=(12, 8))
plt.bar(crime_type_count_pd["PrimaryType"], crime_type_count_pd["CrimeCount"], color="skyblue")
plt.xlabel("Crime Type")
plt.ylabel("Number of Crimes")
plt.title(f"Number of Crimes by Type ({start_date} to {end_date})")
plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
plt.tight_layout()

# Save the bar chart as an image
chart_output_path = "/root/BigData/workspace/project/output/CrimeTypeBarChart.png"
plt.savefig(chart_output_path, dpi=300)
print(f"Bar chart saved successfully to {chart_output_path}")

# -----------------------------
# Generate Choropleth Map
# -----------------------------

# Directory containing GeoJSON output
directory_path = "/root/BigData/workspace/project/output/ZIPCodeCrimeCount/"

# Find the first JSON file in the directory
json_files = [f for f in os.listdir(directory_path) if f.endswith(".json")]
if not json_files:
    raise FileNotFoundError(f"No JSON files found in directory: {directory_path}")

# Construct the full file path
json_file_path = os.path.join(directory_path, json_files[0])

# Load the JSON file as a GeoDataFrame using GeoPandas
gdf = gpd.read_file(json_file_path)

# Plot the choropleth map
choropleth_map_output_path = "/root/BigData/workspace/project/output/ChoroplethMap.png"
fig, ax = plt.subplots(1, 1, figsize=(12, 8))
gdf.plot(
    column="CrimeCount",         # Column to visualize
    cmap="OrRd",                 # Color map for visualization
    legend=True,                 # Add a legend
    ax=ax,                       # Axes to plot on
    missing_kwds={"color": "lightgrey"}  # Handle missing geometries
)
ax.set_title("Crime Count by ZIP Code in Chicago", fontsize=16)
ax.set_axis_off()  # Remove the axis for a cleaner look

# Save the choropleth map as an image
plt.savefig(choropleth_map_output_path, dpi=300, bbox_inches="tight")
print(f"Choropleth map saved successfully to {choropleth_map_output_path}")

# Stop the Spark session
spark.stop()

