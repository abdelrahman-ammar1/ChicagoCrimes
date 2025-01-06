import matplotlib
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, to_date
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
import geopandas as gpd
import pandas as pd
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ChicagoCrimeAnalysis") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.serializer", KryoSerializer.getName) \
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
    .config("spark.jars", "geotools-wrapper-1.7.0-28.5.jar,sedona-spark-shaded-3.5.2.12-1.7.0.jar") \
    .getOrCreate()

SedonaRegistrator.registerAll(spark)

##TASK 1

# Load the CSV file from the local file system
crime_df = spark.read.csv("file:///root/BigData/workspace/project/Chicago_Crimes_10k.csv", header=True, inferSchema=True)

# Introduce a geometry attribute using ST_Point
crime_df = crime_df.withColumn("crime_geometry", expr("ST_Point(x, y)"))

# Rename attributes with spaces
crime_df = crime_df.withColumnRenamed("Case Number", "CaseNumber") \
                   .withColumnRenamed("Primary Type", "PrimaryType") \
                   .withColumnRenamed("Location Description", "LocationDescription") \
                   .withColumnRenamed("Community Area", "CommunityArea") \
                   .withColumnRenamed("FBI Code", "FBICode")

# Load the ZIP Code dataset
zip_code_df = spark.read.format("shapefile").load("file:///root/BigData/workspace/project/tl_2018_us_zcta510/tl_2018_us_zcta510.shp")

# Print the schema of the ZIP code dataset to identify the geometry column
zip_code_df.printSchema()

# Alias the DataFrames
crime_df_alias = crime_df.alias("c")
zip_code_df_alias = zip_code_df.alias("z")

# Spatial join to find ZIP code of each crime
# Use the aliases to qualify the column names
joined_df = crime_df_alias.join(
    zip_code_df_alias,
    expr("ST_Contains(z.geometry, c.crime_geometry)")
)

# Add ZIPCode attribute
joined_df = joined_df.withColumn("ZIPCode", col("ZCTA5CE10"))

# Drop unnecessary columns
final_df = joined_df.drop("c.crime_geometry", "z.geometry")

# Write output as Parquet file
final_df.write.parquet("file:///root/BigData/workspace/project/output/Chicago_Crimes_ZIP")



##TASK 2


# Load the Parquet file from Task 1
crime_zip_df = spark.read.parquet("file:///root/BigData/workspace/project/output/Chicago_Crimes_ZIP/")

# Group by ZIPCode and count the number of crimes
crime_count_df = crime_zip_df.groupBy("ZIPCode").agg(count("*").alias("CrimeCount"))

# Load the ZIP Code dataset
zip_code_df = spark.read.format("shapefile").load("file:///root/BigData/workspace/project/tl_2018_us_zcta510/tl_2018_us_zcta510.shp")

# Alias the DataFrames
crime_count_df_alias = crime_count_df.alias("c")
zip_code_df_alias = zip_code_df.alias("z")

# Join the crime count with the ZIP code geometry
joined_df = crime_count_df_alias.join(
    zip_code_df_alias,
    crime_count_df_alias.ZIPCode == zip_code_df_alias.ZCTA5CE10
)

# Select the necessary columns for the Shapefile
final_df = joined_df.select(
    col("z.geometry").alias("geometry"),  # Ensure the geometry column is named correctly
    col("c.ZIPCode").alias("ZIPCode"),   # Shorten column names if necessary
    col("c.CrimeCount").alias("CrimeCnt")  # Shorten column names if necessary
)

# Print the schema and a few rows for debugging
final_df.printSchema()
final_df.show(5)

# Check for null values in the geometry column
null_geometry_count = final_df.filter(col("geometry").isNull()).count()
if null_geometry_count > 0:
    print(f"Warning: {null_geometry_count} rows have null geometry. These rows will be dropped.")
    final_df = final_df.filter(col("geometry").isNotNull())

# Coalesce to a single partition to ensure a single output file
final_df = final_df.coalesce(1)

# Write the output as GeoJSON (alternative to Shapefile)
try:
    final_df.write.format("geojson").save("file:///root/BigData/workspace/project/output/ZIPCodeCrimeCount_GeoJSON")
    print("GeoJSON file written successfully.")
except Exception as e:
    print(f"Error writing GeoJSON: {e}")



##CHOROPLETH MAP



matplotlib.use('Agg')  # Use a non-interactive backend

directory_path = "/root/BigData/workspace/project/output/ZIPCodeCrimeCount_GeoJSON/"

# Find the first JSON file in the directory
json_files = [f for f in os.listdir(directory_path) if f.endswith(".json")]
if not json_files:
    raise FileNotFoundError(f"No JSON files found in directory: {directory_path}")

# Construct the full file path
json_file_path = os.path.join(directory_path, json_files[0])

# Read the JSON file using GeoPandas
crime_count_gdf = gpd.read_file(json_file_path)

# Print columns to verify the correct column name
print("Columns in GeoDataFrame:", crime_count_gdf.columns)

# Plot the choropleth map
fig, ax = plt.subplots(1, 1, figsize=(12, 8))
crime_count_gdf.plot(column="CrimeCnt", cmap="OrRd", legend=True, ax=ax, missing_kwds={"color": "lightgrey"})
ax.set_title("Crime Count by ZIP Code in Chicago", fontsize=16)
ax.set_axis_off()  # Hide the axis

# Save the choropleth map as an image
plt.savefig("/root/BigData/workspace/project/output/ChoroplethMap.png", dpi=300, bbox_inches="tight")

# Show the plot (optional)
plt.show()


##TASK 3


matplotlib.use('PS')

crime_zip_df = spark.read.parquet("file:///root/BigData/workspace/project/output/Chicago_Crimes_ZIP/")

# Convert the 'Date' column to a proper date format
crime_zip_df = crime_zip_df.withColumn("Date", to_date(col("Date"), "MM/dd/yyyy hh:mm:ss a"))

# Define the date range for filtering
start_date = "2005-01-01"  # Example start date
end_date = "2010-12-31"    # Example end date

# Filter the data based on the date range
filtered_df = crime_zip_df.filter((col("Date") >= start_date) & (col("Date") <= end_date))

# Group by crime type and count the number of crimes
crime_type_count_df = filtered_df.groupBy("PrimaryType").agg(count("*").alias("CrimeCount"))

# Convert the result to a Pandas DataFrame for visualization
crime_type_count_pd = crime_type_count_df.toPandas()

# Sort the data by crime count in descending order
crime_type_count_pd = crime_type_count_pd.sort_values(by="CrimeCount", ascending=False)

# Plot the bar chart
plt.figure(figsize=(12, 8))
plt.bar(crime_type_count_pd["PrimaryType"], crime_type_count_pd["CrimeCount"], color="skyblue")
plt.xlabel("Crime Type")
plt.ylabel("Number of Crimes")
plt.title(f"Number of Crimes by Type ({start_date} to {end_date})")
plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
plt.tight_layout()

# Save the plot as an image
plt.savefig("/root/BigData/workspace/project/output/CrimeTypeBarChart.png", dpi=300, format="png")
  # Save as PostScript file

# Stop the Spark session
spark.stop()