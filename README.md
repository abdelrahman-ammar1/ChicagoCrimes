# ChicagoCrimes
## README

## Group Information
- **Group Number:** 1
- **Project Title:** Analyzing and Visualizing Chicago Crime Data Using Big Data Tools

---

## Students Information

| Name                   | Task Contribution                        | GitHub Profile |
|------------------------|------------------------------------------|----------------|
| **Omar Ackef**         | - Data Cleaning and Enrichment (Task 1)  |
| **Abdelrahman Ammar**  | - Spatial Analysis (Task 2)              |
| **Omar Ackef**         | - Temporal Analysis (Task 3)             |
| **Abdelrahman Ammar**  | - Report Writing and Documentation       | [GitHub](https://github.com/abdelrahman-ammar1/ChicagoCrimes/tree/main) |

---

## Project Overview

This project analyzes a sample of crime data from Chicago to uncover patterns and trends using **Big Data Tools**. It demonstrates the use of technologies like **Apache Spark** and **Apache Sedona** for data processing and geospatial analysis, ensuring scalability and efficiency.

---

## Task Allocation

### Task 1: Data Cleaning and Enrichment
- **Details:**
  - Loaded raw CSV crime data into a Spark DataFrame.
  - Renamed columns for better compatibility.
  - Added geospatial attributes (e.g., ZIP codes) to crime records using Apache Sedona's spatial join capabilities.
  - Saved enriched data as a **Parquet** file for efficient querying and storage.

---

### Task 2: Spatial Analysis
- **Details:**
  - Aggregated crime data by ZIP code using Spark SQL.
  - Joined crime counts with geospatial data (ZIP code geometries).
  - Exported the resulting dataset in **GeoJSON format** for compatibility with geospatial visualization tools like QGIS.
  - Generated a **choropleth map** using GeoPandas and Matplotlib to visualize crime distribution.

---

### Task 3: Temporal Analysis
- **Details:**
  - Filtered crimes by date range using command-line arguments for flexibility.
  - Grouped crimes by type and analyzed trends using Spark SQL.
  - Saved the temporal analysis results as a **CSV** file.
  - Generated a **bar chart** using Matplotlib to illustrate crime trends by type.

---

## Tools and Technologies Used

1. **Big Data Tools:**
   - Apache Spark for large-scale data processing and SQL querying.
   - Apache Sedona for geospatial operations like spatial joins and geometry handling.

2. **Visualization Tools:**
   - **GeoPandas** and **Matplotlib** for automated generation of bar charts and choropleth maps.
   - Outputs are compatible with external tools like **QGIS** for advanced geospatial visualization.

---

## Output and Visualizations

1. **Task 1 Output:**
   - Enriched crime data saved as a **Parquet file** for efficient querying.

2. **Task 2 Output:**
   - Aggregated data by ZIP code saved as a **GeoJSON file**.
   - **Choropleth Map**: Visualizes crime count distribution by ZIP code.

3. **Task 3 Output:**
   - Temporal analysis results saved as a **CSV file**.
   - **Bar Chart**: Visualizes the number of crimes by type for the selected date range.

---

## GitHub Repository

- [Project Repository](https://github.com/abdelrahman-ammar1/ChicagoCrimes/tree/main)

This repository contains:
- **ChicagoCrimesAnalysis.py**: The updated Python script for the analysis pipeline.
- **Dataset Samples**: Example datasets used for analysis.
- **Output Files**:
  - **Parquet** and **GeoJSON** files for data storage.
  - Visualization files (choropleth maps and bar charts) in PNG format.
- **Documentation**: Final report and detailed methodology.
- **Presentation**: An overview of the code and project results.
