# postgres-pgadmin-codespaces
Containerized PostgreSQL and pgAdmin for structured data ingestion.

## Dataset Overview

This project uses the **NYC Taxi Trip Record Datasets**, originally 
sourced from the official [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) page.

## Data Source & Content
The dataset contains trip records for **Yellow** and **Green** taxis in New York City. It includes detailed information for each trip, such as:
- Pickup and drop-off dates and times
- Pickup and drop-off locations
- Trip distances
- Itemized fares (breakdown of costs)
- Rate types
- Payment types
- Driver-reported passenger counts

## Timeframe & Original Format
The data covers trips from **2004 through 2006**. It was originally downloaded in **Parquet** format from the official source.

## Processing & Availability
For ease of use in this project, the data was:
1. Converted from Parquet to **CSV** format.
2. Compressed into **GZ** files.

The processed copy of this dataset is hosted in the [project repository](https://github.com/tantikristanti/NYC-Taxi-Dataset) for direct access.
