# GPS Trajectory Analysis System

## Overview
A PySpark-based analytical system for processing and deriving insights from large-scale GPS trajectory data. The system processes over 287 million data points to perform geographical pattern analysis, user behavior tracking, and movement statistics calculation, handling key aspects of big geospatial data processing.

## Key Features
- **Multi-timezone Timestamp Conversion** - Automatic adjustment based on longitudinal position
- **Geofencing Capabilities** - Beijing area filtering with rectangular boundary detection
- **User Behavior Analysis** - Daily/weekly activity pattern detection
- **Altitude Variation Tracking** - Maximum elevation span calculations
- **Movement Analytics** - Haversine-based distance calculations between consecutive points
- **Geographical Correlations** - Northernmost position analysis with Beijing visit correlations
- **Big Data Optimization** - Spark SQL and Window function optimizations for large datasets

## Core Analytical Components

### 1. Time Zone Normalization
- Automatic timezone offset calculation (15° longitude = 1 hour)
- Excel timestamp conversion (1899 epoch handling)
- Local time adjustment while maintaining original timestamps

### 2. Beijing Area Analysis
- Geographical filtering (39.5°-40.5°N, 115.5°-117.5°E)
- Visitor correlation with northernmost positions
- Spatial-temporal pattern analysis

### 3. Activity Pattern Detection
- Daily point concentration analysis (>10 points/day)
- Weekly activity tracking (>100 points/week)
- User ranking by activity frequency

### 4. Elevation Analysis
- Daily altitude range calculations
- Maximum span detection across observation period
- Extreme elevation change identification

### 5. Movement Analytics
- Haversine formula implementation for accurate distance calculations
- Consecutive point distance aggregation
- Daily movement patterns and total distance traveled




# Performance Considerations
1. Window Function Optimization - Partitioned by UserID/Date for efficient processing
2. UDF Registration - Haversine formula optimization for Spark parallelization
3. Memory Management - Clean DataFrame handling and session management
4. Schema Enforcement - Strict typing for efficient memory utilization
### Prerequisites
pyspark>=3.0
python>=3.8



# Example Outputs
```bash


# Time Zone Adjustment Sample
+---------+----------------+-------------+-------------------+--------+-------------+-----+-------------+
|Longitude|timezone_offset|   Timestamp|adjusted_timestamp|    Date|adjusted_date| Time|adjusted_time|
+---------+----------------+-------------+-------------------+--------+-------------+-----+-------------+
|  116.407|               7|41452.664062|        41452.74727|2013-6-9|   2013-06-10|15:56|     17:56:00|
+---------+----------------+-------------+-------------------+--------+-------------+-----+-------------+

# Beijing Area Analysis
Number of records in Beijing area: 1,240,582

# Distance Analytics
Total distance traveled by all users: 382,791.42 km
Longest daily distance: 127.65 km (User 1421, 2012-08-15)

```

# Dataset Preparation
## Place your GPS data in CSV format as dataset.txt with header:

```csv
UserID,Latitude,Longitude,AllZero,Altitude,Timestamp,Date,Time
```


# Runnning the code
```bash
spark-submit gps_analysis.py
```
