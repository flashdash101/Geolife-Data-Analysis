# Import required libraries for PySpark operations, mathematical calculations, 
# and datetime handling
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from math import radians, cos, sin, asin, sqrt
from datetime import datetime, timedelta

# Initialize Spark Session with all available cores
# This creates a local session that can utilize all CPU cores
spark = SparkSession.builder \
    .appName("GPS_Trajectory_Analysis") \
    .master("local[*]") \
    .getOrCreate()
    
#Run 'spark-submit gps_analysis.py' in the terminal to execute this script.


# Define schema for the GPS trajectory data
# This ensures proper data type handling for each column:
# - UserID: Integer identifier for each user
# - Latitude/Longitude: Double precision for coordinates
# - Altitude: Double precision for elevation in meters
# - Timestamp: Double for Excel-format timestamps (days since 1899-12-30)
# - Date and Time: String format for readable date/time values
schema = StructType([
    StructField("UserID", IntegerType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("AllZero", IntegerType(), True),
    StructField("Altitude", DoubleType(), True),
    StructField("Timestamp", DoubleType(), True),
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True)
])

# Read the GPS trajectory data from CSV file using the defined schema
df = spark.read.csv("dataset.txt", header=True, schema=schema)

# Task 1: Time Zone Conversion
# Convert timestamps from GMT to local time based on longitude
# Process:
# 1. Calculate timezone offset (15 degrees = 1 hour)
# 2. Adjust timestamp by adding offset (converted to days)
# 3. Convert adjusted timestamp to readable date and time
# 4. Account for Excel epoch (25569 days offset)
df_adjusted = df \
    .withColumn("timezone_offset", (col("Longitude") / 15).cast("int")) \
    .withColumn("adjusted_timestamp", col("Timestamp") + (col("timezone_offset") / 24)) \
    .withColumn("adjusted_date", from_unixtime((col("adjusted_timestamp") - 25569) * 86400, "yyyy-MM-dd")) \
    .withColumn("adjusted_time", from_unixtime((col("adjusted_timestamp") - 25569) * 86400, "HH:mm:ss"))

# Use the timezone-adjusted DataFrame for all subsequent operations
df = df_adjusted

# Display sample of adjusted data to verify timezone conversion
print("Sample of timezone adjusted data:")
df.select("Longitude", "timezone_offset", "Timestamp", "adjusted_timestamp", "Date", "adjusted_date", "Time", "adjusted_time").show(5)

# Task 2: Beijing Records Filter
# Filter records within Beijing's approximate borders:
# - Latitude: 39.5° to 40.5° N
# - Longitude: 115.5° to 117.5° E
# This creates a rectangular boundary around Beijing
beijing_df = df.filter(
    (col("Latitude") >= 39.5) & 
    (col("Latitude") <= 40.5) & 
    (col("Longitude") >= 115.5) & 
    (col("Longitude") <= 117.5)
)

print("\nTask 2: Beijing Records")
beijing_count = beijing_df.count()
print(f"Number of records in Beijing area: {beijing_count}")

# Task 3: Days with >10 Points Analysis
# Process:
# 1. Group records by user and date
# 2. Count points per day
# 3. Filter for days with more than 10 points
# 4. Count qualifying days per user
# 5. Sort by count (descending) and UserID (ascending) for ties
daily_points = df.groupBy("UserID", "adjusted_date") \
    .agg(count("*").alias("daily_points")) \
    .filter(col("daily_points") > 10) \
    .groupBy("UserID") \
    .agg(count("*").alias("days_with_points")) \
    .orderBy(col("days_with_points").desc(), col("UserID").asc()) \
    .limit(6)

print("\nTask 3: Users with >10 daily points")
daily_points.show()

# Task 4: Weeks with >100 Points Analysis
# Process:
# 1. Extract ISO week number from adjusted date
# 2. Group by user and week
# 3. Count points per week
# 4. Filter for weeks with >100 points
# 5. Count qualifying weeks per user
weekly_points = df \
    .withColumn("WeekOfYear", weekofyear(to_date(col("adjusted_date")))) \
    .groupBy("UserID", "WeekOfYear") \
    .agg(count("*").alias("weekly_points")) \
    .filter(col("weekly_points") > 100) \
    .groupBy("UserID") \
    .agg(count("*").alias("weeks_with_points")) \
    .orderBy(col("weeks_with_points").desc())

print("\nTask 4: Users with >100 weekly points")
weekly_points.show()

# Task 5: Northernmost Points Analysis
# Process:
# 1. Find highest latitude point for each user (with earliest date for ties)
# 2. Identify users who visited Beijing
# 3. Join results to show Beijing visits for top 6 northernmost points
northernmost = df \
    .withColumn("rank", 
        row_number().over(
            Window.partitionBy("UserID")
            .orderBy(col("Latitude").desc(), to_date(col("adjusted_date")).asc())
        )) \
    .filter(col("rank") == 1) \
    .select("UserID", "Latitude", "adjusted_date") \
    .orderBy(col("Latitude").desc()) \
    .limit(6)

# Get list of users who visited Beijing area
beijing_visitors = beijing_df.select("UserID").distinct()

# Join northernmost points with Beijing visitors information
northernmost_with_beijing = northernmost \
    .join(beijing_visitors, "UserID", "left_outer") \
    .withColumn("VisitedBeijing", 
        when(beijing_visitors.UserID.isNotNull(), "True")
        .otherwise("False")) \
    .select("UserID", "Latitude", "adjusted_date", "VisitedBeijing") \
    .orderBy(col("Latitude").desc())

print("\nTask 5: Northernmost points and Beijing visits")
northernmost_with_beijing.show()

# Task 6: Altitude Span Calculation
# Process:
# 1. Calculate daily altitude range (max - min) for each user
# 2. Find maximum range across all days for each user
# 3. Select top 6 users by maximum range
altitude_span = df \
    .groupBy("UserID", "adjusted_date") \
    .agg((max("Altitude") - min("Altitude")).alias("daily_span")) \
    .groupBy("UserID") \
    .agg(max("daily_span").alias("max_span")) \
    .orderBy(col("max_span").desc(), col("UserID").asc()) \
    .limit(6)

print("\nTask 6: Maximum altitude spans")
altitude_span.show()

# Task 7: Distance Calculations
# Implementation of Haversine formula for calculating great circle distances
# between consecutive GPS points
def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth specified in decimal degrees
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Haversine formula components
    dlat = lat2 - lat1 
    dlon = lon2 - lon1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371  # Earth's radius in kilometers
    return c * r

# Register haversine function as UDF for use in Spark SQL
spark.udf.register("haversine", haversine, DoubleType())

# Calculate distances between consecutive points
# Process:
# 1. Create window for accessing previous points
# 2. Calculate distance between consecutive points using Haversine formula
# 3. Group by user and date to get daily distances
# 4. Find day with maximum distance for each user
print("\nTask 7: Distance calculations")
w = Window.partitionBy("UserID", "adjusted_date").orderBy("adjusted_timestamp")
distance_df = df \
    .withColumn("prev_lat", lag("Latitude").over(w)) \
    .withColumn("prev_lon", lag("Longitude").over(w)) \
    .filter(col("prev_lat").isNotNull()) \
    .withColumn("point_distance", 
        expr("haversine(prev_lat, prev_lon, Latitude, Longitude)")
    )

# Calculate daily distances and find maximum distance day for each user
daily_distances = distance_df \
    .groupBy("UserID", "adjusted_date") \
    .agg(sum("point_distance").alias("daily_distance")) \
    .withColumn("rank", 
        row_number().over(
            Window.partitionBy("UserID")
            .orderBy(col("daily_distance").desc(), col("adjusted_date").asc())
        )
    ) \
    .filter(col("rank") == 1) \
    .orderBy(col("daily_distance").desc())

# Calculate total distance across all users
total_distance = distance_df.agg(sum("point_distance")).collect()[0][0]

print(f"\nTotal distance traveled by all users: {total_distance:.2f} km")
print("\nLongest daily distances per user:")
daily_distances.show()

# Clean up Spark session
spark.stop()