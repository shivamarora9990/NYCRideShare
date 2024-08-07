import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col, month
from graphframes import *
from pyspark.sql.functions import sum, avg, rank
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window




if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    #TASK-1
    rideshare_data_path = "s3a://{}/ECS765/rideshare_2023/rideshare_data.csv".format(s3_data_repository_bucket)
    taxi_zone_lookup_df_path = "s3a://{}/ECS765/rideshare_2023/taxi_zone_lookup.csv".format(s3_data_repository_bucket)
    
    rideshare_df = spark.read.csv(rideshare_data_path, header=True)
    taxi_zone_df = spark.read.csv(taxi_zone_lookup_df_path,header=True)
    
    # Perform the joins
    joined_df = rideshare_df.join(taxi_zone_df, rideshare_df.pickup_location == taxi_zone_df.LocationID) \
        .withColumnRenamed("Borough", "Pickup_Borough") \
        .withColumnRenamed("Zone", "Pickup_Zone") \
        .withColumnRenamed("Service_zone", "Pickup_service_zone") \
        .drop("LocationID")
    
    joined_df = joined_df.join(taxi_zone_df, joined_df.dropoff_location == taxi_zone_df.LocationID) \
        .withColumnRenamed("Borough", "Dropoff_Borough") \
        .withColumnRenamed("Zone", "Dropoff_Zone") \
        .withColumnRenamed("Service_zone", "Dropoff_service_zone") \
        .drop("LocationID")
    
    # Convert date from UNIX timestamp to 'yyyy-MM-dd'
    joined_df = joined_df.withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd"))
   
    # Print dataframe
    joined_df.show()
    
    #  Print the schema and count the number of rows
     joined_df.printSchema()
    count_rows = joined_df.count()
    print('\n'+ f'Number of Rows: {count_rows}'+'\n')

    
# #    #TASK-2
    
   # # Convert 'date' from string to date type to extract month
    joined_df = joined_df.withColumn("month", month("date"))
    
    # # # # Group by business and month, and count trips
    monthly_trips = joined_df.groupBy('business', 'month').count().orderBy('business', 'month')
    monthly_trips.show()

    # Calculate the platform's profits
    monthly_profits = joined_df.groupBy('business', 'month').agg(sum('rideshare_profit').alias('total_profit'))\
                            .orderBy("total_profit",ascending=False)
    # Group by business and month, and sum profits

    monthly_profits.show(5)
    
    # Group by business and month, and sum driver's earnings
    monthly_earnings = joined_df.groupBy('business', 'month').agg(sum('driver_total_pay').alias('total_earnings'))\
                                .orderBy("total_earnings",ascending=False)
    
    monthly_earnings.show(5)
    
    # Plottingg code using Matplotlib for trip counts
    import matplotlib.pyplot as plt
    
    # Data preparation for plotting trips
    trip_plot_data = {f"{row['business']}-{row['month']}": row['count'] for row in monthly_trips_data}
    
    # Bar plot for trip counts
    plt.bar(trip_plot_data.keys(), trip_plot_data.values())
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.xlabel('Business-Month')
    plt.ylabel('Number of Trips')
    plt.title('Number of Trips per Business per Month')
    plt.show()
    
    # Collect the data for plotting profits
    # monthly_profits_data = monthly_profits.collect()
    
    # Data preparation for plotting profits
    # profit_plot_data = {f"{row['business']}-{row['month']}": row['sum(rideshare_profit)'] for row in monthly_profits_data}
    
    # Bar plot for profits
    # plt.bar(profit_plot_data.keys(), profit_plot_data.values(), color='green')
    # plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    # plt.xlabel('Business-Month')
    # plt.ylabel('Total Profits')
    # plt.title('Total Profits per Business per Month')
    # plt.show()
    
    # Group by business and month, and sum driver's earnings
    monthly_earnings = joined_df.groupBy('business', 'month').agg(sum('driver_total_pay'))\
                                .withColumn("driver_total_pay",col("driver_total_pay").cast(FloatType()))\
                                .orderBy("monthly_earning",ascending=False)
    monthly_earnings.show()
    
    # Collect the data for plotting earnings
    monthly_earnings_data = monthly_earnings.collect()
    
    # Data preparation for plotting earnings
    earnings_plot_data = {f"{row['business']}-{row['month']}": row['sum(driver_total_pay)'] for row in monthly_earnings_data}
    
    # Bar plot for earnings
    plt.bar(earnings_plot_data.keys(), earnings_plot_data.values(), color='blue')
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.xlabel('Business-Month')
    plt.ylabel('Total Earnings')
    plt.title('Total Earnings per Business per Month')
    plt.show()

    # #TASK-3
    Stop the Spark session if no more operations are needed

    
    # Group by pickup borough and month, then count and order by count descending
    top_pickup_boroughs = joined_df.groupBy("Pickup_Borough", "month") \
                                       .agg(count("*").alias("trip_count")) \
                                       .orderBy("month", "trip_count", ascending=False)

    # Get the top 5 for each month
    top_pickup_boroughs.show(5)
        
    Group by dropoff borough and month, then count and order by count descending
    top_dropoff_boroughs = joined_df.groupBy("Dropoff_Borough", "month") \
                                        .agg(count("*").alias("trip_count")) \
                                        .orderBy("month", "trip_count", ascending=False)

    # Get the top 5 for each month
    top_dropoff_boroughs.show(5)

    # Group by pickup to dropoff borough and sum the total pay, then order by sum descending
    top_earning_routes = joined_df.groupBy("Pickup_Borough", "Dropoff_Borough") \
                                 .agg({"driver_total_pay": "sum"}) \
                                 .withColumnRenamed("sum(driver_total_pay)", "total_profit") \
                                 .orderBy("total_profit", ascending=False)
    
    # Get the top 30 earning routes
    top_30_earning_routes = top_earning_routes.head(30)
    print(top_30_earning_routes)
    top_earning_routes.show(30)

    # #TASK-4
    # Calculate average driver_total_pay by time_of_day
    avg_driver_pay_by_time = joined_df.groupBy('time_of_day') \
        .agg(F.avg('driver_total_pay').alias('average_driver_total_pay')) \
        .orderBy(F.col('average_driver_total_pay').desc())
    
    
    # Display the result
    avg_driver_pay_by_time.show()
    
    # Calculate average trip_length by time_of_day
    avg_trip_length_by_time = joined_df.groupBy('time_of_day') \
        .agg(F.avg('trip_length').alias('average_trip_length')) \
        .orderBy(F.col('average_trip_length').desc())
    
    # # Display the result
    avg_trip_length_by_time.show()
    
    # avg_driver_pay_by_time and avg_trip_length_by_time are the DataFrames from tasks 4.1 and 4.2
    # Join them on the time_of_day column
    avg_earnings_per_mile_by_time = avg_driver_pay_by_time.join(
        avg_trip_length_by_time, 'time_of_day'
    ).select(
        'time_of_day',
        (F.col('average_driver_total_pay') / F.col('average_trip_length')).alias('average_earning_per_mile')
    ).orderBy(F.col('average_earning_per_mile').desc())
    
    # Display the result
    avg_earnings_per_mile_by_time.show()

    # # #TASK-5
    from pyspark.sql.functions import dayofmonth
    
    # Filter for January data
    january_data = joined_df.filter(F.month('date') == 1)
    
    # Calculate the average waiting time per day in January
    avg_waiting_time_per_day = january_data.groupBy(dayofmonth('date').alias('day')) \
        .agg(F.avg('request_to_pickup').alias('average_waiting_time')) \
        .orderBy('day')
    
    avg_waiting_time_per_day.show()
    # Visualize the results in a histogram using Matplotlib
    avg_waiting_time_per_day_pd = avg_waiting_time_per_day.toPandas()
    
    import matplotlib.pyplot as plt
    
    plt.bar(avg_waiting_time_per_day_pd['day'], avg_waiting_time_per_day_pd['average_waiting_time'])
    plt.xlabel('Day')
    plt.ylabel('Average Waiting Time (seconds)')
    plt.title('Average Waiting Time by Day in January')
    plt.show()

    # Find days with average waiting time greater than 300 seconds
    days_above_300 = avg_waiting_time_per_day.filter('average_waiting_time > 300')
    
    # Display the result
    days_above_300.show()

    #TASK-6
    trip_count_df = joined_df.groupBy('Pickup_Borough', 'time_of_day').count().withColumnRenamed('count', 'trip_count')
    
    filtered_trip_counts = trip_count_df.filter((col('count') > 0) & (col('count') < 1000))
    
    # Collect and display the results
    filtered_trip_counts.show()

    evening_trip_counts = joined_df.filter(col('time_of_day') == 'evening') \
    .groupBy('Pickup_Borough') \
    .count() \
    .orderBy('count', ascending=False)

    # Collect and display the results
    evening_trip_counts.show()

    brooklyn_to_staten_island_trips = joined_df.filter((col('Pickup_Borough') == 'Brooklyn') & (col('Dropoff_Borough') == 'Staten Island')) \
        .groupBy('Pickup_Borough', 'Dropoff_Borough', 'Pickup_Zone') \
        .count()
    
    # Show 10 samples
    brooklyn_to_staten_island_trips.show(10)

    # #Task-7
    from pyspark.sql.functions import concat_ws, col, lit    
    # Concatenate the pickup and dropoff zones to create the route
    joined_df = joined_df.withColumn("Route", concat_ws(" to ", "Pickup_Zone", "Dropoff_Zone"))
    
    # Count the number of Uber and Lyft trips separately
    uber_trips = joined_df.filter(col("business") == "Uber").groupBy("Route").count().withColumnRenamed("count", "uber_count")
    lyft_trips = joined_df.filter(col("business") == "Lyft").groupBy("Route").count().withColumnRenamed("count", "lyft_count")
    
    # Join the counts on the Route
    route_counts = uber_trips.join(lyft_trips, "Route", "outer")  # Use outer join to keep all routes
    
    # Fill nulls with 0 since there might be routes with only Uber or only Lyft trips
    route_counts = route_counts.na.fill(0)
    
    # Calculate the total count for each route
    route_counts = route_counts.withColumn("total_count", col("uber_count") + col("lyft_count"))
    
    # Order by total_count to get the top 10 routes
    top_routes = route_counts.orderBy("total_count", ascending=False).limit(10)
    
    # Show the results
    top_routes.show(truncate=False)



    spark.stop()









  
    





