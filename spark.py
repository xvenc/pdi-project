from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse

# Argument parser
def parse_args():
    parser = argparse.ArgumentParser()
    #parser.add_argument("--task", type=int, default=1, help="Task number. [1-6]")
    parser.add_argument("--input", type=str, default="ODAE.json", help="Input file")
    args = parser.parse_args()
    return args

def df_preprocess(df):
    # Explode the 'features' array column to handle the nested struct
    df_exploded = df.select(
        F.explode("features").alias("feature"),
    )
    df_exploded = df_exploded.filter(df_exploded.feature.attributes.isinactive == 'false')

    return df_exploded

def task1_alternative(df):
    # Select relevant columns
    df_selected = df.select(
        "feature.attributes.id",
        "feature.attributes.lastupdate",
        "feature.attributes.vtype",
        "feature.attributes.bearing",
    )
    cond0 = F.col('vtype') == 0
    cond1 = F.col('vtype') == 1 # Tram
    cond2 = F.col('vtype') == 2 # Trolleybus
    cond3 = F.col('vtype') == 3 # Bus
    cond4 = F.col('vtype') == 4 # Boat
    cond5 = F.col('vtype') == 5 # Train
    # If the vehicle is moving south, the bearing is between 135 and 225 degrees
    cond_bearing = F.col('bearing').between(135, 225) 
    # Filter 
    cond = (cond1 | cond2 | cond3 | cond4 | cond5 | cond0) & cond_bearing
    df_filter = df_selected.filter(cond)

    # Define a window specification over vehicleID, ordered by lastupdate in descending order
    w = Window.partitionBy("id").orderBy(F.desc("lastupdate"))

    # Assign a rank to each record within each vehicleID partition based on lastupdate
    df_filter = df_filter.withColumn("rank", F.row_number().over(w)).filter("rank == 1").drop("rank")

    df_filter = df_filter.withColumn("lastupdate", F.from_unixtime(F.col("lastupdate")/1000).cast("timestamp"))

    return df_filter

def task1(df):
    # Select relevant columns
    df_selected = df.select(
        "feature.attributes.id",
        "feature.attributes.lastupdate",
        "feature.attributes.vtype",
        "feature.attributes.lat",
        "feature.attributes.bearing"
    )
    cond0 = F.col('vtype') == 0
    cond1 = F.col('vtype') == 1 # Tram
    cond2 = F.col('vtype') == 2 # Trolleybus
    cond3 = F.col('vtype') == 3 # Bus
    cond4 = F.col('vtype') == 4 # Boat
    cond5 = F.col('vtype') == 5 # Train
    cond = cond1 | cond2 | cond3 | cond4 | cond5 | cond0
    df_filter = df_selected.filter(cond)

    # If the latitude decreases the vehicle is moving south
    df_moving_south = df_filter.withColumn("moving_south", F.when(F.lag("lat").over(Window.partitionBy("id").orderBy("lastupdate")) > F.col("lat"), 1).otherwise(0))
    df_moving_south = df_moving_south.filter("moving_south == 1").drop("moving_south").drop("latitude_diff")
    # Define a window specification over vehicleID, ordered by lastupdate in descending order
    w = Window.partitionBy("id").orderBy(F.desc("lastupdate"))

    # Assign a rank to each record within each vehicleID partition based on lastupdate
    df_moving_south = df_moving_south.withColumn("rank", F.row_number().over(w)).filter("rank == 1").drop("rank")

    df_moving_south = df_moving_south.withColumn("lastupdate", F.from_unixtime(F.col("lastupdate")/1000).cast("timestamp"))

    return df_moving_south

def task2_alternative(df):
    # Select relevant columns
    df_selected = df.select(
        "feature.attributes.id",
        "feature.attributes.lastupdate",
        "feature.attributes.vtype",
        "feature.attributes.bearing",
    )

    cond1 = F.col('vtype') == 1 # Tram
    cond2 = F.col('vtype') == 2 # Trolleybus
    cond3 = F.col('vtype') == 3 # Bus
    cond4 = F.col('vtype') == 4 # Boat
    cond5 = F.col('vtype') == 5 # Train

    # If the vehicle is moving south, the bearing is between 135 and 225 degrees
    cond_bearing_south = F.col('bearing').between(135, 225)
    # If the vehicle is moving north, the bearing is between 315 and 45 degrees
    cond_bearing_north = F.col('bearing').between(315, 360)
    cond_bearing_north = cond_bearing_north | F.col('bearing').between(0, 45)
    # If the vehicle is moving west, the bearing is between 225 and 315 degrees
    cond_bearing_west = F.col('bearing').between(225, 315)
    # If the vehicle is moving east, the bearing is between 45 and 135 degrees
    cond_bearing_east = F.col('bearing').between(45, 135)

    # Filter
    cond_type = cond1 | cond2 | cond3 | cond4 | cond5 
    cond_south =  cond_type & cond_bearing_south
    cond_north = cond_type & cond_bearing_north
    cond_west = cond_type & cond_bearing_west
    cond_east = cond_type & cond_bearing_east

    df_filter_south = df_selected.filter(cond_south)
    df_filter_north = df_selected.filter(cond_north)
    df_filter_west = df_selected.filter(cond_west)
    df_filter_east = df_selected.filter(cond_east)

    # Define a window specification over vehicleID, ordered by lastupdate in descending order
    w = Window.partitionBy("id").orderBy(F.desc("lastupdate"))

    # Assign a rank to each record within each vehicleID partition based on lastupdate
    df_filter_south = df_filter_south.withColumn("rank", F.row_number().over(w)).filter("rank == 1").drop("rank")
    df_filter_north = df_filter_north.withColumn("rank", F.row_number().over(w)).filter("rank == 1").drop("rank")
    df_filter_west = df_filter_west.withColumn("rank", F.row_number().over(w)).filter("rank == 1").drop("rank")
    df_filter_east = df_filter_east.withColumn("rank", F.row_number().over(w)).filter("rank == 1").drop("rank")
    df_filter_south = df_filter_south.withColumn("lastupdate", F.from_unixtime(F.col("lastupdate")/1000).cast("timestamp"))
    df_filter_north = df_filter_north.withColumn("lastupdate", F.from_unixtime(F.col("lastupdate")/1000).cast("timestamp"))
    df_filter_west = df_filter_west.withColumn("lastupdate", F.from_unixtime(F.col("lastupdate")/1000).cast("timestamp"))
    df_filter_east = df_filter_east.withColumn("lastupdate", F.from_unixtime(F.col("lastupdate")/1000).cast("timestamp"))

    # Count the number of vehicles moving in each direction
    df_filter_south = df_filter_south.groupBy().count().withColumnRenamed("count", "south")
    df_filter_north = df_filter_north.groupBy().count().withColumnRenamed("count", "north")
    df_filter_west = df_filter_west.groupBy().count().withColumnRenamed("count", "west")
    df_filter_east = df_filter_east.groupBy().count().withColumnRenamed("count", "east")

    # Join the results
    result_df = df_filter_south.join(df_filter_north).join(df_filter_west).join(df_filter_east)
    return result_df

def task2(df):
    # Select relevant columns
    df_selected = df.select(
        "feature.attributes.id",
        "feature.attributes.lastupdate",
        "feature.attributes.vtype",
        "feature.attributes.lat",
        "feature.attributes.lng",
    )
    cond1 = F.col('vtype') == 1 # Tram
    cond2 = F.col('vtype') == 2 # Trolleybus
    cond3 = F.col('vtype') == 3 # Bus
    cond = cond1 | cond2 | cond3
    df_filter = df_selected.filter(cond)

    # Calculate the number of vehicles moving north, east, south and west, respectively, but take only the most recent record for each vehicle

    # If the latitude decreases the vehicle is moving south
    df_moving_south = df_filter.withColumn("moving_south", F.when(F.lag("lat").over(Window.partitionBy("id").orderBy("lastupdate")) > F.col("lat"), 1).otherwise(0))
    df_moving_south = df_moving_south.filter("moving_south == 1").drop("moving_south").drop("latitude_diff")
    # If the latitude increases the vehicle is moving north
    df_moving_north = df_filter.withColumn("moving_north", F.when(F.lag("lat").over(Window.partitionBy("id").orderBy("lastupdate")) < F.col("lat"), 1).otherwise(0))
    df_moving_north = df_moving_north.filter("moving_north == 1").drop("moving_north").drop("latitude_diff")
    # If the longitude decreases the vehicle is moving west
    df_moving_west = df_filter.withColumn("moving_west", F.when(F.lag("lng").over(Window.partitionBy("id").orderBy("lastupdate")) > F.col("lng"), 1).otherwise(0))
    df_moving_west = df_moving_west.filter("moving_west == 1").drop("moving_west").drop("longitude_diff")
    # If the longitude increases the vehicle is moving east
    df_moving_east = df_filter.withColumn("moving_east", F.when(F.lag("lng").over(Window.partitionBy("id").orderBy("lastupdate")) < F.col("lng"), 1).otherwise(0))
    df_moving_east = df_moving_east.filter("moving_east == 1").drop("moving_east").drop("longitude_diff")

    # Define a window specification over vehicleID, ordered by lastupdate in descending order
    w = Window.partitionBy("id").orderBy(F.desc("lastupdate"))

    # Assign a rank to each record within each vehicleID partition based on lastupdate
    df_moving_south = df_moving_south.withColumn("rank", F.row_number().over(w)).filter("rank == 1").drop("rank")
    df_moving_north = df_moving_north.withColumn("rank", F.row_number().over(w)).filter("rank == 1").drop("rank")
    df_moving_west = df_moving_west.withColumn("rank", F.row_number().over(w)).filter("rank == 1").drop("rank")
    df_moving_east = df_moving_east.withColumn("rank", F.row_number().over(w)).filter("rank == 1").drop("rank")

    # Count the number of vehicles moving in each direction
    df_moving_south = df_moving_south.groupBy().count().withColumnRenamed("count", "south")
    df_moving_north = df_moving_north.groupBy().count().withColumnRenamed("count", "north")
    df_moving_west = df_moving_west.groupBy().count().withColumnRenamed("count", "west")
    df_moving_east = df_moving_east.groupBy().count().withColumnRenamed("count", "east")

    # Join the results
    result_df = df_moving_south.join(df_moving_north).join(df_moving_west).join(df_moving_east)

    return result_df


def task3(df):
    # Select relevant columns
    df_selected = df.select(
        "feature.attributes.id",
        "feature.attributes.lastupdate",
        "feature.attributes.laststopid",
        "feature.attributes.ltype",
        "feature.attributes.vtype",
    )

    cond1 = F.col("ltype") == 1
    cond2 = F.col("vtype") == 1
    cond = cond1 & cond2
    df_filter = df_selected.filter(cond)

    w = Window.partitionBy("id").orderBy(F.desc('lastupdate'))
    df_last_stop = df_filter.withColumn("row_num", F.row_number().over(w)).filter("row_num == 1").drop("row_num").drop("ltype").drop("vtype")
    df_last_stop = df_last_stop.withColumnRenamed("id", "vehicle_id")
    df_last_stop = df_last_stop.withColumn('lastupdate', F.from_unixtime(F.col('lastupdate')/1000).cast('timestamp'))

    return df_last_stop

def task4(df):
    # Select relevant columns
    df_selected = df.select(
        "feature.attributes.id",
        "feature.attributes.delay",
        "feature.attributes.vtype",
    )
    cond1 = F.col('vtype') == 1
    cond2 = F.col('vtype') == 2
    cond3 = F.col('vtype') == 3
    cond = cond1 | cond2 | cond3
    df_filter = df_selected.filter(cond)

    # Define a window specification over vehicleID, ordered by delay in descending order
    w = Window.partitionBy("id").orderBy(F.desc("delay"))

    # Assign a rank to each record within each vehicleID partition based on delay
    df_delayed = df_filter.withColumn("rank", F.row_number().over(w)).filter("rank == 1")

    # Now order by delay in descending order and keep only the first 5 records
    df_delayed = df_delayed.orderBy(F.desc("delay")).limit(5).drop("rank")

    return df_delayed

def task5(df):
    # Select relevant columns
    df_selected = df.select(
        "feature.attributes.id",
        "feature.attributes.lastupdate",
        "feature.attributes.laststopid",
        "feature.attributes.vtype",
    )
    
    cond1 = F.col('vtype') == 1 # Tram
    cond2 = F.col('vtype') == 2 # Trolleybus 
    cond3 = F.col('vtype') == 3 # Bus
    cond = cond1 | cond2 | cond3
    df_filter = df_selected.filter(cond)

    w = Window.partitionBy("id").orderBy(F.desc('lastupdate'))

    # Add a column that represents the last stop for each vehicle
    df_last_stop = df_filter.withColumn("last_stop", F.first("laststopid").over(w))
    # Group the data by last_stop and collect the list of cars for each stop
    result_df = df_last_stop.groupBy("last_stop").agg(F.collect_list("id").alias("vehicle_at_stop"))

    return result_df

def task6(df):
    # Select relevant columns
    df_selected = df.select(
        "feature.attributes.id",
        "feature.attributes.delay",
        "feature.attributes.vtype",
    )
    cond1 = F.col('vtype') == 1
    cond2 = F.col('vtype') == 2
    cond3 = F.col('vtype') == 3
    cond4 = F.col('vtype') == 4 
    cond5 = F.col('vtype') == 5 
    cond = cond1 | cond2 | cond3 | cond4 | cond5
    df_filter = df_selected.filter(cond)

    # Calculate the average delay for each vehicle type
    result_df = df_filter.groupBy("vtype").agg(F.avg("delay").alias("avg_delay"))

    return result_df

# Write the results to a file
def write_to_file(df, task):
    df.write.mode("overwrite").csv("task{}.csv".format(task))

# MAIN

# Parse arguments
args = parse_args()
spark = SparkSession.builder.appName("PDI").getOrCreate()
df = spark.read.json(args.input)
df_exploded = df_preprocess(df)

# Run the tasks
#df_moving_south = task1(df_exploded)
#df_moving_south.show()
#df_moving_south_alt = task1_alternative(df_exploded)
#df_moving_south_alt.show()
#df_moving_south = df_moving_south.groupBy().count().withColumnRenamed("count", "south")
#df_moving_direction = task2(df_exploded)
#df_moving_direction.show()
df_moving_direction_alt = task2_alternative(df_exploded)
df_moving_direction_alt.show()
#df_last_stop = task3(df_exploded)
#df_last_stop.show()
#df_delayed = task4(df_exploded)
#df_delayed.show()
#df_last_stop = task5(df_exploded)
#df_last_stop.show()
#avg_delay = task6(df_exploded)
#avg_delay.show()
