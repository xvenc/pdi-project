from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# TODO argument parsing

def task1(df):
    # Select relevant columns
    df_selected = df.select(
        "feature.attributes.id",
        "feature.attributes.lastupdate",
        "feature.attributes.vtype",
        "feature.attributes.lat",
    )
    cond1 = F.col('vtype') == 1 # Tram
    cond2 = F.col('vtype') == 2 # Trolleybus
    cond3 = F.col('vtype') == 3 # Bus
    cond = cond1 | cond2 | cond3
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

    df_delayed = df_filter.orderBy(F.desc('delay')) 

    # Add a row number to the DataFrame
    df_delayed = df_delayed.withColumn("row_num", F.row_number().over(Window.orderBy(F.desc("delay"))))

    # Select up to 5 delayed wagons
    df_delayed = df_delayed.filter("row_num <= 5").drop("row_num")
    # Rename columns
    df_delayed = df_delayed.withColumnRenamed("id", "vehicle_id")

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

# MAIN
spark = SparkSession.builder.appName("PDI").getOrCreate()
df = spark.read.json("ODAE.json")
#df_selected.printSchema()

# Explode the 'features' array column to handle the nested struct
df_exploded = df.select(
    F.explode("features").alias("feature"),
)
df_exploded = df_exploded.filter(df_exploded.feature.attributes.isinactive == 'false')

df_moving_south = task1(df_exploded)
#df_last_stop = task3(df_exploded)
#df_delayed = task4(df_exploded)
#df_last_stop = task5(df_exploded)
#avg_delay = task6(df_exploded)