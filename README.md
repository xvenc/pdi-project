# Processing the IDSJMK transport dataset using Apache Spark

**Author:** Václav Korvas (xkorva03)

This repository contains the code for processing the IDSJMK transport dataset using Apache Spark. The dataset is available at [https://data.brno.cz/datasets/public-transit-positional-data/about](https://data.brno.cz/datasets/mestobrno::polohy-vozidel-hromadn%C3%A9-dopravy-public-transit-positional-data/about).

**Disclaimer:** If you download the dataset, the test will not work, because the tests are written for my dataset, which is in the repo.

## Requirements

* Docker 24.0.7 
* python 3.10.13
* pip 23.2.1 or conda 22.11.1

## Building

This project uses pyspark, which is not available in the default conda repository. Therefore, it is necessary to create a new conda environment and install pyspark using pip:

```
$ conda create -n spark-env python=3.10.13
$ conda activate spark-env
$ pip install pyspark
```
The pyspark is under license Apache License 2.0.

Next it is necessary to have the dataset downloaded localy. In the repo is my dataset (ODAE.json), which I used and the tests are written for it. 

If you want to obtain your own dataset, you can use the following command:

```
curl 'https://gis.brno.cz/ags1/rest/services/Hosted/ODAE_public_transit_positional_feature_service/FeatureServer/0/query?outFields=id,vtype,ltype,lat,lng,bearing,lineid,linename,routeid,course,lf,delay,laststopid,finalstopid,isinactive,lastupdate,objectid,globalid&where=1%3D1&f=json' > <my_dataset>.json
```

## Implementation details

All the implementation is done in `spark.py`. 

General implementation details:

* As vehicle are considered all types eg. trains, buses, troleybuses, trams and boats ex and also business line / passenger-free rides. And these types where determined by the column `vtype`.
* Because the dataset can contain multiple records for one vehicleID, the records are sorted by the `lastupdate` field and only the last record or last 2 records are considered, based on the task(except for the last task).
* All outputs are stored in json files in the `logs` folder. The name of the file is the name of the task. The files are overwritten every time the program is run.

Now the implementation details to each task:

* Task 1: The task is implemented in the function `task1()`. I considered all types of vehicles. The function returns number of vehicles that are going south with deviation of 45 degrees. I took the float data from column `bearing` and checked if it is in the range of 135-225 degrees. But there is no additional explanation of the column `bearing` in the dataset, so I am not sure if it is the right way to do it. But i assumed that 0 degrees is north, 90 degrees is east, 180 degrees is south and 270 degrees is west. I only considered the last record of each vehicleID.
* Task 2: The task is implemented in the function `task2()`. I again used the column `bearing`. And if the value was between 135-225 the vehicle is moving south, if 315-45 is moving north, if 225-315 is moving west and if 45-135 is moving east. I only considered the last record of each vehicleID. 
* Task 3: I only considered the column `vtype` equal to 1. And again I only considered the last record of each vehicleID. The task is implemented in the function `task3()`. 
* Task 4: In this task in the output there can be more than one vehicle with the same `vehicleID`, if they have the biggest delays.
* Task 5: In this task again, I take only tha last records for each vehicleID as relevant.
* Task 6: I take all the data from the whole dataset, so if there are more records for one vehicleID, I take all of them. The task is implemented in the function `task6()`.