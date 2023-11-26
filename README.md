# Processing the IDSJMK transport dataset using Apache Spark

**Author:** Václav Korvas (xkorva03)

This repository contains the code for processing the IDSJMK transport dataset using Apache Spark. The dataset is available at [https://data.brno.cz/datasets/public-transit-positional-data/about](https://data.brno.cz/datasets/mestobrno::polohy-vozidel-hromadn%C3%A9-dopravy-public-transit-positional-data/about).

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
## Running

If you want to run the code locally, you need to have the dataset in the same folder as the code. Then you can run the code using the following command:

```
$ python main.py --input <my_dataset>.json
```
By default it will use the **ODAE.json** dataset.

If you want to run the same code on cluster, you need to do the following steps:

1. Build the docker image using the following command:

```
$ docker build -t spark .
```

2. Run the docker container to copy the output files from the container to the local machine:

```
$ docker run -w /opt/spark -it --rm -v $(pwd):/opt/spark spark
```

Now you should be able to see the output files in the same folder as the code.
The output file name is spark_out.txt.