## Running

If you want to run the code locally, you need to have the dataset in the same folder as the code. Then you can run the code using the following command:

```
python main.py --input <my_dataset>.json
```

By default it will use the **ODAE.json** dataset.

If you want to run the same code on cluster, you need to do the following steps:

1. Build the docker image using the following command:

```
docker build -t spark .
```

2. Run the docker container to copy the output files from the container to the local machine:

```
docker create --name temp_container spark
docker cp temp_container:/opt/spark/logs .
docker rm temp_container
```

Now you should be able to see the output files in the folder named logs.
Each task is in it's own folder, and the name of the folder is the task{id}.
