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