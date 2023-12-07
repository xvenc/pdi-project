# Python tests for Apache spark project

## Requirements

- unittest module (should be installed by default)
- filecmp module (should be installed by default)

## How to run the tests

**Disclaimer:** The tests are written for the dataset provided in the project folder. Because if you download newer dataset the tests will fail.

Before running the test you need to run the `spark.py` file to generate the output files. To do that just simply run this command in the terminal command line:
```
$ python spark.py --input ODAE.json
```

To run the test just simply run this command in the terminal command line:
```
$ python -m unittest test.py
```