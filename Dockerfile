# Use a base image with spark
From apache/spark

# Set the working directory 
WORKDIR /opt/spark

# Copy the spark app file to the container
COPY spark.py /opt/spark
COPY *.json /opt/spark
COPY run.sh /opt/spark

# Set environment variables for Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN ./run.sh > spark_out.txt
