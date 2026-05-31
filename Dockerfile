# Use Jupyter PySpark Notebook as base image
FROM jupyter/pyspark-notebook

# Copy requirements file
COPY requirements.txt /tmp/

# Install dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create directory for JDBC driver
RUN mkdir -p /usr/local/spark/jars/

# Copy PostgreSQL JDBC driver
COPY driver/postgresql-42.6.0.jar /usr/local/spark/jars/
