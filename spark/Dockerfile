# Download base image ubuntu 22.04
FROM ubuntu:22.04

# Update Ubuntu Software repository and install packages in one layer
# Clean up the apt cache to reduce image size
RUN apt-get update && apt-get install -y \
    wget \
    openjdk-11-jdk \
    python3 \
    python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/

# Upgrade pip and install Python packages in one layer
RUN pip3 install --upgrade pip && \
    pip3 install pyspark==3.5.0 spark-nlp==5.1.4 scikit-learn==1.3.2 \
    cassandra-driver==3.28.0

# Set up working directory
WORKDIR /app/spark

# Set the default command for the container
# CMD ["tail", "-f", "/dev/null"]
CMD ["python3", "/app/spark/apps/nlp_pipeline.py"]
