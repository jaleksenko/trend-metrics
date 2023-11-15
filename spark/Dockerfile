# Download base image ubuntu 22.04
FROM ubuntu:22.04

# Update Ubuntu Software repository
RUN apt-get update && apt-get install -y \
    wget \
    openjdk-11-jdk \
    python3 \
    python3-pip

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Upgrade pip and install Python packages
RUN pip3 install --upgrade pip && \
    pip3 install pyspark==3.5.0 spark-nlp==5.1.4 scikit-learn==1.3.2

# Add a user with less privileges
ENV NB_USER spark
ENV NB_UID 1000
ENV HOME /home/${NB_USER}
RUN useradd -m -s /bin/bash -N -u ${NB_UID} ${NB_USER}

# Set up working directory
WORKDIR ${HOME}
USER ${NB_USER}

# Copy your application code with appropriate permissions
COPY --chown=${NB_USER} ./apps /home/${NB_USER}/apps
COPY --chown=${NB_USER} ./data /home/${NB_USER}/data

# The default command that runs when the container starts
# CMD ["tail", "-f", "/dev/null"]
CMD ["/usr/local/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4", "/home/spark/apps/start.py"]

