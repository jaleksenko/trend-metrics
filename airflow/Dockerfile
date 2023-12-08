# Use Ubuntu 22.04 as the base image
FROM ubuntu:22.04

# Set non-interactive frontend (to avoid some prompts)
ARG DEBIAN_FRONTEND=noninteractive

# Set timezone
ENV TZ=Europe/Berlin

# Set the default umask to 002, which will create files with 664 and directories with 775 permissions
RUN echo "umask 002" >> /etc/bash.bashrc && \
    echo "export TZ=$TZ" >> /etc/bash.bashrc && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    netcat \
    telnet \
    traceroute \
    curl \
    docker.io \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create a user and group for Airflow with specified UID and GID to match the host permissions
RUN useradd -u 50000 -g 0 -m -d /opt/airflow airflow && \
    chown -R airflow:0 /opt/airflow

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.7.3
ENV PYTHON_VERSION=3.10

# Set working directory
WORKDIR /opt/airflow

# Copy requirements file
COPY requirements.txt .

# Additional Python packages including Docker Compose
RUN pip install --no-cache-dir -r requirements.txt

# Install Airflow with required extras
RUN pip install --no-cache-dir "apache-airflow[celery,redis,cncf.kubernetes]==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Expose port 8080 for Airflow webserver
EXPOSE 8080

# Switch to the airflow user
USER airflow

# Use CMD instead of ENTRYPOINT to define the default command
CMD ["airflow", "webserver"]


