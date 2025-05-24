# Use Python 3.11 base image
FROM python:3.11-slim

# Install Java (required by PySpark) and utilities
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl \
    unzip \
    && apt-get clean

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Hadoop (for PySpark compatibility, not to run)
ENV HADOOP_VERSION=3.3.1
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV PATH="${HADOOP_HOME}/bin:${PATH}"

# Set work directory and copy project files
WORKDIR /app
COPY . .

# Install dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Default command
CMD ["python", "server.py"]
