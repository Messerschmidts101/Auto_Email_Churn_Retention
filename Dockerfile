FROM ubuntu:22.04

# Install Java 17, Python 3.10+, and system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    python3 python3-pip python3-venv \
    build-essential \
    git \
    curl wget \
    gfortran libopenblas-dev liblapack-dev \
    llvm && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV PYSPARK_PYTHON=python3

WORKDIR /app
COPY . /app
RUN rm -rf venv

# Install Python dependencies
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 8000
CMD ["python3", "server.py"]