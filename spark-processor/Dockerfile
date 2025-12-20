FROM python:3.13-slim

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-21-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for the container
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set SPARK_HOME to where pyspark is installed via pip
ENV SPARK_HOME=/usr/local/lib/python3.13/site-packages/pyspark
ENV PYTHONPATH=/app

# Copy application code
COPY src/ ./src/
COPY tests/ ./tests/

CMD ["python", "-m", "src.processor"]