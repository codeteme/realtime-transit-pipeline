FROM apache/airflow:2.10.2-python3.12

USER root

# Install OS dependencies including Java
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Find and set Java home dynamically
RUN JAVA_BIN=$(which java) && \
    JAVA_HOME=$(readlink -f $JAVA_BIN | sed "s:/bin/java::") && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile.d/java.sh && \
    echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment

USER airflow

COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Set JAVA_HOME for airflow user
ENV JAVA_HOME=/usr/lib/jvm/default-java