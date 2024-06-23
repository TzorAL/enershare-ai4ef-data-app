# Use an official Python runtime as a parent image
FROM python:3.10.12-slim

# Set the working directory in the container
WORKDIR /ds_viz/
ENV DAGSTER_HOME=/ds_viz/

# Copy the current directory contents into the container at /ds_viz
COPY . /ds_viz
COPY . ./ai4ef_data_app

# Install build-essential and other necessary system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*
    
# Install any needed packages specified in requirements.txt and cleanup temp files
# RUN apt update && apt upgrade
RUN pip install --no-cache-dir -r python_requirements.txt \
    && rm -rf /var/cache/apk/* /tmp/*