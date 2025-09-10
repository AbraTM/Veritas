# Base Python Image
FROM python:3.11-slim

# Working Directory in the Container
WORKDIR /postgres

# Python settings to prevent python from writing .pyc files and buffer output
# (i.e see print statments as they are executed, instead of waiting for a buffer)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/postgres

# Copying requirements.txt to install the required packages and the base_consumer
COPY postgres/ /postgres
COPY base_consumer.py /postgres/consumers

# RUN Command to install the packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
