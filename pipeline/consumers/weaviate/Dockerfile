# Base Python Image
FROM python:3.11-slim

# Working Directory in the Container
WORKDIR /weaviate

# Python settings to prevent python from writing .pyc files and buffer output
# (i.e see print statments as they are executed, instead of waiting for a buffer)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/weaviate

# Copying requirements.txt to install the required packages and the base_consumer
COPY weaviate/ /weaviate
COPY base_consumer.py /weaviate/consumers

# RUN Command to install the packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Run Command to install minimal sentence-tarnsformer resources
RUN pip install --no-cache-dir sentence-transformers==5.1.0 --no-deps && \
    pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir transformers tokenizers
