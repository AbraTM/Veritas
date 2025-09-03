#!/bin/sh

while true; do
    echo "Running Arxiv Spider at $(date)"
    scrapy crawl arxiv
    echo "Arxiv Spider finished, sleeping for 1 hour..."
    sleep 3600
done