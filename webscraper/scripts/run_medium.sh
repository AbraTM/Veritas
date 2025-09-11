#!/bin/sh

while true; do
    echo "Running Medium Spider $(date)"
    scrapy crawl medium
    echo "Medium Spider finished, sleeping for 1 hour..."
    sleep 3600
done