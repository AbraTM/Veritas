#!/bin/sh

while true; do 
    echo "Running GitHub Spider $(date)"
    scrapy crawl github
    echo "GitHub Spider finished, sleeping for 1 hour..."
    sleep 3600
done