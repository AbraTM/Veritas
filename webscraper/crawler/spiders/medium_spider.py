import scrapy
import feedparser
from w3lib.html import remove_tags

class MediumSpider(scrapy.Spider):
    name = "medium"
    allowed_domains = ["medium.com"]
    tags = [
        "artificial-intelligence",
        "machine-learning",
        "deep-learning",
        "nlp",
        "computer-vision",
        "reinforcement-learning",
        "cybersecurity",
        "software-engineering",
        "data-structures",
        "algorithms",
        "distributed-systems",
        "blockchain",
        "programming"
    ]
    authors = [
        "towardsdatascience",
        "deeplearningai",
        "analyticsvidhya",
        "hackernoon"
    ]

    rss_feeds = [
        f"https://medium.com/feed/tag/{tag}" for tag in tags
    ] + [
        f"https://medium.com/feed/@{author}" for author in authors
    ]

    def start_requests(self):
        for feed in self.rss_feeds:
            yield scrapy.Request(url=feed, callback=self.parse)

    
    def parse(self, response):
        feed = feedparser.parse(response.text)
        for entry in feed.entries:
            item = {
                "id": entry.get("id", entry.link),
                "title": entry.title,
                "abstract": remove_tags(entry.summary),
                "published": entry.published,
                "updated": entry.published,
                "authors": [entry.author] if hasattr(entry, "author") else [],
                "link": entry.link,
                "source_category": "medium"
            }
            yield item