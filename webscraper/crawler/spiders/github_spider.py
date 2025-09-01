import scrapy
import time
import os
from dotenv import load_dotenv

load_dotenv()

class GitHubSpider(scrapy.Spider):
    name="github"
    allowed_domains=["api.github.com"]

    topics=["machine-learning", "ai", "deep-learning"]
    per_page=10
    max_pages=5

    github_token = os.getenv("GITHUB_TOKEN")
    headers = {"Authorization": f"token {github_token}"} if github_token else {}

    def start_requests(self):
        for topic in self.topics:
            for page in range(1, self.max_pages + 1):
                url = (
                    f"https://api.github.com/search/repositories?"
                    f"q=topic:{topic}&sort=updated&order=desc&per_page={self.per_page}&page={page}"
                )
                yield scrapy.Request(url=url, callback=self.parse, meta={"topic": topic, "page": page})

    def parse(self, response):
        try:
            data = response.json()
            items = data.get("items", [])
        except Exception as e:
            self.logger.error(f"Failed to parse JSON from {response.url}.\n{e}")
            return
        
        for repo in items:
            item = {
                "id": repo["full_name"],  # unique GitHub repo identifier
                "title": repo["name"],
                "abstract": repo.get("description") or "",
                "published": repo["created_at"],
                "updated": repo["updated_at"],
                "authors": [repo["owner"]["login"]],
                "link": repo["html_url"],
                "source_category": "github"
            }
            yield item

            # GitHub Rate Limiting
            remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
            if remaining == 0:
                reset = int(response.headers.get("X-RateLimit-Reset". time.time()+60))
                wait = reset - int(time.time()) + 1
                self.logger.info(f"Rate limit hit. Waiting {wait} seconds..")
                time.sleep(wait)