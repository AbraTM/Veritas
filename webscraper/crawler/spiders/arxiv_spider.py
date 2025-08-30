import scrapy
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

class ArxivSpider(scrapy.Spider):
    name = "arxiv"
    # So if throught a link the spider goes to different webpage it will stop
    allowed_domains = ["export.arxiv.org"]

    base_url = "http://export.arxiv.org/api/query?"

    # Crawl Categories
    categories = ["cs.AI", "cs.LG", "stat.ML"]

    results_per_page = 10 # Number of results per request
    max_results = 50 # Max Results

    def start_requests(self):
        for category in self.categories:
            for start in range(0, self.max_results, self.results_per_page):
                params = {
                    "search_query": f"cat:{category}",
                    "start": start,
                    "max_results": self.results_per_page,
                    "sortBy": "submittedDate",
                    "sortOrder": "descending"
                }

                url = self.base_url + urlencode(params)
                yield scrapy.Request(url=url, callback=self.parse, meta={ "category": category })

    def parse(self, response):
        print(response)
        root = ET.fromstring(response.text)

        # XML Namespace
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        for entry in root.findall("atom:entry", ns):
            link = next(
                (l.attrib["href"] for l in entry.findall("atom:link", ns) if l.attrib.get("rel") == "alternate"),
                None
            )
            item = {
                "id": entry.find("atom:id", ns).text,
                "title": entry.find("atom:title", ns).text.strip(),
                "abstract": entry.find("atom:summary", ns).text.strip(),
                "published": entry.find("atom:published", ns).text,
                "updated": entry.find("atom:updated", ns).text,
                "authors": [ author.find("atom:name", ns).text for author in entry.findall("atom:author", ns)],
                "link": link,
                "source_category": "arxiv"
            }
            yield item