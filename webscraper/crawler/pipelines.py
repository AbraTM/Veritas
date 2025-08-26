import json
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err:
        print(f"Delivery Failed: {err}")
    else:
        print(f"Message Delivered to {msg.topic()} [{msg.partition()}]")

class KafkaPipeline:
    def __init__(self, kafka_bootstrap_servers, kafka_topic):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.producer = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            kafka_bootstrap_servers = crawler.settings.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic = crawler.settings.get("KAFKA_TOPIC", "veritas-pages")
        )

    def open_spider(self, spider):
        conf = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "client.id": "scrapy-producer"
        }
        self.producer = Producer(conf)
    
    def close_spider(self, spider):
        if self.producer:
            self.producer.flush()

    def process_item(self, item, spider):
        try:
            self.producer.produce(
                topic=self.kafka_topic,
                key=spider.name.encode("utf-8"),
                value=json.dumps(dict(item)).encode("utf-8"),
                callback=delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            spider.logger.error(f"Kafka Produce Error: {e}")
        return item

    