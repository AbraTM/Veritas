# Veritas: Open Knowledge Graph for Science & Tech

Veritas is a research-oriented data pipeline designed to **crawl, process, and organize scientific and technical content**.  
It collects data from multiple sources (e.g., ArXiv, GitHub, RSS), stores structured metadata in **Postgres**, builds relationships in **Neo4j**, and indexes semantic embeddings in a **Vector Database** for intelligent search and RAG workflows.  

---

## 
- **Web Crawling**: Scrapy spiders to extract data from research and tech sources.
- **Kafka Streaming**: Producers send scraped data to Kafka topics.
- **Unified Consumer**: A single pipeline consumer processes, enriches, and stores all data.
- **Multi-DB Storage**:
  - Postgres for structured metadata
  - Neo4j for entity relationships
  - Vector DB for semantic search
- **Future-Proof API**: Exposed via REST or MCP server for LLM integration.

---