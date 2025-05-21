import asyncio
import random
from typing import List, Optional
import datetime as dt
from mock_data import DataEntity, DataSource, DataLabel, ScraperId
# Create a minimal Scraper base class and ScrapeConfig
class Scraper:
    pass

class ScrapeConfig:
    def __init__(self, entity_limit=None, date_range=None, labels=None):
        self.entity_limit = entity_limit
        self.date_range = date_range
        self.labels = labels

class MockScraper(Scraper):
    def __init__(self, scraper_id: ScraperId, avg_latency_ms: int = 500, failure_rate: float = 0.05):
        self.scraper_id = scraper_id
        self.avg_latency_ms = avg_latency_ms
        self.failure_rate = failure_rate
        self.call_count = 0
        self.data_generated = 0
        
    async def scrape(self, config: ScrapeConfig) -> List[DataEntity]:
        self.call_count += 1
        
        # Simulate network latency with some randomness
        latency = random.normalvariate(self.avg_latency_ms, self.avg_latency_ms / 4) / 1000.0
        await asyncio.sleep(latency)
        
        # Simulate occasional failures
        if random.random() < self.failure_rate:
            raise Exception(f"Simulated scraping failure for {self.scraper_id}")
            
        # Generate random number of entities (1-20)
        entity_count = min(random.randint(1, 20), config.entity_limit or 20)
        self.data_generated += entity_count
        
        # Determine the correct data source based on scraper ID
        if self.scraper_id in [ScraperId.X_FLASH, ScraperId.X_MICROWORLDS, ScraperId.X_APIDOJO, ScraperId.X_QUACKER]:
            source = DataSource.X
        elif self.scraper_id in [ScraperId.REDDIT_LITE, ScraperId.REDDIT_CUSTOM]:
            source = DataSource.REDDIT
        else:
            source = DataSource.YOUTUBE
            
        # Create a label
        label = DataLabel(value=f"mock_label_{self.scraper_id}")
        
        return [
            DataEntity(
                uri=f"mock://data/{self.scraper_id}/{self.call_count}/{i}",
                datetime=dt.datetime.now(dt.timezone.utc),
                source=source,
                label=label,
                content=f"Mock content from {self.scraper_id} scraper #{i}".encode('utf-8'),
                content_size_bytes=random.randint(100, 10000)
            )
            for i in range(entity_count)
        ]