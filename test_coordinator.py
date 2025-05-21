import asyncio
import time
import datetime as dt
import os
import sys
from typing import Dict, List, Optional
from mock_bittensor import logging as bt_logging
from mock_scraper import MockScraper, ScrapeConfig
from mock_storage import MockMinerStorage
from mock_data import ScraperId, DataLabel, DateRange, TimeBucket

# Create mock classes for Coordinator functionality
class ScraperProvider:
    def get(self, scraper_id):
        pass

class LabelScrapingConfig:
    def __init__(self, label_choices=None, max_age_hint_minutes=60, max_data_entities=None):
        self.label_choices = label_choices
        self.max_age_hint_minutes = max_age_hint_minutes
        self.max_data_entities = max_data_entities

class ScraperConfig:
    def __init__(self, cadence_seconds, labels_to_scrape):
        self.cadence_seconds = cadence_seconds
        self.labels_to_scrape = labels_to_scrape

class CoordinatorConfig:
    def __init__(self, scraper_configs):
        self.scraper_configs = scraper_configs

# Mock the bittensor module
import types
sys.modules['bittensor'] = types.ModuleType('bittensor')
sys.modules['bittensor'].logging = bt_logging

class MockScraperProvider(ScraperProvider):
    def __init__(self, scrapers: Dict[ScraperId, MockScraper]):
        self.scrapers = scrapers
    
    def get(self, scraper_id: ScraperId):
        return self.scrapers.get(scraper_id)

# Create a simplified ScraperCoordinator class
class ScraperCoordinator:
    def __init__(self, provider, storage, config):
        self.provider = provider
        self.storage = storage
        self.config = config
        self.max_workers = 5
        self.is_running = True
        self.queue = asyncio.Queue()
        
    async def _worker(self, name):
        """A worker thread"""
        while self.is_running:
            try:
                # Wait for a scraping task to be added to the queue.
                scrape_fn = await self.queue.get()

                # Perform the scrape
                data_entities = await scrape_fn()

                self.storage.store_data_entities(data_entities)
                self.queue.task_done()
            except Exception as e:
                bt_logging.error("Worker " + name + ": " + str(e))
    
    async def _start(self):
        workers = []
        for i in range(self.max_workers):
            worker = asyncio.create_task(
                self._worker(
                    f"worker-{i}",
                )
            )
            workers.append(worker)
            
        # Simple simulation of periodic task scheduling
        while self.is_running:
            # Simulate scheduling tasks
            now = dt.datetime.now(dt.timezone.utc)
            for scraper_id, scraper_config in self.config.scraper_configs.items():
                scraper = self.provider.get(scraper_id)
                
                for label_config in scraper_config.labels_to_scrape:
                    # Choose a date range
                    max_age_minutes = label_config.max_age_hint_minutes
                    start_time = now - dt.timedelta(minutes=max_age_minutes)
                    date_range = DateRange(start=start_time, end=now)
                    
                    # Create a scrape config
                    config = ScrapeConfig(
                        entity_limit=label_config.max_data_entities,
                        date_range=date_range,
                        labels=label_config.label_choices
                    )
                    
                    # Add to queue
                    self.queue.put_nowait(lambda c=config, s=scraper: s.scrape(c))
            
            # Wait before scheduling again
            await asyncio.sleep(5)
            
        # Wait for all workers to finish
        for worker in workers:
            worker.cancel()
        
        await asyncio.gather(*workers, return_exceptions=True)
    
    def stop(self):
        self.is_running = False

async def run_test(duration_seconds: int = 60, worker_count: int = 5, 
                  use_batching: bool = False, batch_size: int = 20,
                  batch_interval_seconds: int = 2):
    """Run a test of the coordinator for a specified duration"""
    # Create mock scrapers with different latency profiles
    scrapers = {
        ScraperId.X_FLASH: MockScraper(ScraperId.X_FLASH, avg_latency_ms=300),
        ScraperId.REDDIT_LITE: MockScraper(ScraperId.REDDIT_LITE, avg_latency_ms=800),
        ScraperId.YOUTUBE_TRANSCRIPT: MockScraper(ScraperId.YOUTUBE_TRANSCRIPT, avg_latency_ms=1200),
    }
    
    # Create mock storage
    storage = MockMinerStorage()
    
    # Create mock provider
    provider = MockScraperProvider(scrapers)
    
    # Create coordinator config
    config = CoordinatorConfig(
        scraper_configs={
            ScraperId.X_FLASH: ScraperConfig(
                cadence_seconds=5,
                labels_to_scrape=[
                    LabelScrapingConfig(
                        label_choices=[DataLabel(value="politics")],
                        max_age_hint_minutes=60,
                        max_data_entities=15
                    )
                ]
            ),
            ScraperId.REDDIT_LITE: ScraperConfig(
                cadence_seconds=10,
                labels_to_scrape=[
                    LabelScrapingConfig(
                        label_choices=[DataLabel(value="technology")],
                        max_age_hint_minutes=120,
                        max_data_entities=10
                    )
                ]
            ),
            ScraperId.YOUTUBE_TRANSCRIPT: ScraperConfig(
                cadence_seconds=15,
                labels_to_scrape=[
                    LabelScrapingConfig(
                        label_choices=[DataLabel(value="transcripts")],
                        max_age_hint_minutes=180,
                        max_data_entities=5
                    )
                ]
            )
        }
    )
    
    # Create coordinator with custom worker count
    coordinator = ScraperCoordinator(provider, storage, config)
    coordinator.max_workers = worker_count
    
    # Setup batch processing if enabled
    batch_processor_task = None
    entity_batch = []
    batch_lock = asyncio.Lock()
    
    async def batch_processor():
        nonlocal entity_batch
        while coordinator.is_running:
            async with batch_lock:
                if len(entity_batch) >= batch_size:
                    storage.store_data_entities_batch(entity_batch)
                    print(f"Batch processed {len(entity_batch)} entities")
                    entity_batch = []
            
            # Sleep between batch checks
            await asyncio.sleep(batch_interval_seconds)
            
            # Process any remaining items before shutting down
            if not coordinator.is_running and entity_batch:
                async with batch_lock:
                    if entity_batch:  # Check again in case it was processed
                        storage.store_data_entities_batch(entity_batch)
                        print(f"Final batch processed {len(entity_batch)} entities")
                        entity_batch = []
    
    # If batching is enabled, patch the storage method in the worker
    if use_batching:
        original_worker = coordinator._worker
        
        async def patched_worker(name):
            nonlocal entity_batch
            while coordinator.is_running:
                try:
                    # Wait for a scraping task to be added to the queue.
                    scrape_fn = await coordinator.queue.get()

                    # Perform the scrape
                    data_entities = await scrape_fn()
                    
                    # Store in batch instead of directly
                    async with batch_lock:
                        entity_batch.extend(data_entities)
                    
                    coordinator.queue.task_done()
                except Exception as e:
                    bt_logging.error("Worker " + name + ": " + str(e))
        
        # Replace the worker method
        coordinator._worker = patched_worker
    
    # Start coordinator
    start_time = time.time()
    print(f"Starting test with {worker_count} workers for {duration_seconds} seconds")
    print(f"Batching: {'enabled' if use_batching else 'disabled'}")
    
    if use_batching:
        batch_processor_task = asyncio.create_task(batch_processor())
    
    # Create a task to stop the test after the specified duration
    async def stop_after_duration():
        await asyncio.sleep(duration_seconds)
        coordinator.stop()
    
    stop_task = asyncio.create_task(stop_after_duration())
    coordinator_task = asyncio.create_task(coordinator._start())
    
    # Wait for the coordinator task to complete
    await asyncio.gather(coordinator_task, stop_task)
    
    if batch_processor_task:
        await batch_processor_task
    
    # Calculate metrics
    elapsed = time.time() - start_time
    total_scrapes = sum(s.call_count for s in scrapers.values())
    total_entities = sum(s.data_generated for s in scrapers.values())
    
    print(f"\nTest Results (duration: {elapsed:.2f}s):")
    print(f"Total scrapes: {total_scrapes} ({total_scrapes/elapsed:.2f}/s)")
    print(f"Total entities: {total_entities} ({total_entities/elapsed:.2f}/s)")
    print(f"Storage writes: {storage.write_count} ({storage.write_count/elapsed:.2f}/s)")
    
    for scraper_id, scraper in scrapers.items():
        print(f"{scraper_id}: {scraper.call_count} scrapes, {scraper.data_generated} entities")
    
    return {
        "duration": elapsed,
        "total_scrapes": total_scrapes, 
        "scrapes_per_second": total_scrapes/elapsed,
        "total_entities": total_entities,
        "entities_per_second": total_entities/elapsed,
        "storage_writes": storage.write_count,
        "storage_writes_per_second": storage.write_count/elapsed,
        "storage_stats": storage.get_stats(),
        "batching_enabled": use_batching,
        "worker_count": worker_count,
        "scrapers": {str(id): {"calls": s.call_count, "entities": s.data_generated} for id, s in scrapers.items()}
    }

# Function to test different worker counts
async def test_worker_scaling(duration_per_test=30):
    results = []
    for worker_count in [1, 2, 5, 10, 20, 50]:
        print(f"\n=== Testing with {worker_count} workers ===")
        result = await run_test(duration_seconds=duration_per_test, worker_count=worker_count)
        results.append((worker_count, result))
        await asyncio.sleep(2)  # Brief pause between tests
    
    # Print comparison
    print("\nWorker Scaling Results:")
    print("Worker Count | Scrapes/s | Entities/s | Writes/s")
    print("------------|-----------|------------|--------")
    for workers, result in results:
        print(f"{workers:11d} | {result['scrapes_per_second']:9.2f} | {result['entities_per_second']:10.2f} | {result['storage_writes_per_second']:7.2f}")
    
    return results

# Function to test batching vs non-batching
async def test_batching(duration_per_test=30, worker_count=10):
    print("\n=== Testing without batching ===")
    no_batch_result = await run_test(
        duration_seconds=duration_per_test, 
        worker_count=worker_count,
        use_batching=False
    )
    
    print("\n=== Testing with batching ===")
    batch_result = await run_test(
        duration_seconds=duration_per_test, 
        worker_count=worker_count,
        use_batching=True,
        batch_size=20,
        batch_interval_seconds=2
    )
    
    # Print comparison
    print("\nBatching Results:")
    print("Mode        | Scrapes/s | Entities/s | Writes/s | Avg Write Time (ms)")
    print("------------|-----------|------------|----------|------------------")
    print(f"No Batching | {no_batch_result['scrapes_per_second']:9.2f} | {no_batch_result['entities_per_second']:10.2f} | {no_batch_result['storage_writes_per_second']:7.2f} | {no_batch_result['storage_stats']['avg_write_time']*1000:18.2f}")
    print(f"Batching    | {batch_result['scrapes_per_second']:9.2f} | {batch_result['entities_per_second']:10.2f} | {batch_result['storage_writes_per_second']:7.2f} | {batch_result['storage_stats']['avg_write_time']*1000:18.2f}")
    
    return {
        "no_batching": no_batch_result,
        "batching": batch_result
    }

async def run_all_tests():
    """Run a complete set of tests and return all results"""
    print("=== WORKER SCALING TESTS ===")
    scaling_results = await test_worker_scaling(duration_per_test=20)
    
    print("\n=== BATCHING TESTS ===")
    batching_results = await test_batching(duration_per_test=30)
    
    return {
        "scaling": scaling_results,
        "batching": batching_results
    }

if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Test the scraping coordinator")
    parser.add_argument("--test", choices=["single", "workers", "batching", "all"], default="single",
                      help="Test to run: single, workers, batching, or all")
    parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds")
    parser.add_argument("--workers", type=int, default=10, help="Number of workers for single tests")
    parser.add_argument("--batching", action="store_true", help="Enable batching for single test")
    args = parser.parse_args()
    
    if args.test == "single":
        asyncio.run(run_test(
            duration_seconds=args.duration, 
            worker_count=args.workers,
            use_batching=args.batching
        ))
    elif args.test == "workers":
        asyncio.run(test_worker_scaling(duration_per_test=args.duration))
    elif args.test == "batching":
        asyncio.run(test_batching(duration_per_test=args.duration, worker_count=args.workers))
    elif args.test == "all":
        asyncio.run(run_all_tests())