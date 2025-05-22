import json
import uuid
import datetime as dt
from typing import Dict, List, Optional, Any
import aioredis
import bittensor as bt
from common.data import DataEntity, DataSource, DataLabel, DateRange

class GoScraperClient:
    """
    Client for interacting with the Go scraper microservice via Redis queue.
    
    This client sends scraping requests to a Redis queue for the Go service to consume.
    It doesn't wait for responses, implementing a fire-and-forget pattern for asynchronous processing.
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        queue_name: str = "scrape_queue"
    ):
        """
        Initialize the Go scraper client.
        
        Args:
            redis_url: Redis connection URL
            queue_name: Redis list name used as a queue for scrape jobs
        """
        self.redis_url = redis_url
        self.queue_name = queue_name
        self.redis = None
        self._connected = False
    
    async def connect(self) -> None:
        """Connect to Redis if not already connected."""
        if not self._connected:
            try:
                # Use the new Redis client API if available (aioredis >= 2.0)
                try:
                    from redis.asyncio import Redis
                    self.redis = Redis.from_url(self.redis_url)
                except ImportError:
                    # Fallback to older API
                    self.redis = await aioredis.create_redis_pool(self.redis_url)
                
                self._connected = True
                bt.logging.info(f"Connected to Redis at {self.redis_url}")
            except Exception as e:
                bt.logging.error(f"Failed to connect to Redis: {e}")
                raise
    
    async def close(self) -> None:
        """Close the Redis connection."""
        if self._connected and self.redis:
            # Handle different Redis client versions
            if hasattr(self.redis, "close"):
                self.redis.close()
                if hasattr(self.redis, "wait_closed"):
                    await self.redis.wait_closed()
            elif hasattr(self.redis, "aclose"):
                await self.redis.aclose()
            
            self._connected = False
            bt.logging.info("Disconnected from Redis")
    
    async def enqueue_scrape_job(
        self,
        scraper_id: str,
        date_range: DateRange,
        labels: Optional[List[DataLabel]] = None,
        entity_limit: Optional[int] = None,
        callback_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Enqueue a scrape job to the Redis queue.
        
        Args:
            scraper_id: ID of the scraper to use
            date_range: Date range to scrape
            labels: Optional list of data labels
            entity_limit: Maximum number of entities to return
            callback_info: Optional information for the consumer to use for callbacks
            
        Returns:
            The job ID that was generated for this scrape request
            
        Raises:
            ConnectionError: If there is an issue with the Redis connection
        """
        # Ensure connected
        if not self._connected:
            await self.connect()
        
        # Generate a unique job ID
        job_id = str(uuid.uuid4())
        
        # Prepare the job
        job = {
            "job_id": job_id,
            "scraper_id": scraper_id,
            "enqueued_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "config": {
                "entity_limit": entity_limit,
                "date_range": {
                    "start": date_range.start.isoformat(),
                    "end": date_range.end.isoformat()
                },
                "labels": [{"value": label.value} for label in (labels or [])]
            }
        }
        
        # Add callback info if provided
        if callback_info:
            job["callback_info"] = callback_info
        
        # Send the job to the queue
        try:
            await self._push_to_queue(job)
            bt.logging.info(f"Enqueued scrape job {job_id} for {scraper_id}")
            return job_id
        except Exception as e:
            bt.logging.error(f"Failed to enqueue scrape job: {e}")
            raise ConnectionError(f"Failed to enqueue scrape job: {e}")
    
    async def _push_to_queue(self, job: Dict[str, Any]) -> None:
        """Push a job to the Redis queue."""
        encoded_job = json.dumps(job)
        
        # Different Redis client versions have different APIs
        if hasattr(self.redis, "rpush"):
            await self.redis.rpush(self.queue_name, encoded_job)
        else:
            await self.redis.execute_command("RPUSH", self.queue_name, encoded_job)


class BatchScraperClient:
    """
    Enhanced client that supports batched operations for improved efficiency.
    
    This client can enqueue multiple scrape jobs in parallel,
    reducing the overhead of multiple Redis round-trips.
    """
    
    def __init__(
        self,
        base_client: Optional[GoScraperClient] = None,
        redis_url: str = "redis://localhost:6379",
        queue_name: str = "scrape_queue",
        max_batch_size: int = 20
    ):
        """
        Initialize the batch scraper client.
        
        Args:
            base_client: Optional existing GoScraperClient to use
            redis_url: Redis connection URL
            queue_name: Redis list name used as a queue for scrape jobs
            max_batch_size: Maximum number of scrape jobs to enqueue in parallel
        """
        self.client = base_client or GoScraperClient(
            redis_url=redis_url,
            queue_name=queue_name
        )
        self.max_batch_size = max_batch_size
    
    async def connect(self) -> None:
        """Connect the underlying client to Redis."""
        await self.client.connect()
    
    async def close(self) -> None:
        """Close the underlying client's Redis connection."""
        await self.client.close()
    
    async def enqueue_batch(
        self,
        scrape_configs: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Enqueue multiple scrape jobs in parallel.
        
        Args:
            scrape_configs: List of scrape configurations, each containing:
                - scraper_id: ID of the scraper to use
                - date_range: DateRange object to scrape
                - labels: Optional list of DataLabel objects
                - entity_limit: Maximum number of entities to return
                - callback_info: Optional callback information
            
        Returns:
            List of job IDs in the same order as the scrape_configs.
        """
        # Ensure connected
        await self.connect()
        
        # Process in chunks to avoid overwhelming the system
        all_job_ids = []
        for i in range(0, len(scrape_configs), self.max_batch_size):
            batch = scrape_configs[i:i + self.max_batch_size]
            
            # Process batch in parallel
            job_ids = await self._process_batch(batch)
            all_job_ids.extend(job_ids)
        
        return all_job_ids
    
    async def _process_batch(self, batch: List[Dict[str, Any]]) -> List[str]:
        """Process a batch of scrape configurations in parallel."""
        tasks = []
        for config in batch:
            task = self._enqueue_config(config)
            tasks.append(task)
        
        # Wait for all tasks to complete
        job_ids = await self._gather_with_concurrency(tasks)
        return job_ids
    
    async def _enqueue_config(self, config: Dict[str, Any]) -> str:
        """Enqueue a single scrape configuration."""
        try:
            # Extract configuration
            scraper_id = config["scraper_id"]
            date_range = config["date_range"]
            labels = config.get("labels")
            entity_limit = config.get("entity_limit")
            callback_info = config.get("callback_info")
            
            # Call the base client's enqueue method
            return await self.client.enqueue_scrape_job(
                scraper_id=scraper_id,
                date_range=date_range,
                labels=labels,
                entity_limit=entity_limit,
                callback_info=callback_info
            )
        except Exception as e:
            bt.logging.error(f"Error enqueueing scrape config for {config.get('scraper_id', 'unknown')}: {e}")
            raise
    
    async def _gather_with_concurrency(self, tasks):
        """Execute tasks concurrently and gather results in order."""
        return await asyncio.gather(*tasks, return_exceptions=True)

# Import asyncio at the top level to avoid issues with _gather_with_concurrency
import asyncio