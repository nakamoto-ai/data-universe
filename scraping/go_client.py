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
            max
