import asyncio
import datetime as dt
import argparse
import json
from typing import List, Optional

import bittensor as bt
from bittensor import logging
from common.data import DateRange, DataLabel
from scraping.go_client import GoScraperClient

logging.set_debug()

async def test_enqueue_job(
    client: GoScraperClient,
    scraper_id: str,
    hours_ago: int = 24,
    entity_limit: Optional[int] = 10,
    labels: Optional[List[str]] = None,
    callback_info: Optional[dict] = None
):
    """Test enqueueing a single scrape job with the specified parameters."""
    print(f"\n=== Enqueueing job for {scraper_id} ===")

    # Create date range
    now = dt.datetime.now(dt.timezone.utc)
    start_time = now - dt.timedelta(hours=hours_ago)
    date_range = DateRange(start=start_time, end=now)

    # Convert label strings to DataLabel objects
    data_labels = None
    if labels:
        data_labels = [DataLabel(value=label) for label in labels]

    print(f"Date range: {date_range.start} to {date_range.end}")
    print(f"Entity limit: {entity_limit}")
    print(f"Labels: {labels}")

    try:
        # Enqueue the job
        start = dt.datetime.now()
        job_id = await client.enqueue_scrape_job(
            scraper_id=scraper_id,
            date_range=date_range,
            labels=data_labels,
            entity_limit=entity_limit,
            callback_info=callback_info
        )
        elapsed = (dt.datetime.now() - start).total_seconds()

        # Print results
        print(f"Success! Job enqueued with ID: {job_id} in {elapsed:.2f}s")
        return job_id
    except Exception as e:
        print(f"Error: {e}")
        return None


async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test the Go scraper queue client")
    parser.add_argument("--scraper", type=str, default="X.flash",
                      help="Scraper ID to test (default: X.flash)")
    parser.add_argument("--redis", type=str, default="redis://localhost:6379",
                      help="Redis connection URL")
    parser.add_argument("--queue", type=str, default="scrape_queue",
                      help="Redis queue name")
    parser.add_argument("--hours", type=int, default=24,
                      help="Hours ago to start scraping from")
    parser.add_argument("--limit", type=int, default=10,
                      help="Maximum number of entities to retrieve")
    parser.add_argument("--labels", type=str, nargs="+",
                      help="Labels to filter by")
    parser.add_argument("--batch", action="store_true",
                      help="Test batch job enqueueing")
    parser.add_argument("--callback-url", type=str,
                      help="Optional callback URL for results")
    args = parser.parse_args()

    # Create client
    client = GoScraperClient(redis_url=args.redis, queue_name=args.queue)

    # Prepare callback info if provided
    callback_info = None
    if args.callback_url:
        callback_info = {
            "url": args.callback_url,
            "method": "POST",
            "headers": {"Content-Type": "application/json"}
        }

    try:
        # Connect to Redis
        await client.connect()

        # Test single job enqueueing
        await test_enqueue_job(
            client,
            scraper_id=args.scraper,
            hours_ago=args.hours,
            entity_limit=args.limit,
            labels=args.labels,
            callback_info=callback_info
        )
    finally:
        await client.close()

    print("\nNOTE: This test only confirms jobs were enqueued successfully.")
    print("The Go service should process these jobs asynchronously.")
    if args.callback_url:
        print(f"Results will be sent to: {args.callback_url}")
    else:
        print("No callback URL provided - results will be handled according to Go service configuration.")

if __name__ == "__main__":
    asyncio.run(main())
