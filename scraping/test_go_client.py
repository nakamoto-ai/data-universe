import asyncio
import datetime as dt
import argparse
import json
from typing import List, Optional

import bittensor as bt
from common.data import DateRange, DataLabel
from scraping.go_client import GoScraperClient, BatchScraperClient

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

async def test_enqueue_batch(client: BatchScraperClient, configs: List[dict]):
    """Test batch job enqueueing with multiple configurations."""
    print("\n=== Testing Batch Job Enqueueing ===")
    print(f"Number of configurations: {len(configs)}")
    
    try:
        # Enqueue batch jobs
        start = dt.datetime.now()
        job_ids = await client.enqueue_batch(configs)
        elapsed = (dt.datetime.now() - start).total_seconds()
        
        # Print results
        print(f"Batch enqueued in {elapsed:.2f}s")
        
        success_count = 0
        
        for i, result in enumerate(job_ids):
            if isinstance(result, Exception):
                print(f"Config {i+1}: Error - {result}")
            else:
                success_count += 1
                print(f"Config {i+1}: Job ID: {result}")
        
        print(f"\nSummary: {success_count}/{len(job_ids)} jobs successfully enqueued")
        return job_ids
    except Exception as e:
        print(f"Batch error: {e}")
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
        
        if args.batch:
            # Create batch client
            batch_client = BatchScraperClient(base_client=client)
            
            # Create batch configurations
            configs = [
                {
                    "scraper_id": "X.flash",
                    "date_range": DateRange(
                        start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=args.hours),
                        end=dt.datetime.now(dt.timezone.utc)
                    ),
                    "entity_limit": args.limit,
                    "labels": [DataLabel(value="politics")] if args.labels else None,
                    "callback_info": callback_info
                },
                {
                    "scraper_id": "Reddit.lite",
                    "date_range": DateRange(
                        start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=args.hours),
                        end=dt.datetime.now(dt.timezone.utc)
                    ),
                    "entity_limit": args.limit,
                    "labels": [DataLabel(value="technology")] if args.labels else None,
                    "callback_info": callback_info
                },
                {
                    "scraper_id": "YouTube.transcript",
                    "date_range": DateRange(
                        start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=args.hours),
                        end=dt.datetime.now(dt.timezone.utc)
                    ),
                    "entity_limit": args.limit,
                    "labels": [DataLabel(value="educational")] if args.labels else None,
                    "callback_info": callback_info
                }
            ]
            
            await test_enqueue_batch(batch_client, configs)
        else:
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
        # Close the connection
        await client.close()
        
    print("\nNOTE: This test only confirms jobs were enqueued successfully.")
    print("The Go service should process these jobs asynchronously.")
    if args.callback_url:
        print(f"Results will be sent to: {args.callback_url}")
    else:
        print("No callback URL provided - results will be handled according to Go service configuration.")

if __name__ == "__main__":
    asyncio.run(main())