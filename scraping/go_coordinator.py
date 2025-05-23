import asyncio
import datetime as dt
import threading
import traceback
from typing import Dict, List

import bittensor as bt

from scraping.coordinator import CoordinatorConfig, _choose_scrape_configs
from scraping.go_client import GoScraperClient
from scraping.scraper import ScraperId, ScrapeConfig


class GoScraperCoordinator:
    """
    Coordinates scraping by offloading the actual scraping work to a Go microservice.
    Uses the same scheduling logic as the original ScraperCoordinator but delegates
    scrape tasks to the Go service via Redis.

    Database operations are split:
    - The Go microservice (and another dedicated microservice) handle database WRITES
    - The miner itself directly reads from the same database to serve network requests

    This separation improves performance and scalability while maintaining direct read access.
    """

    class Tracker:
        """Tracks scrape runs for the coordinator."""

        def __init__(self, config: CoordinatorConfig, now: dt.datetime):
            self.cadence_by_scraper_id = {
                scraper_id: dt.timedelta(seconds=cfg.cadence_seconds)
                for scraper_id, cfg in config.scraper_configs.items()
            }

            # Initialize the last scrape time as now, to protect against frequent scraping during Miner crash loops.
            self.last_scrape_time_per_scraper_id: Dict[ScraperId, dt.datetime] = {
                scraper_id: now for scraper_id in config.scraper_configs.keys()
            }

        def get_scraper_ids_ready_to_scrape(self, now: dt.datetime) -> List[ScraperId]:
            """Returns a list of ScraperIds which are due to run."""
            results = []
            for scraper_id, cadence in self.cadence_by_scraper_id.items():
                last_scrape_time = self.last_scrape_time_per_scraper_id.get(
                    scraper_id, None
                )
                if last_scrape_time is None or now - last_scrape_time >= cadence:
                    results.append(scraper_id)
            return results

        def on_scrape_scheduled(self, scraper_id: ScraperId, now: dt.datetime):
            """Notifies the tracker that a scrape has been scheduled."""
            self.last_scrape_time_per_scraper_id[scraper_id] = now

    def __init__(
        self,
        config: CoordinatorConfig,
        redis_url: str = "redis://localhost:6379",
        queue_name: str = "scrape_queue",
    ):
        self.config = config
        self.go_client = GoScraperClient(redis_url=redis_url, queue_name=queue_name)

        self.tracker = GoScraperCoordinator.Tracker(self.config, dt.datetime.utcnow())
        self.is_running = False

    def run_in_background_thread(self):
        """
        Runs the Coordinator on a background thread. The coordinator will run until the process dies.
        """
        assert not self.is_running, "GoScraperCoordinator already running"

        bt.logging.info("Starting GoScraperCoordinator in a background thread.")

        self.is_running = True
        threading.Thread(target=self.run, daemon=True).start()

    def run(self):
        """Blocking call to run the Coordinator, indefinitely."""
        asyncio.run(self._start())

    def stop(self):
        bt.logging.info("Stopping the GoScraperCoordinator.")
        self.is_running = False

    async def _start(self):
        # Connect to Redis first
        await self.go_client.connect()

        try:
            while self.is_running:
                now = dt.datetime.utcnow()
                now = now.replace(tzinfo=dt.timezone.utc)  # Ensure timezone information

                scraper_ids_to_scrape_now = self.tracker.get_scraper_ids_ready_to_scrape(now)

                if not scraper_ids_to_scrape_now:
                    bt.logging.trace("Nothing ready to scrape yet. Trying again in 15s.")
                    # Nothing is due a scrape. Wait a few seconds and try again
                    await asyncio.sleep(15)
                    continue

                for scraper_id in scraper_ids_to_scrape_now:
                    scrape_configs = _choose_scrape_configs(scraper_id, self.config, now)

                    for config in scrape_configs:
                        bt.logging.info(f"Enqueueing scrape task for {scraper_id}: {config}.")
                        await self._enqueue_go_scrape(scraper_id, config)

                    self.tracker.on_scrape_scheduled(scraper_id, now)

                # Short sleep to avoid tight loop
                await asyncio.sleep(1)

        except Exception as e:
            bt.logging.error(f"Error in coordinator: {e}")
            bt.logging.error(traceback.format_exc())
        finally:
            await self.go_client.close()
            bt.logging.info("Coordinator stopped.")

    async def _enqueue_go_scrape(self, scraper_id: ScraperId, config: ScrapeConfig):
        """
        Enqueue a scrape job to the Go microservice via Redis.

        The Go microservice will:
        1. Perform the actual scraping
        2. Write data to the database via a separate microservice
        3. The miner will then read this data directly from the database

        Args:
            scraper_id: The ID of the scraper to use
            config: The scrape configuration
        """
        try:
            # Enqueue the job to the Go service
            job_id = await self.go_client.enqueue_scrape_job(
                scraper_id=scraper_id.value,
                date_range=config.date_range,
                labels=config.labels,
                entity_limit=config.entity_limit,
                callback_info={"storage_type": "database_service"}  # Instructs Go service to store via database service
            )

            bt.logging.info(f"Enqueued scrape job {job_id} for {scraper_id} - data will be written to the database")

        except Exception as e:
            bt.logging.error(f"Failed to enqueue scrape job for {scraper_id}: {e}")
            bt.logging.error(traceback.format_exc())
