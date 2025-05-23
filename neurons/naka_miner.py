import asyncio
import copy
import datetime as dt
import sys
import threading
import time
import traceback
from collections import defaultdict

import bittensor as bt

from common.protocol import (
    GetMinerIndex,
    GetDataEntityBucket,
    GetContentsByBuckets,
    GetHuggingFaceMetadata,
)
from neurons.config import NeuronType, check_config, create_config
from scraping.config.config_reader import ConfigReader
from scraping.go_coordinator import GoScraperCoordinator
from storage.miner.naka_miner_storage import NakaMinerStorage
from huggingface_utils.huggingface_uploader import DualUploader
from huggingface_utils.encoding_system import EncodingKeyManager


class NakaMiner:
    """
    A miner implementation that uses the Go microservice for scraping content.
    This version offloads the scraping workload to a Go service for improved performance.
    """

    def __init__(self, config=None):
        self.config = copy.deepcopy(config or create_config(NeuronType.MINER))
        check_config(self.config)

        bt.logging(config=self.config, logging_dir=self.config.full_path)
        bt.logging.info(self.config)
        self.use_hf_uploader = self.config.huggingface
        self.use_gravity_retrieval = self.config.gravity

        if self.config.offline:
            bt.logging.success(
                "Running in offline mode. Skipping bittensor object setup and axon creation."
            )
            self.uid = 0  # Offline mode so assume it's == 0

        else:
            # The wallet holds the cryptographic key pairs for the miner.
            self.wallet = bt.wallet(config=self.config)
            bt.logging.info(f"Wallet: {self.wallet}.")

            # The subtensor is our connection to the Bittensor blockchain.
            self.subtensor = bt.subtensor(config=self.config)
            bt.logging.info(f"Subtensor: {self.subtensor}.")

            # The metagraph holds the state of the network, letting us know about other validators and miners.
            self.metagraph = self.subtensor.metagraph(self.config.netuid)
            bt.logging.info(f"Metagraph: {self.metagraph}.")

            # Each miner gets a unique identity (UID) in the network for differentiation.
            if self.wallet.hotkey.ss58_address in self.metagraph.hotkeys:
                self.uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
                bt.logging.info(
                    f"Running neuron on subnet: {self.config.netuid} with uid {self.uid} using network: {self.subtensor.chain_endpoint}."
                )
            else:
                self.uid = 0
                bt.logging.warning(
                    f"Hotkey {self.wallet.hotkey.ss58_address} not found in metagraph. Assuming this is a test."
                )

            self.last_sync_timestamp = dt.datetime.min
            self.step = 0

            # The axon handles request processing, allowing validators to send this miner requests.
            self.axon = bt.axon(wallet=self.wallet, port=self.config.axon.port)

            # Attach determiners which functions are called when servicing a request.
            bt.logging.info("Attaching forward function to miner axon.")
            self.axon.attach(
                forward_fn=self.get_index,
                blacklist_fn=self.get_index_blacklist,
                priority_fn=self.get_index_priority,
            ).attach(
                forward_fn=self.get_data_entity_bucket,
                blacklist_fn=self.get_data_entity_bucket_blacklist,
                priority_fn=self.get_data_entity_bucket_priority,
            ).attach(
                forward_fn=self.get_contents_by_buckets,
                blacklist_fn=self.get_contents_by_buckets_blacklist,
                priority_fn=self.get_contents_by_buckets_priority,
            ).attach(
                forward_fn=self.get_huggingface_metadata,
                blacklist_fn=self.get_huggingface_metadata_blacklist,
                priority_fn=self.get_huggingface_metadata_priority,
            ).attach(
                forward_fn=self.decode_urls,
                blacklist_fn=self.decode_urls_blacklist,
                priority_fn=self.decode_urls_priority
            ).attach(
                forward_fn=self.handle_on_demand,
                blacklist_fn=self.handle_on_demand_blacklist,
                priority_fn=self.handle_on_demand_priority
            )

            bt.logging.success(f"Axon created: {self.axon}.")

        # Instantiate runners.
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: threading.Thread = None
        self.compressed_index_refresh_thread: threading.Thread = None
        self.hugging_face_thread: threading.Thread = None
        self.lock = threading.RLock()
        self.vpermit_rao_limit = self.config.vpermit_rao_limit

        # Instantiate encoding keys
        self.encoding_key_manager = EncodingKeyManager(key_path=self.config.encoding_key_json_file)
        self.private_encoding_key_manager = EncodingKeyManager(key_path=self.config.private_encoding_key_json_file)

        bt.logging.info("Initialized EncodingKeyManager for URL encoding/decoding.")

        if self.use_hf_uploader:
            self.hf_uploader = DualUploader(
                db_path=self.config.neuron.database_name,
                encoding_key_manager=self.encoding_key_manager,
                private_encoding_key_manager=self.private_encoding_key_manager,
                wallet=self.wallet,
                subtensor=self.subtensor,
                state_file=self.config.miner_upload_state_file,
                s3_auth_url=self.config.s3_auth_url,
            )

        # Instantiate storage API client for read operations
        api_url = getattr(self.config.neuron, "storage_api_url", "http://localhost:8080/api")
        api_key = getattr(self.config.neuron, "storage_api_key", None)
        
        self.storage = NakaMinerStorage(
            api_url=api_url,
            api_key=api_key,
        )

        bt.logging.success(
            f"Successfully connected to NakaMinerStorage API at: {api_url}"
        )

        # Configure the GoScraperCoordinator
        bt.logging.info(
            f"Loading scraping config from {self.config.neuron.scraping_config_file}."
        )
        scraping_config = ConfigReader.load_config(
            self.config.neuron.scraping_config_file
        )
        bt.logging.success(f"Loaded scraping config: {scraping_config}.")

        # Get Redis configuration from config or use defaults
        redis_url = getattr(self.config.neuron, "redis_url", "redis://localhost:6379")
        redis_queue = getattr(self.config.neuron, "redis_queue", "scrape_queue")

        # Initialize the Go scraper coordinator instead of the regular scraper coordinator
        # Storage is not passed to coordinator as writes are handled by a separate microservice
        self.scraping_coordinator = GoScraperCoordinator(
            config=scraping_config,
            redis_url=redis_url,
            queue_name=redis_queue,
        )

        bt.logging.success(f"Initialized Go Scraper Coordinator with Redis at {redis_url}")

        # Configure per hotkey per request limits.
        self.request_lock = threading.RLock()
        self.last_cleared_request_limits = dt.datetime.now()
        self.requests_by_type_by_hotkey = defaultdict(lambda: defaultdict(lambda: 0))

    def refresh_index(self):
        """
        Refreshes the cached compressed miner index periodically off the hot path of GetMinerIndex requests.
        """
        while not self.should_exit:
            try:
                # Refresh the index if it hasn't been refreshed in the configured time period.
                self.storage.refresh_compressed_index(
                    time_delta=constants.MINER_CACHE_FRESHNESS
                )
                bt.logging.trace("Refresh index thread finished refreshing the index.")
                # Wait freshness period + 1 minute to try refreshing again.
                # Wait the additional minute to ensure that the next refresh sees a 'stale' index.
                time.sleep(
                    (
                        constants.MINER_CACHE_FRESHNESS + dt.timedelta(minutes=1)
                    ).total_seconds()
                )
            # In case of unforeseen errors, the refresh thread will log the error and continue operations.
            except Exception:
                bt.logging.error(traceback.format_exc())
                # Sleep 5 minutes to avoid constant refresh attempts if they are consistently erroring.
                time.sleep(60 * 5)

    def get_updated_lookup(self):
        if not self.use_gravity_retrieval:
            bt.logging.info("Gravity lookup retrieval is not enabled.")
            return

        last_update = None
        while not self.should_exit:
            try:
                current_datetime = dt.datetime.utcnow()

                bt.logging.info(f"Checking for update. Last update: {last_update}, Current time: {current_datetime}")

                # Check if it's a new day and we haven't updated yet
                if last_update is None or current_datetime.date() > last_update.date():
                    bt.logging.info("Retrieving the latest dynamic lookup...")
                    sync_run_retrieval(self.config)
                    bt.logging.info(f"New desirable data list has been written to total.json")
                    last_update = current_datetime
                    bt.logging.info(f"Updated dynamic lookup at {last_update}")
                else:
                    bt.logging.info("No update needed at this time.")

                # Sleep for 5 minutes before checking again
                bt.logging.info("Sleeping for 5 minutes...")
                time.sleep(300)

            except Exception as e:
                bt.logging.error(f"Error in get_updated_lookup: {str(e)}")
                bt.logging.exception("Exception details:")
                time.sleep(300)  # Wait 5 minutes before trying again

    def upload_hugging_face(self):
        """
        Uploads data to HuggingFace, if enabled.
        """
        if not self.use_hf_uploader:
            bt.logging.info("HF Uploader disabled, skipping...")
            return

        with self.lock:
            bt.logging.info("Starting HF Upload")
            # Upload data
            self.hf_uploader.upload()

    def run(self):
        """
        Initiates and manages the main loop for the miner.
        """

        if self.config.offline:
            bt.logging.success("Running in offline mode. Skipping axon serving.")
        else:
            # Check that miner is registered on the network.
            self.sync()

            # Serve passes the axon information to the network + netuid we are hosting on.
            # This will auto-update if the axon port of external ip have changed.
            bt.logging.info(
                f"Serving miner axon {self.axon} on network: {self.config.subtensor.chain_endpoint} with netuid: {self.config.netuid}."
            )
            self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor)

            # Start  starts the miner's axon, making it active on the network.
            self.axon.start()

            self.last_sync_timestamp = dt.datetime.now()
            bt.logging.success(f"Miner starting at {self.last_sync_timestamp}.")

        # Start the Go scraper coordinator
        self.scraping_coordinator.run_in_background_thread()

        while not self.should_exit:
            # This loop maintains the miner's operations until intentionally stopped.
            try:
                # In offline mode we just idle while the scraping_coordinator runs.
                if self.config.offline:
                    while not self.should_exit:
                        time.sleep(12)
                else:
                    # Epoch length defaults to 100 blocks at 12 seconds each for 20 minutes.
                    while dt.datetime.now() - self.last_sync_timestamp < (
                        dt.timedelta(seconds=12 * self.config.neuron.epoch_length)
                    ):
                        # Wait before checking again.
                        time.sleep(12)

                        # Check if we should exit.
                        if self.should_exit:
                            break

                    # Sync metagraph.
                    self.sync()

                    self._log_status(self.step)

                    self.last_sync_timestamp = dt.datetime.now()
                    self.step += 1

            # If someone intentionally stops the miner, it'll safely terminate operations.
            except KeyboardInterrupt:
                if not self.config.offline:
                    self.axon.stop()
                self.scraping_coordinator.stop()
                bt.logging.success("Miner killed by keyboard interrupt.")
                sys.exit()

            # In case of unforeseen errors, the miner will log the error and continue operations.
            except Exception as e:
                bt.logging.error(traceback.format_exc())

    def run_in_background_thread(self):
        """
        Starts miner in a background thread. The miner runs until self.should_exit is set to True
        or until the program exits.
        """
        if self.is_running:
            bt.logging.warning("Miner is already running.")
            return

        if not self.thread or not self.thread.is_alive():
            self.should_exit = False
            self.is_running = True

            bt.logging.info("Starting miner in a background thread.")
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            bt.logging.info("Started")

            if self.use_hf_uploader:
                bt.logging.info("Starting HF uploader in a background thread.")
                self.hugging_face_thread = threading.Thread(
                    target=self.upload_hugging_face, daemon=True
                )
                self.hugging_face_thread.start()
                bt.logging.info("Started HF uploader.")

    def stop_run_thread(self):
        """
        Stop the run thread.
        """
        if self.is_running:
            bt.logging.info("Stopping run thread.")
            self.should_exit = True
            self.is_running = False

            if self.thread and self.thread.is_alive():
                self.thread.join(5)
                if self.thread.is_alive():
                    bt.logging.warning("Thread did not stop within 5 seconds.")

    def __enter__(self):
        """
        Called when entering a context block. Simplifies resource cleanup.
        """
        if not self.is_running:
            self.run_in_background_thread()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Called when exiting a context block. Ensures the miner's resources are properly cleaned up.
        """
        if self.is_running:
            self.stop_run_thread()

        if not self.config.offline:
            self.axon.stop()
            bt.logging.info("Axon stopped.")

        bt.logging.info("Miner exited.")

    def resync_metagraph(self):
        """Forces a resync of the metagraph."""
        if self.config.offline:
            return

        bt.logging.info("Resyncing metagraph.")
        self.metagraph = self.subtensor.metagraph(self.config.netuid)
        bt.logging.info(f"Resynced metagraph: {self.metagraph}")

    def _log_status(self, step: int):
        """
        Logs the current status of the miner.
        """
        if self.config.offline:
            return

        # Log the current miner status.
        table_data = []
        for hotkey, requesters in list(self.requests_by_type_by_hotkey.items()):
            total_requests = sum(requesters.values())
            if total_requests > 0:
                # Convert the defaultdict to a normal dict for logging
                requesters_dict = dict(requesters)
                table_data.append([hotkey, requesters_dict, total_requests])

        bt.logging.info(f"Step: {step}, requests since last log: {table_data}")

    async def get_index(self, synapse: GetMinerIndex) -> GetMinerIndex:
        """Runs after the GetMinerIndex synapse has been deserialized (i.e. after synapse.data is available)."""
        bt.logging.info(
            f"Got to a GetMinerIndex request from {synapse.dendrite.hotkey}."
        )

        # Only synapse.version 4 is supported at this time.
        if synapse.version < 4:
            bt.logging.error(f"Unsupported protocol version: {synapse.version}.")
            return synapse

        # Return the appropriate amount of max buckets based on protocol of the requesting validator.
        compressed_index = self.storage.get_compressed_index(
            bucket_count_limit=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4
        )
        synapse.compressed_index_serialized = compressed_index.model_dump_json()
        bt.logging.success(
            f"Returning compressed miner index of {CompressedMinerIndex.size_bytes(compressed_index)} bytes "
            + f"across {CompressedMinerIndex.bucket_count(compressed_index)} buckets to {synapse.dendrite.hotkey}."
        )

        synapse.version = constants.PROTOCOL_VERSION

        return synapse

    # Add all other methods from the original Miner class here...
    # Including all the async methods, blacklist methods, etc.

    async def get_index_blacklist(self, synapse: bt.Synapse) -> bool:
        """Determines whether the request for an index should be blacklisted."""
        return self.default_blacklist(synapse)

    async def get_index_priority(self, synapse: bt.Synapse) -> float:
        """Assigns priority score to an index request."""
        return self.default_priority(synapse)

    async def get_data_entity_bucket(self, synapse: GetDataEntityBucket) -> GetDataEntityBucket:
        """Returns data from a specific bucket."""
        bt.logging.info(
            f"Got to a GetDataEntityBucket request from {synapse.dendrite.hotkey} for bucket {synapse.bucket_id}."
        )

        if synapse.version < 4:
            bt.logging.error(f"Unsupported protocol version: {synapse.version}.")
            return synapse

        # Increment counter.
        with self.request_lock:
            self.requests_by_type_by_hotkey[synapse.dendrite.hotkey][
                "get_data_entity_bucket"
            ] += 1

        if not synapse.bucket_id:
            bt.logging.error(f"No bucket_id specified in synapse.")
            return synapse

        # Get the data for the bucket.
        bucket_data = self.storage.get_data_entity_bucket(bucket_id=synapse.bucket_id)
        if not bucket_data or not bucket_data.data_entities:
            bt.logging.warning(f"No data found for bucket {synapse.bucket_id}.")
            return synapse

        # Serialize the bucket data for transmission.
        synapse.bucket_serialized = bucket_data.model_dump_json()
        bt.logging.success(
            f"Returning bucket {synapse.bucket_id} with {len(bucket_data.data_entities)} "
            + f"data entities to {synapse.dendrite.hotkey}."
        )

        synapse.version = constants.PROTOCOL_VERSION

        return synapse

    async def get_huggingface_metadata(self, synapse: GetHuggingFaceMetadata) -> GetHuggingFaceMetadata:
        """Returns metadata for HuggingFace data."""
        bt.logging.info(
            f"Got to a GetHuggingFaceMetadata request from {synapse.dendrite.hotkey}."
        )

        if synapse.version < 4:
            bt.logging.error(f"Unsupported protocol version: {synapse.version}.")
            return synapse

        # Increment counter.
        with self.request_lock:
            self.requests_by_type_by_hotkey[synapse.dendrite.hotkey][
                "get_huggingface_metadata"
            ] += 1

        if not self.use_hf_uploader:
            bt.logging.warning(f"HuggingFace uploader is disabled.")
            return synapse

        # Get HuggingFace metadata
        synapse.hf_metadata = self.hf_uploader.get_metadata()
        bt.logging.success(
            f"Returning HuggingFace metadata to {synapse.dendrite.hotkey}."
        )

        synapse.version = constants.PROTOCOL_VERSION

        return synapse

    async def decode_urls(self, synapse: DecodeUrls) -> DecodeUrls:
        """Decodes URLs using the encoding key manager."""
        bt.logging.info(
            f"Got to a DecodeUrls request from {synapse.dendrite.hotkey}."
        )

        if synapse.version < 4:
            bt.logging.error(f"Unsupported protocol version: {synapse.version}.")
            return synapse

        # Increment counter.
        with self.request_lock:
            self.requests_by_type_by_hotkey[synapse.dendrite.hotkey][
                "decode_urls"
            ] += 1

        if not synapse.encoded_urls:
            bt.logging.error(f"No encoded_urls specified in synapse.")
            return synapse

        # Decode the URLs
        try:
            synapse.decoded_urls = {}
            for url_hash, encoded_url in synapse.encoded_urls.items():
                try:
                    if self.private_encoding_key_manager.is_encrypted(encoded_url):
                        synapse.decoded_urls[url_hash] = self.private_encoding_key_manager.decrypt(encoded_url)
                    else:
                        synapse.decoded_urls[url_hash] = self.encoding_key_manager.decrypt(encoded_url)
                except Exception as e:
                    bt.logging.error(f"Failed to decode URL {url_hash}: {e}")

            bt.logging.success(
                f"Decoded {len(synapse.decoded_urls)} URLs for {synapse.dendrite.hotkey}."
            )
        except Exception as e:
            bt.logging.error(f"Error decoding URLs: {e}")

        synapse.version = constants.PROTOCOL_VERSION

        return synapse

    async def decode_urls_blacklist(self, synapse: bt.Synapse) -> bool:
        """Determines whether a URL decode request should be blacklisted."""
        return self.default_blacklist(synapse)

    async def decode_urls_priority(self, synapse: bt.Synapse) -> float:
        """Assigns priority score to a URL decode request."""
        return self.default_priority(synapse)

    async def get_huggingface_metadata_blacklist(self, synapse: bt.Synapse) -> bool:
        """Determines whether a HuggingFace metadata request should be blacklisted."""
        return self.default_blacklist(synapse)

    async def get_huggingface_metadata_priority(self, synapse: bt.Synapse) -> float:
        """Assigns priority score to a HuggingFace metadata request."""
        return self.default_priority(synapse)

    async def get_data_entity_bucket_blacklist(self, synapse: bt.Synapse) -> bool:
        """Determines whether a data entity bucket request should be blacklisted."""
        return self.default_blacklist(synapse)

    async def get_data_entity_bucket_priority(self, synapse: bt.Synapse) -> float:
        """Assigns priority score to a data entity bucket request."""
        return self.default_priority(synapse)

    async def handle_on_demand(self, synapse: HandleOnDemand) -> HandleOnDemand:
        """Handles on-demand requests by forwarding them to the Go microservice."""
        bt.logging.info(
            f"Got to a HandleOnDemand request from {synapse.dendrite.hotkey}."
        )

        if synapse.version < 4:
            bt.logging.error(f"Unsupported protocol version: {synapse.version}.")
            return synapse

        # Increment counter.
        with self.request_lock:
            self.requests_by_type_by_hotkey[synapse.dendrite.hotkey][
                "handle_on_demand"
            ] += 1

        # Validate the request
        if not synapse.url:
            bt.logging.error("No URL provided in on-demand request")
            synapse.success = False
            synapse.message = "No URL provided"
            return synapse

        try:
            # Create a date range for the scrape (usually current time)
            now = dt.datetime.now(dt.timezone.utc)
            date_range = DateRange(start=now - dt.timedelta(hours=24), end=now)

            # Create a unique job ID for tracking
            job_id = f"on_demand_{synapse.dendrite.hotkey}_{int(now.timestamp())}"

            # Forward the request to the Go microservice
            bt.logging.info(f"Forwarding on-demand request for URL {synapse.url} to Go microservice")

            # Use synapse.source for the scraper ID
            scraper_id = synapse.source

            # Prepare callback info with validator's hotkey for tracking
            callback_info = {
                "storage_type": "database_service",
                "request_id": job_id,
                "validator_hotkey": synapse.dendrite.hotkey
            }

            # Forward the request to the Go microservice via Redis
            await self.scraping_coordinator.go_client.enqueue_scrape_job(
                scraper_id=scraper_id,
                date_range=date_range,
                entity_limit=1,  # We only need one result for this URL
                callback_info=callback_info,
            )

            # Add import for asyncio at the top level if missing

            bt.logging.info(f"On-demand request forwarded, job ID: {job_id}")

            # Set up a timeout for waiting for results
            timeout = 30  # seconds
            start_time = time.time()
            poll_interval = 1  # seconds

            # Poll the database until results are available or timeout
            while time.time() - start_time < timeout:
                # Check if data is available via the storage API
                result = self.storage.get_data_for_on_demand_request(job_id, synapse.url)

                if result:
                    bt.logging.success(f"Retrieved on-demand result for {synapse.url}")
                    synapse.success = True
                    synapse.result = result.model_dump_json()
                    synapse.version = constants.PROTOCOL_VERSION
                    return synapse

                # Wait before polling again
                await asyncio.sleep(poll_interval)

            # If we got here, we timed out waiting for results
            bt.logging.warning(f"Timeout waiting for on-demand scraping results for {synapse.url}")
            synapse.success = False
            synapse.message = f"Timeout waiting for results. The data may still be processed later."

        except Exception as e:
            bt.logging.error(f"Error processing on-demand request: {e}")
            bt.logging.error(traceback.format_exc())
            synapse.success = False
            synapse.message = f"Error processing request: {str(e)}"

        synapse.version = constants.PROTOCOL_VERSION
        return synapse

    async def handle_on_demand_blacklist(self, synapse: bt.Synapse) -> bool:
        """Determines whether an on-demand request should be blacklisted."""
        return self.default_blacklist(synapse)

    async def handle_on_demand_priority(self, synapse: bt.Synapse) -> float:
        """Assigns priority score to an on-demand request."""
        return self.default_priority(synapse)

    async def get_contents_by_buckets(self, synapse: GetContentsByBuckets) -> GetContentsByBuckets:
        """Returns content from multiple buckets."""
        bt.logging.info(
            f"Got to a GetContentsByBuckets request from {synapse.dendrite.hotkey} for {len(synapse.bucket_ids) if synapse.bucket_ids else 0} buckets."
        )

        if synapse.version < 4:
            bt.logging.error(f"Unsupported protocol version: {synapse.version}.")
            return synapse

        # Increment counter.
        with self.request_lock:
            self.requests_by_type_by_hotkey[synapse.dendrite.hotkey][
                "get_contents_by_buckets"
            ] += 1

        if not synapse.bucket_ids:
            bt.logging.error(f"No bucket_ids specified in synapse.")
            return synapse

        # Get the data for each bucket and combine them.
        combined_buckets = []

        for bucket_id in synapse.bucket_ids:
            bucket_data = self.storage.get_data_entity_bucket(bucket_id=bucket_id)
            if bucket_data and bucket_data.data_entities:
                combined_buckets.append(bucket_data)

        if not combined_buckets:
            bt.logging.warning(f"No data found for any of the requested buckets.")
            return synapse

        # Serialize the combined bucket data for transmission.
        synapse.buckets_serialized = [bucket.model_dump_json() for bucket in combined_buckets]

        bt.logging.success(
            f"Returning {len(combined_buckets)} buckets with data to {synapse.dendrite.hotkey}."
        )

        synapse.version = constants.PROTOCOL_VERSION

        return synapse

    async def get_contents_by_buckets_blacklist(self, synapse: bt.Synapse) -> bool:
        """Determines whether a request for multiple buckets should be blacklisted."""
        return self.default_blacklist(synapse)

    async def get_contents_by_buckets_priority(self, synapse: bt.Synapse) -> float:
        """Assigns priority score to a request for multiple buckets."""
        return self.default_priority(synapse)

    def default_blacklist(self, synapse: bt.Synapse) -> bool:
        """
        Default blacklist implementation.
        Returns True if the request should be ignored, False otherwise.
        """
        # Add your blacklist logic here
        return False

    def default_priority(self, synapse: bt.Synapse) -> float:
        """
        Default priority implementation.
        Returns a priority score for the request, where higher is more important.
        """
        # Add your priority logic here
        return 1.0

    def get_config_for_test(self):
        """Returns configuration for testing."""
        return self.config

    def sync(self):
        """Sync the metagraph and update our knowledge of the network."""
        if self.config.offline:
            return

        self.resync_metagraph()

    def check_registered(self):
        """
        Checks if the wallet is registered on the subnetwork.
        Returns True if wallet is registered, False otherwise.
        """
        if self.config.offline:
            return True

        # Check if the hotkey is registered.
        return self.wallet.hotkey.ss58_address in self.metagraph.hotkeys
