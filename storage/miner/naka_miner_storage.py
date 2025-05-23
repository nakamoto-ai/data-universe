from storage.miner.miner_storage import MinerStorage
from common.data import (
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucketId,
    DataEntityBucket,
)
from typing import Dict, List, Optional
import datetime as dt
import requests
import time
import bittensor as bt

class NakaMinerStorage(MinerStorage):
    """
    Implementation of MinerStorage that uses an API service for all read operations.
    This class does not handle database writes as they are handled by a separate microservice.
    """

    def __init__(
        self,
        api_url: str,
        api_key: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 0.5,
        timeout: float = 10.0,
    ):
        """
        Initialize the NakaMinerStorage with API connection parameters.

        Args:
            api_url: Base URL for the storage API
            api_key: Optional API key for authentication
            max_retries: Maximum number of retry attempts for API calls
            retry_delay: Delay between retry attempts in seconds
            timeout: Timeout for API requests in seconds
        """
        self.api_url = api_url.rstrip("/")  # Remove trailing slash if present
        self.api_key = api_key
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout

        # Set up the headers for API requests
        self.headers = {
            "Content-Type": "application/json",
        }

        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"

        bt.logging.info(f"Initialized NakaMinerStorage with API at {api_url}")

    def _make_api_request(self, endpoint: str, method: str = "GET", data: Optional[dict] = None):
        """
        Make an API request with retry logic.

        Args:
            endpoint: API endpoint to call (without base URL)
            method: HTTP method to use
            data: Optional data to send in the request

        Returns:
            The JSON response from the API

        Raises:
            Exception: If the API call fails after all retries
        """
        url = f"{self.api_url}/{endpoint.lstrip('/')}"

        for attempt in range(self.max_retries):
            try:
                if method.upper() == "GET":
                    response = requests.get(url, headers=self.headers, timeout=self.timeout)
                elif method.upper() == "POST":
                    response = requests.post(url, headers=self.headers, json=data, timeout=self.timeout)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                response.raise_for_status()  # Raise exception for non-2xx responses
                return response.json()

            except requests.exceptions.RequestException as e:
                bt.logging.warning(f"API request failed (attempt {attempt+1}/{self.max_retries}): {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    bt.logging.error(f"API request failed after {self.max_retries} attempts: {str(e)}")
                    raise

    def store_data_entities(self, data_entities: List[DataEntity]):
        """
        This method is intentionally not implemented as data storage is handled by a separate microservice.

        Args:
            data_entities: List of DataEntity objects

        Raises:
            NotImplementedError: Always raised as this operation is not supported
        """
        raise NotImplementedError("Data storage is handled by a separate microservice")

    def list_data_entities_in_data_entity_bucket(
        self, data_entity_bucket_id: DataEntityBucketId
    ) -> List[DataEntity]:
        """
        Lists from storage all DataEntities matching the provided DataEntityBucket.

        Args:
            data_entity_bucket_id: ID of the bucket to retrieve

        Returns:
            List of DataEntity objects in the bucket
        """
        bt.logging.info(f"Fetching data entities for bucket ID {data_entity_bucket_id}")

        response = self._make_api_request(
            f"buckets/{data_entity_bucket_id}/entities",
            method="GET"
        )

        # Convert the response to DataEntity objects
        data_entities = []
        for entity_data in response.get("entities", []):
            data_entities.append(DataEntity.model_validate(entity_data))

        bt.logging.info(f"Retrieved {len(data_entities)} data entities for bucket {data_entity_bucket_id}")
        return data_entities

    def get_data_entity_bucket(self, bucket_id: DataEntityBucketId) -> Optional[DataEntityBucket]:
        """
        Gets a complete DataEntityBucket for the given bucket ID.

        Args:
            bucket_id: ID of the bucket to retrieve

        Returns:
            DataEntityBucket object or None if not found
        """
        bt.logging.info(f"Fetching data entity bucket for ID {bucket_id}")

        try:
            response = self._make_api_request(
                f"buckets/{bucket_id}",
                method="GET"
            )

            # Convert the response to a DataEntityBucket object
            if response and "bucket" in response:
                bucket = DataEntityBucket.model_validate(response["bucket"])
                bt.logging.info(f"Retrieved bucket {bucket_id} of size {bucket.size_bytes} bytes")
                return bucket
            return None

        except Exception as e:
            bt.logging.error(f"Error retrieving bucket {bucket_id}: {str(e)}")
            return None

    def get_compressed_index(self, bucket_count_limit: Optional[int] = None) -> CompressedMinerIndex:
        """
        Gets the compressed MinedIndex, which is a summary of all of the DataEntities
        that this MinerStorage is currently serving.

        Args:
            bucket_count_limit: Optional limit on the number of buckets to include

        Returns:
            CompressedMinerIndex containing the summary information
        """
        bt.logging.info(f"Fetching compressed index with bucket_count_limit={bucket_count_limit}")

        params = {}
        if bucket_count_limit is not None:
            params["limit"] = bucket_count_limit

        endpoint = "index"
        if params:
            endpoint += "?" + "&".join(f"{k}={v}" for k, v in params.items())

        response = self._make_api_request(endpoint, method="GET")

        if response is None:
            bt.logging.error("Received a None response from the API endpoint")
            raise Exception("Failed to retrieve a valid response from the API")

        # Convert the response to a CompressedMinerIndex object
        compressed_index = CompressedMinerIndex.model_validate(response["index"])

        bt.logging.info(f"Retrieved compressed index with {compressed_index.bucket_count} buckets")
        return compressed_index

    def refresh_compressed_index(self, date_time: dt.timedelta):
        """
        Requests the API to refresh the compressed MinerIndex.

        Args:
            date_time: Time delta to use for the refresh
        """
        bt.logging.info(f"Requesting refresh of compressed index with date_time={date_time}")

        data = {
            "date_time_seconds": date_time.total_seconds()
        }

        try:
            self._make_api_request("index/refresh", method="POST", data=data)
            bt.logging.info("Compressed index refresh requested successfully")
        except Exception as e:
            bt.logging.error(f"Failed to request index refresh: {str(e)}")

    def list_contents_in_data_entity_buckets(
        self, data_entity_bucket_ids: List[DataEntityBucketId]
    ) -> Dict[DataEntityBucketId, List[bytes]]:
        """
        Lists contents for each requested DataEntityBucketId.

        Args:
            data_entity_bucket_ids: Which buckets to get contents for

        Returns:
            Dict mapping each bucket ID to its contained contents
        """
        bt.logging.info(f"Fetching contents for {len(data_entity_bucket_ids)} buckets")

        data = {
            "bucket_ids": data_entity_bucket_ids
        }

        response = self._make_api_request("contents", method="POST", data=data)

        if response is None:
            bt.logging.error("Received None response from contents API endpoint")
            return {}

        # Convert the response to the expected format
        # The API returns base64-encoded content which needs to be decoded to bytes
        import base64

        result = {}
        for bucket_id, contents in response.get("contents", {}).items():
            result[bucket_id] = [base64.b64decode(content) for content in contents]

        bt.logging.info(f"Retrieved contents for {len(result)} buckets")
        return result

    def get_data_for_on_demand_request(self, job_id: str, url: str) -> Optional[DataEntity]:
        """
        Gets data for an on-demand request that was previously submitted.

        Args:
            job_id: ID of the job that was submitted
            url: URL that was requested

        Returns:
            DataEntity if found, None otherwise
        """
        bt.logging.info(f"Checking for on-demand data for job {job_id}, URL {url}")

        try:
            response = self._make_api_request(
                "on_demand/result",
                method="POST",
                data={"job_id": job_id, "url": url}
            )

            if response and "data_entity" in response:
                entity = DataEntity.model_validate(response["data_entity"])
                bt.logging.info(f"Found on-demand data for job {job_id}")
                return entity

            bt.logging.info(f"No on-demand data found yet for job {job_id}")
            return None

        except Exception as e:
            bt.logging.error(f"Error retrieving on-demand data for job {job_id}: {str(e)}")
            return None
