import enum
import datetime as dt
from typing import Dict, List, Optional, Any
from enum import IntEnum, Enum
from pydantic import BaseModel, Field, ConfigDict

# Mock constants
class Constants:
    DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES = 100_000_000  # 100MB

constants = Constants()

class StrictBaseModel(BaseModel):
    """Base model for all data structures."""
    model_config = ConfigDict(frozen=True)

class DataSource(IntEnum):
    """The source of data."""
    REDDIT = 1
    X = 2
    YOUTUBE = 3
    UNKNOWN_4 = 4
    UNKNOWN_5 = 5
    UNKNOWN_6 = 6
    UNKNOWN_7 = 7

class DataLabel(StrictBaseModel):
    """An optional label to classify a data entity."""
    
    value: str = Field(
        max_length=140,
        description="The label. E.g. a subreddit for Reddit data.",
    )
    
    def __hash__(self) -> int:
        return hash(self.value)

class TimeBucket(StrictBaseModel):
    """A bucket of time, currently defined as an hour block."""
    
    id: int = Field(
        description="The number of hours since the epoch.",
    )
    
    @classmethod
    def from_datetime(cls, dt_obj: dt.datetime) -> 'TimeBucket':
        """Create a TimeBucket from a datetime object."""
        # Ensure datetime has timezone info
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
            
        # Convert to hours since epoch
        epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        hours_since_epoch = int((dt_obj - epoch).total_seconds() / 3600)
        return cls(id=hours_since_epoch)
    
    @classmethod
    def to_date_range(cls, bucket: 'TimeBucket') -> 'DateRange':
        """Convert a TimeBucket to a DateRange."""
        epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        start = epoch + dt.timedelta(hours=bucket.id)
        end = start + dt.timedelta(hours=1)
        return DateRange(start=start, end=end)
    
    def __hash__(self) -> int:
        return hash(self.id)

class DateRange(StrictBaseModel):
    """A range of dates."""
    
    start: dt.datetime
    end: dt.datetime
    
    def __hash__(self) -> int:
        return hash((self.start, self.end))

class DataEntity(StrictBaseModel):
    """A logical unit of data that has been scraped. E.g. a Reddit post"""
    
    uri: str
    datetime: dt.datetime
    source: DataSource
    label: Optional[DataLabel] = Field(default=None)
    content: bytes
    content_size_bytes: int = Field(ge=0)
    
    def __hash__(self) -> int:
        return hash(self.uri)

class DataEntityBucketId(StrictBaseModel):
    """Uniquely identifies a bucket to group DataEntities by time bucket, source, and label."""
    
    time_bucket: TimeBucket
    source: DataSource = Field()
    label: Optional[DataLabel] = Field(default=None)
    
    def __hash__(self) -> int:
        return hash(hash(self.time_bucket) + hash(self.source) + hash(self.label))

class DataEntityBucket(StrictBaseModel):
    """Summarizes a group of data entities stored by a miner."""
    
    id: DataEntityBucketId = Field(
        description="Identifies the qualities by which this bucket is grouped."
    )
    size_bytes: int = Field(ge=0, le=constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES)
    
    def __hash__(self) -> int:
        return hash(self.id)

class ScraperId(str, Enum):
    """The id for each of the scrapers."""
    
    REDDIT_LITE = "Reddit.lite"
    X_FLASH = "X.flash"
    REDDIT_CUSTOM = "Reddit.custom"
    X_MICROWORLDS = "X.microworlds"
    X_APIDOJO = "X.apidojo"
    X_QUACKER = "X.quacker"
    YOUTUBE_TRANSCRIPT = "YouTube.transcript"