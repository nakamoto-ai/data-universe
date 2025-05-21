from typing import List
import time
from mock_data import DataEntity

class MockMinerStorage:
    def __init__(self, write_latency_ms: int = 10):
        self.write_latency_ms = write_latency_ms
        self.stored_entities = []
        self.write_count = 0
        self.last_write_time = 0
        self.total_write_time = 0
        
    def store_data_entities(self, entities: List[DataEntity]):
        start_time = time.time()
        # Simulate storage latency
        time.sleep(self.write_latency_ms / 1000.0)
        
        self.stored_entities.extend(entities)
        self.write_count += 1
        write_time = time.time() - start_time
        self.last_write_time = write_time
        self.total_write_time += write_time
        return len(entities)
    
    def store_data_entities_batch(self, entities: List[DataEntity]):
        """Batch version of store_data_entities with slightly better efficiency"""
        start_time = time.time()
        # Simulate slightly more efficient batch storage (20% faster)
        time.sleep(self.write_latency_ms * 0.8 / 1000.0)
        
        self.stored_entities.extend(entities)
        self.write_count += 1
        write_time = time.time() - start_time
        self.last_write_time = write_time
        self.total_write_time += write_time
        return len(entities)
    
    def get_stats(self):
        """Return statistics about storage operations"""
        return {
            "total_entities": len(self.stored_entities),
            "write_operations": self.write_count,
            "avg_write_time": (self.total_write_time / self.write_count) if self.write_count > 0 else 0,
            "last_write_time": self.last_write_time
        }