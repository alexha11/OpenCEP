#!/usr/bin/env python3
"""
Load Shedding Pattern Match Storage
CS-E4780 Project

Utility-based load shedding for bounded latency.
"""

import random
import time
from datetime import datetime
from typing import List, Optional

from base.PatternMatch import PatternMatch
from tree.PatternMatchStorage import PatternMatchStorage


class LoadSheddingConfig:
    """Load shedding configuration."""
    def __init__(self, 
                 enabled: bool = True,
                 max_partial_matches: int = 1000,
                 utility_threshold: float = 0.3,
                 shedding_strategy: str = "utility",
                 latency_bound_ms: float = 100.0,
                 aggressive_shedding: bool = True,
                 input_shedding_ratio: float = 0.0,
                 target_stations: Optional[set] = None):
        self.enabled = enabled
        self.max_partial_matches = max_partial_matches
        self.utility_threshold = utility_threshold
        self.shedding_strategy = shedding_strategy
        self.latency_bound_ms = latency_bound_ms
        self.aggressive_shedding = aggressive_shedding
        self.input_shedding_ratio = input_shedding_ratio
        self.target_stations = set(str(s) for s in target_stations) if target_stations else None


class LoadSheddingPatternMatchStorage:
    """Wrapper that implements utility-based load shedding on pattern match storage."""
    
    def __init__(self, wrapped_storage: PatternMatchStorage, config: LoadSheddingConfig):
        self.wrapped_storage = wrapped_storage
        self.config = config
        self.dropped_matches = 0
        self.total_matches = 0
        self.last_shedding_time = time.time()
        
    def __len__(self):
        return len(self.wrapped_storage)
    
    def __getitem__(self, index):
        return self.wrapped_storage[index]
    
    def __setitem__(self, index, item):
        self.wrapped_storage[index] = item
    
    def __delitem__(self, index):
        del self.wrapped_storage[index]
        
    def __iter__(self):
        return iter(self.wrapped_storage)
    
    def __contains__(self, item):
        return item in self.wrapped_storage
    
    def get_key_function(self):
        return self.wrapped_storage.get_key_function()
    
    def try_clean_expired_partial_matches(self, earliest_timestamp: datetime):
        return self.wrapped_storage.try_clean_expired_partial_matches(earliest_timestamp)
    
    def get_internal_buffer(self):
        return self.wrapped_storage.get_internal_buffer()
    
    def get(self, value):
        return self.wrapped_storage.get(value)
    
    def add(self, pm: PatternMatch):
        """Add partial match with load shedding logic."""
        self.total_matches += 1
        
        if not self.config.enabled:
            self.wrapped_storage.add(pm)
            return
            
        if len(self.wrapped_storage) >= self.config.max_partial_matches:
            self._apply_load_shedding()
            
        self.wrapped_storage.add(pm)
        
    def _apply_load_shedding(self):
        """Apply load shedding to reduce storage."""
        self.last_shedding_time = time.time()
        internal_buffer = self.wrapped_storage.get_internal_buffer()
        current_size = len(internal_buffer)
        
        if current_size == 0:
            return
            
        if self.config.aggressive_shedding:
            target_size = int(self.config.max_partial_matches * 0.5)
        else:
            target_size = self.config.max_partial_matches - 1
            
        matches_to_drop = max(0, current_size - target_size)
        if matches_to_drop <= 0:
            return
        if self.config.shedding_strategy == "utility":
            self._drop_by_utility(internal_buffer, matches_to_drop)
        elif self.config.shedding_strategy == "random":
            self._drop_random(internal_buffer, matches_to_drop)
        elif self.config.shedding_strategy == "oldest" or self.config.shedding_strategy == "fifo":
            self._drop_oldest(internal_buffer, matches_to_drop)
        else:
            self._drop_random(internal_buffer, matches_to_drop)
            
        self.dropped_matches += matches_to_drop
        
    def _calculate_utility(self, pm: PatternMatch) -> float:
        """Calculate utility score for partial match (higher = more valuable)."""
        utility = 0.0
        
        chain_length = len(pm.events) if pm.events else 1
        utility += chain_length * 0.3
        
        if pm.events and len(pm.events) > 0:
            latest_event = max(pm.events, key=lambda e: e.timestamp)
            age_seconds = (datetime.now() - latest_event.timestamp).total_seconds()
            recency_bonus = max(0, 0.4 * (1.0 - min(age_seconds / 3600.0, 1.0)))
            utility += recency_bonus
            
        cfg_targets = getattr(self.config, 'target_stations', None)
        target_stations = set(str(s) for s in cfg_targets) if cfg_targets else {"7", "8", "9"}
        progress_bonus = 0.0
        
        if pm.events:
            for event in pm.events:
                if hasattr(event, 'payload') and event.payload:
                    end_station = str(event.payload.get('end station id', ''))
                    if end_station in target_stations:
                        progress_bonus += 0.2
                        
        utility += min(progress_bonus, 0.3)
        utility += random.uniform(0, 0.1)
        
        return utility
        
    def _drop_by_utility(self, buffer: List[PatternMatch], count: int):
        """Drop matches with lowest utility."""
        if len(buffer) <= count:
            buffer.clear()
            return
            
        utilities = [(i, self._calculate_utility(pm)) for i, pm in enumerate(buffer)]
        utilities.sort(key=lambda x: x[1])
        indices_to_drop = sorted([utilities[i][0] for i in range(count)], reverse=True)
        
        for idx in indices_to_drop:
            del buffer[idx]
            
    def _drop_random(self, buffer: List[PatternMatch], count: int):
        """Drop random matches."""
        if len(buffer) <= count:
            buffer.clear()
            return
            
        indices_to_drop = sorted(random.sample(range(len(buffer)), count), reverse=True)
        for idx in indices_to_drop:
            del buffer[idx]
            
    def _drop_oldest(self, buffer: List[PatternMatch], count: int):
        """Drop oldest matches (FIFO)."""
        if len(buffer) <= count:
            buffer.clear()
            return
            
        indexed_matches = [(i, pm.first_timestamp if pm.first_timestamp else datetime.min) 
                          for i, pm in enumerate(buffer)]
        indexed_matches.sort(key=lambda x: x[1])
        indices_to_drop = sorted([indexed_matches[i][0] for i in range(count)], reverse=True)
        
        for idx in indices_to_drop:
            del buffer[idx]
            
            
    def get_load_shedding_stats(self):
        """Return load shedding statistics."""
        return {
            "total_matches": self.total_matches,
            "dropped_matches": self.dropped_matches,
            "current_size": len(self.wrapped_storage),
            "drop_rate": self.dropped_matches / max(self.total_matches, 1),
            "last_shedding_time": self.last_shedding_time,
            "config": {
                "enabled": self.config.enabled,
                "max_partial_matches": self.config.max_partial_matches,
                "strategy": self.config.shedding_strategy,
                "aggressive": self.config.aggressive_shedding
            }
        }