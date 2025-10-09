#!/usr/bin/env python3
"""
Load Shedding Pattern Match Storage
CS-E4780 Project: Efficient Pattern Detection over Data Streams

This module implements utility-based load shedding for pattern match storage.
It wraps existing storage classes and drops low-utility partial matches when
storage capacity is exceeded to maintain bounded latency.
"""

import random
import time
from datetime import datetime
from typing import List, Optional

from base.PatternMatch import PatternMatch
from tree.PatternMatchStorage import PatternMatchStorage


class LoadSheddingConfig:
    """Configuration for load shedding behavior"""
    def __init__(self, 
                 enabled: bool = True,
                 max_partial_matches: int = 1000,
                 utility_threshold: float = 0.3,
                 shedding_strategy: str = "utility",
                 latency_bound_ms: float = 100.0,
                 aggressive_shedding: bool = True):
        self.enabled = enabled
        self.max_partial_matches = max_partial_matches
        self.utility_threshold = utility_threshold
        self.shedding_strategy = shedding_strategy  # "utility", "random", "oldest", "fifo"
        self.latency_bound_ms = latency_bound_ms
        self.aggressive_shedding = aggressive_shedding


class LoadSheddingPatternMatchStorage:
    """
    A wrapper around PatternMatchStorage that implements utility-based load shedding.
    
    When the number of stored partial matches exceeds the configured limit,
    this wrapper drops low-utility matches to maintain bounded latency.
    """
    
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
        """
        Add a partial match with load shedding logic.
        
        If storage is at capacity, drop low-utility matches before adding the new one.
        """
        self.total_matches += 1
        
        if not self.config.enabled:
            # Load shedding disabled - use original behavior
            self.wrapped_storage.add(pm)
            return
            
        # Check if we need to shed load
        current_size = len(self.wrapped_storage)
        
        if current_size >= self.config.max_partial_matches:
            # Storage is at capacity - apply load shedding
            self._apply_load_shedding()
            
        # Add the new partial match
        self.wrapped_storage.add(pm)
        
    def _apply_load_shedding(self):
        """Apply load shedding to reduce storage size"""
        current_time = time.time()
        self.last_shedding_time = current_time
        
        internal_buffer = self.wrapped_storage.get_internal_buffer()
        current_size = len(internal_buffer)
        
        if current_size == 0:
            return
            
        # Calculate how many matches to drop
        if self.config.aggressive_shedding:
            # Drop more aggressively to prevent frequent shedding
            target_size = int(self.config.max_partial_matches * 0.5)  # Drop to 50% capacity
        else:
            target_size = self.config.max_partial_matches - 1  # Drop just one
            
        matches_to_drop = max(0, current_size - target_size)
        
        if matches_to_drop <= 0:
            return
            
        # Apply shedding strategy
        if self.config.shedding_strategy == "utility":
            self._drop_by_utility(internal_buffer, matches_to_drop)
        elif self.config.shedding_strategy == "random":
            self._drop_random(internal_buffer, matches_to_drop)
        elif self.config.shedding_strategy == "oldest":
            self._drop_oldest(internal_buffer, matches_to_drop)
        elif self.config.shedding_strategy == "fifo":
            self._drop_fifo(internal_buffer, matches_to_drop)
        else:
            # Default to random
            self._drop_random(internal_buffer, matches_to_drop)
            
        self.dropped_matches += matches_to_drop
        
    def _calculate_utility(self, pm: PatternMatch) -> float:
        """
        Calculate utility score for a partial match.
        Higher scores = more valuable matches that should be kept.
        
        Factors:
        - Chain length (more events = higher utility) 
        - Recency (newer matches = higher utility)
        - Progress toward completion (closer to target stations = higher utility)
        """
        utility = 0.0
        
        # Base utility from chain length
        chain_length = len(pm.events) if pm.events else 1
        utility += chain_length * 0.3  # Longer chains are more valuable
        
        # Recency bonus - newer matches are more likely to complete
        if pm.events and len(pm.events) > 0:
            latest_event = max(pm.events, key=lambda e: e.timestamp)
            age_seconds = (datetime.now() - latest_event.timestamp).total_seconds()
            # Decay utility based on age (max bonus = 0.4)
            recency_bonus = max(0, 0.4 * (1.0 - min(age_seconds / 3600.0, 1.0)))  # 1 hour decay
            utility += recency_bonus
            
        # Completion progress bonus
        # Check if any events in chain end at target stations (indicates progress)
        target_stations = {"7", "8", "9", "72", "79", "83", "262", "296", "300"}  # Common targets
        progress_bonus = 0.0
        
        if pm.events:
            for event in pm.events:
                if hasattr(event, 'payload') and event.payload:
                    end_station = str(event.payload.get('end station id', ''))
                    if end_station in target_stations:
                        progress_bonus += 0.2  # Bonus for being near target stations
                        
        utility += min(progress_bonus, 0.3)  # Cap progress bonus
        
        # Add small random component to break ties
        utility += random.uniform(0, 0.1)
        
        return utility
        
    def _drop_by_utility(self, buffer: List[PatternMatch], count: int):
        """Drop matches with lowest utility scores"""
        if len(buffer) <= count:
            buffer.clear()
            return
            
        # Calculate utility for all matches
        utilities = [(i, self._calculate_utility(pm)) for i, pm in enumerate(buffer)]
        
        # Sort by utility (ascending - lowest first)
        utilities.sort(key=lambda x: x[1])
        
        # Drop the lowest utility matches
        indices_to_drop = sorted([utilities[i][0] for i in range(count)], reverse=True)
        
        for idx in indices_to_drop:
            del buffer[idx]
            
    def _drop_random(self, buffer: List[PatternMatch], count: int):
        """Drop random matches"""
        if len(buffer) <= count:
            buffer.clear()
            return
            
        indices_to_drop = sorted(random.sample(range(len(buffer)), count), reverse=True)
        
        for idx in indices_to_drop:
            del buffer[idx]
            
    def _drop_oldest(self, buffer: List[PatternMatch], count: int):
        """Drop oldest matches (by first timestamp)"""
        if len(buffer) <= count:
            buffer.clear()
            return
            
        # Sort by first timestamp and drop oldest
        indexed_matches = [(i, pm.first_timestamp if pm.first_timestamp else datetime.min) 
                          for i, pm in enumerate(buffer)]
        indexed_matches.sort(key=lambda x: x[1])  # Oldest first
        
        indices_to_drop = sorted([indexed_matches[i][0] for i in range(count)], reverse=True)
        
        for idx in indices_to_drop:
            del buffer[idx]
            
    def _drop_fifo(self, buffer: List[PatternMatch], count: int):
        """Drop first matches (FIFO)"""
        if len(buffer) <= count:
            buffer.clear()
            return
            
        # Remove first N elements
        for _ in range(count):
            buffer.pop(0)
            
    def get_load_shedding_stats(self):
        """Return load shedding statistics"""
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