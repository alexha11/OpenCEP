from base.Event import Event
from typing import List


class PatternMatch:
    """
    Represents a set of primitive events satisfying one or more patterns.
    An instance of this class could correspond either to a full pattern match, or to any intermediate result
    created during the evaluation process.
    """
    def __init__(self, events: List[Event], probability: float = None):
        self.events = events
        self.last_timestamp = max(events, key=lambda x: x.max_timestamp).max_timestamp
        self.first_timestamp = min(events, key=lambda x: x.min_timestamp).min_timestamp
        # this field is only used for full pattern matches
        self.pattern_ids = []
        self.probability = probability

    def __eq__(self, other):
        return isinstance(other, PatternMatch) and set(self.events) == set(other.events) and \
               self.pattern_ids == other.pattern_ids

    def __str__(self):
        result = ""
        match = ""
        for event in self.events:
            match += "%s\n" % event
        if len(self.pattern_ids) == 0:
            result += match
            result += "\n"
        else:
            for idx in self.pattern_ids:
                result += "%s: " % idx
                result += match
                result += "\n"
        return result

    def add_pattern_id(self, pattern_id: int):
        """
        Adds a new pattern ID corresponding to this pattern,
        """
        if pattern_id not in self.pattern_ids:
            self.pattern_ids.append(pattern_id)
    
    def calculate_utility(self, current_time, target_stations=[7, 8, 9], window_seconds=3600):
        """
        Calculate utility score for load shedding.
        Higher utility = more valuable = keep it
        Lower utility = less valuable = drop it
        
        Factors:
        - Chain length: Longer chains are closer to completion
        - Time remaining: More time left = higher chance of completion
        - Target station proximity: Is the last station near target stations?
        """
        # Factor 1: Chain length (longer = better)
        chain_length = len(self.events)
        
        # Factor 2: Time remaining in window (more time = better)
        elapsed = (current_time - self.first_timestamp).total_seconds()
        time_remaining = window_seconds - elapsed
        
        # Factor 3: Last station proximity to target
        # Try to get end station from last event
        try:
            last_event = self.events[-1]
            last_station = last_event.payload.get('end station id', None)
            
            if last_station is not None:
                # Calculate distance to nearest target station
                distance_to_target = min([abs(last_station - t) for t in target_stations])
            else:
                distance_to_target = 100  # High penalty if station unknown
        except (IndexError, KeyError, TypeError):
            distance_to_target = 100  # High penalty if can't determine station
        
        # Combine factors with weights (you can tune these)
        utility = (
            chain_length * 10.0 +           # Longer chains = +10 per event
            time_remaining / 60.0 -         # More time = better (in minutes)
            distance_to_target * 0.5        # Closer to target = better
        )
        
        return utility
