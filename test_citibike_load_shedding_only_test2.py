#!/usr/bin/env python3
"""
Citi Bike Load Shedding Test Script - Test 2 Only
Tests load shedding implementation on Citi Bike trip data
"""

import sys
sys.path.insert(0, '/Users/thanhduong/Efficient Pattern Detection over Data Streams/OpenCEP')

from datetime import timedelta
from CEP import CEP
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from condition.BaseRelationCondition import EqCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable, BinaryCondition
from stream.FileStream import FileInputStream
from stream.Stream import OutputStream
from plugin.citibike.CitiBikeFormatter import CitiBikeDataFormatter
import json


def create_hot_path_pattern():
    """
    Create the hot path pattern from the project requirements.
    
    PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
    WHERE a[i+1].bike = a[i].bike 
      AND b.end in {7,8,9}
      AND a[last].bike = b.bike 
      AND a[i+1].start = a[i].end
    WITHIN 1h
    
    Simplified version for testing (due to OpenCEP limitations):
    We'll detect sequences of bike trips by the same bike ending at stations 7, 8, or 9
    """
    
    # Simpler pattern that OpenCEP can handle:
    # SEQ(BikeTrip a, BikeTrip+ b[], BikeTrip c)
    # WHERE a.bikeid = b.bikeid AND b.bikeid = c.bikeid
    #   AND c.end_station_id IN (7, 8, 9)
    # WITHIN 1 hour
    
    # For patterns with Kleene closure, we need to handle the fact that 'b' will be a list of events
    # We'll check that:
    # 1. a.bikeid == b[0].bikeid (first event in the Kleene closure matches 'a')
    # 2. All events in b have the same bikeid (done via KCIndexCondition or custom logic)
    # 3. b[last].bikeid == c.bikeid (last event in Kleene closure matches 'c')
    # 4. c.end_station_id in {7, 8, 9}
    
    # However, for simplicity and due to OpenCEP's condition evaluation model,
    # we'll use a simpler approach: check the first and last elements of the Kleene closure
    
    def get_bikeid_from_b(b_list):
        """Get bikeid from Kleene closure list - check first element"""
        if isinstance(b_list, list) and len(b_list) > 0:
            # b_list is a list of event payloads
            return b_list[0].get("bikeid")
        elif isinstance(b_list, dict):
            # Fallback if it's a single event
            return b_list.get("bikeid")
        return None
    
    def check_all_same_bikeid(b_list):
        """Check that all events in Kleene closure have same bikeid"""
        if isinstance(b_list, list) and len(b_list) > 0:
            first_bikeid = b_list[0].get("bikeid") if isinstance(b_list[0], dict) else None
            return all(event.get("bikeid") == first_bikeid for event in b_list if isinstance(event, dict))
        return True
    
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("BikeTrip", "a"),
            KleeneClosureOperator(PrimitiveEventStructure("BikeTrip", "b")),
            PrimitiveEventStructure("BikeTrip", "c")
        ),
        AndCondition(
            # Same bike for all trips: a.bikeid == first(b).bikeid
            BinaryCondition(
                Variable("a", lambda x: x.get("bikeid")),
                Variable("b", get_bikeid_from_b),
                relation_op=lambda x, y: x == y if x is not None and y is not None else False
            ),
            # All events in b have same bikeid AND last(b).bikeid == c.bikeid
            # Since we're checking a == first(b) and we need first(b) == last(b) == c,
            # we just need to check that last(b) == c
            BinaryCondition(
                Variable("b", lambda b_list: b_list[-1].get("bikeid") if isinstance(b_list, list) and len(b_list) > 0 else None),
                Variable("c", lambda x: x.get("bikeid")),
                relation_op=lambda x, y: x == y if x is not None and y is not None else False
            ),
            # Ensure all bikeids in b are the same
            BinaryCondition(
                Variable("b", check_all_same_bikeid),
                None,
                relation_op=lambda x: x is True
            ),
            # End station is 7, 8, or 9
            BinaryCondition(
                Variable("c", lambda x: x.get("end station id")),
                None,
                relation_op=lambda x: x in [7, 8, 9] if x is not None else False
            )
        ),
        timedelta(hours=1)
    )
    
    return pattern


def run_test(data_file, max_events=10000, enable_load_shedding=False, 
             max_partial_matches=1000, target_partial_matches=500):
    """
    Run Citi Bike pattern detection test.
    
    Args:
        data_file: Path to Citi Bike CSV file
        max_events: Maximum number of events to process
        enable_load_shedding: Whether to enable load shedding
        max_partial_matches: Threshold to trigger shedding
        target_partial_matches: Target size after shedding
    """
    
    print("=" * 70)
    print("Citi Bike Load Shedding Test")
    print("=" * 70)
    print(f"Data file: {data_file}")
    print(f"Max events: {max_events}")
    print(f"Load shedding: {'ENABLED' if enable_load_shedding else 'DISABLED'}")
    if enable_load_shedding:
        print(f"  Max partial matches: {max_partial_matches}")
        print(f"  Target partial matches: {target_partial_matches}")
    print("=" * 70)
    
    # Create pattern
    print("\n1. Creating hot path pattern...")
    pattern = create_hot_path_pattern()
    print("   Pattern: SEQ(BikeTrip a, BikeTrip+ b[], BikeTrip c)")
    print("   Condition: Same bikeid, end station in {7,8,9}")
    print("   Window: 1 hour")
    
    # Create CEP engine
    print("\n2. Creating CEP engine...")
    cep = CEP([pattern])
    
    # Enable load shedding if requested
    if enable_load_shedding:
        print("\n3. Enabling load shedding...")
        eval_mechanism = cep._CEP__evaluation_manager._SequentialEvaluationManager__eval_mechanism
        eval_mechanism.enable_load_shedding(
            max_partial_matches=max_partial_matches,
            target_partial_matches=target_partial_matches,
            latency_threshold=0.1
        )
    
    # Load event stream
    print(f"\n{'4' if enable_load_shedding else '3'}. Loading event stream...")
    events = FileInputStream(data_file)
    
    # Limit number of events for testing
    print(f"   Limiting to first {max_events} events...")
    limited_events = LimitedInputStream(events, max_events)
    
    # Create output stream
    output = OutputStream()
    
    # Run pattern detection
    print(f"\n{'5' if enable_load_shedding else '4'}. Running pattern detection...")
    formatter = CitiBikeDataFormatter()
    
    try:
        import time
        start_time = time.time()
        elapsed_time = cep.run(limited_events, output, formatter)
        end_time = time.time()
        
        print(f"\n{'6' if enable_load_shedding else '5'}. Detection completed!")
        print(f"   Processing time: {elapsed_time:.4f} seconds")
        print(f"   Wall clock time: {end_time - start_time:.4f} seconds")
        
        # Get matches
        matches = []
        try:
            for match in output:
                if match is None:
                    break
                matches.append(match)
        except StopIteration:
            pass
        
        print(f"\n✓ Found {len(matches)} matches!")
        
        # Show first few matches
        if matches:
            print("\nFirst few matches:")
            for i, match in enumerate(matches[:3]):
                print(f"\n   Match {i+1}:")
                for j, event in enumerate(match.events):
                    bike_id = event.payload.get('bikeid', 'Unknown')
                    end_station = event.payload.get('end station id', 'Unknown')
                    print(f"      Event {j+1}: BikeID={bike_id}, EndStation={end_station}, Time={event.timestamp}")
        
        # Get load shedding statistics if enabled
        if enable_load_shedding:
            print("\n" + "=" * 70)
            print("Load Shedding Statistics:")
            print("=" * 70)
            eval_mechanism = cep._CEP__evaluation_manager._SequentialEvaluationManager__eval_mechanism
            stats = eval_mechanism.get_load_shedding_stats()
            
            print(f"Events processed: {stats['event_count']}")
            print(f"Average latency: {stats['avg_latency']:.6f} seconds")
            print(f"Total latency: {stats['total_latency']:.4f} seconds")
            print(f"Shedding events: {stats['shedding_events']}")
            print(f"Max partial matches: {stats['max_partial_matches']}")
            print(f"Avg partial matches: {stats['avg_partial_matches']:.2f}")
            
            if stats['shedding_details']:
                print("\nShedding events details:")
                for event in stats['shedding_details'][:5]:  # Show first 5
                    print(f"  Event {event['event_count']}: dropped {event['dropped']} matches (latency={event['avg_latency']:.4f}s)")
        
        # Show summary
        print("\n" + "=" * 70)
        print("Summary:")
        print(f"  Total matches found: {len(matches)}")
        print(f"  Processing time: {elapsed_time:.4f} seconds")
        print(f"  Throughput: {max_events/elapsed_time:.2f} events/sec" if elapsed_time > 0 else "  Throughput: N/A")
        print("=" * 70)
        
        return {
            'matches': len(matches),
            'elapsed_time': elapsed_time,
            'wall_time': end_time - start_time,
            'load_shedding_enabled': enable_load_shedding,
            'stats': stats if enable_load_shedding else None
        }
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


class LimitedInputStream:
    """Wrapper to limit number of events from an input stream"""
    def __init__(self, stream, max_events):
        self.stream = stream
        self.max_events = max_events
        self.count = 0
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if self.count >= self.max_events:
            raise StopIteration
        self.count += 1
        return next(self.stream)


def main():
    """Run test with load shedding only"""
    
    data_file = "/Users/thanhduong/Downloads/2013-citibike-tripdata/201307-citibike-tripdata.csv"
    
    # Test 2: With load shedding
    print("\n\n")
    print("TEST 2: WITH LOAD SHEDDING")
    print("=" * 70)
    result_shedding = run_test(
        data_file=data_file,
        max_events=5000,
        enable_load_shedding=True,
        max_partial_matches=500,
        target_partial_matches=250
    )


if __name__ == "__main__":
    main()
