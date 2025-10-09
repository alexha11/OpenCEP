#!/usr/bin/env python3
"""
Simple CEP test to identify performance bottlenecks
"""

import sys
import time
from datetime import timedelta
from typing import List

from CEP import CEP
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.BaseRelationCondition import SmallerThanCondition
from condition.Condition import Variable
from stream.FileInputStream import FileInputStream
from evaluation.TreeBasedEvaluationMechanismParameters import TreeBasedEvaluationMechanismParameters
from tree.TreeStorageParameters import TreeStorageParameters
from stream.CsvFormatter import CsvFormatter
from stream.OutputStream import OutputStream

sys.path.insert(0, '.')


def create_simple_pattern():
    """Create a simple sequential pattern (no Kleene closure)"""
    print("Creating simple sequential pattern: SEQ(BikeTrip a, BikeTrip b)")
    
    # Simple sequential pattern: BikeTrip followed by BikeTrip
    pattern_structure = SeqOperator(
        PrimitiveEventStructure("BikeTrip", "a"),
        PrimitiveEventStructure("BikeTrip", "b")
    )
    
    # Same bike constraint  
    condition = SmallerThanCondition(
        Variable("a", lambda x: 0),  # Always true condition for testing
        Variable("b", lambda x: 1)
    )
    
    time_window = timedelta(hours=2)
    
    return Pattern(pattern_structure, condition, time_window)


def create_temp_csv(lines: int = 10):
    """Create a temporary CSV file with limited lines"""
    import csv
    import tempfile
    
    data_file = "/Users/kanetran29/Aalto/CSE4780/Project01/2013-citibike-tripdata/201306-citibike-tripdata.csv"
    
    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    
    with open(data_file, 'r') as source:
        reader = csv.reader(source)
        writer = csv.writer(temp_file)
        
        # Copy header
        header = next(reader)
        writer.writerow(header)
        
        # Copy limited lines
        for i, row in enumerate(reader):
            if i >= lines:
                break
            writer.writerow(row)
    
    temp_file.close()
    print(f"Created temp file with {lines} data lines: {temp_file.name}")
    return temp_file.name


def run_simple_test(events: int = 5):
    """Run a simple CEP test"""
    print(f"\n{'=' * 60}")
    print(f"SIMPLE CEP TEST - {events} EVENTS")
    print(f"{'=' * 60}")
    
    # Create simple pattern (no Kleene closure)
    pattern = create_simple_pattern()
    
    # Create CEP engine
    print("Creating standard CEP engine...")
    cep = CEP([pattern])
    
    # Create temporary data file
    temp_file = create_temp_csv(events)
    
    try:
        # Create input stream
        print(f"Creating input stream from {temp_file}")
        events_stream = FileInputStream(temp_file)
        
        # Create output components
        output_stream = OutputStream()
        data_formatter = CsvFormatter()
        
        # Run CEP
        print("Running CEP pattern detection...")
        start_time = time.time()
        
        processing_time = cep.run(events_stream, output_stream, data_formatter)
        
        wall_time = time.time() - start_time
        
        print(f"\n✅ CEP completed successfully!")
        print(f"Processing time: {processing_time:.4f}s")
        print(f"Wall time: {wall_time:.4f}s")
        print(f"Matches found: {len(output_stream.events)}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ CEP test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up temp file
        import os
        try:
            os.unlink(temp_file)
        except:
            pass


if __name__ == "__main__":
    print("OpenCEP Simple Performance Test")
    print("Testing basic CEP functionality without Kleene closure")
    
    # Test with increasing event counts
    for event_count in [5, 10, 15, 20]:
        success = run_simple_test(event_count)
        if not success:
            print(f"❌ Test failed at {event_count} events")
            break
        print(f"✅ Test passed with {event_count} events")
        
        # If even simple patterns are slow, we have a deeper issue
        if event_count == 20:
            print("\n🎉 Simple patterns work - issue is specifically with Kleene closure")
        
        time.sleep(1)  # Brief pause between tests
    
    print("\nTest complete!")