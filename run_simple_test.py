#!/usr/bin/env python3
"""
Simple test to see how OpenCEP works
This test will:
1. Load stock price events
2. Define a simple pattern (Google stock ascending)
3. Run pattern detection
4. Show matches found
"""

import sys
sys.path.insert(0, '/Users/thanhduong/Efficient Pattern Detection over Data Streams/OpenCEP')

from datetime import timedelta
from CEP import CEP
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.BaseRelationCondition import SmallerThanCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable
from stream.FileStream import FileInputStream, FileOutputStream
from stream.Stream import OutputStream
from plugin.stocks.Stocks import MetastockDataFormatter

def main():
    print("=" * 60)
    print("OpenCEP Simple Test - Google Stock Ascending Pattern")
    print("=" * 60)
    
    # Define a simple pattern
    # PATTERN SEQ(GOOG a, GOOG b, GOOG c)
    # WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice
    # WITHIN 3 minutes
    print("\n1. Defining pattern...")
    print("   Pattern: SEQ(GOOG a, GOOG b, GOOG c)")
    print("   Condition: a.PeakPrice < b.PeakPrice < c.PeakPrice")
    print("   Window: 3 minutes")
    
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("GOOG", "a"),
            PrimitiveEventStructure("GOOG", "b"),
            PrimitiveEventStructure("GOOG", "c")
        ),
        AndCondition(
            SmallerThanCondition(
                Variable("a", lambda x: x["Peak Price"]),
                Variable("b", lambda x: x["Peak Price"])
            ),
            SmallerThanCondition(
                Variable("b", lambda x: x["Peak Price"]),
                Variable("c", lambda x: x["Peak Price"])
            )
        ),
        timedelta(minutes=3)
    )
    
    # Create CEP engine
    print("\n2. Creating CEP engine...")
    cep = CEP([pattern])
    
    # Load event stream
    print("\n3. Loading event stream...")
    event_file = "/Users/thanhduong/Efficient Pattern Detection over Data Streams/OpenCEP/test/EventFiles/NASDAQ_MEDIUM.txt"
    events = FileInputStream(event_file)
    
    # Create output stream
    output = OutputStream()
    
    # Run pattern detection
    print("\n4. Running pattern detection...")
    formatter = MetastockDataFormatter()
    
    try:
        elapsed_time = cep.run(events, output, formatter)
        
        print(f"\n5. Detection completed in {elapsed_time:.4f} seconds")
        
        # Get matches from output stream
        matches = []
        
        print("\n6. Collecting matches...")
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
                    print(f"      Event {j+1}: {event.event_type} at {event.timestamp}")
                    print(f"              Peak Price: {event.payload.get('Peak Price', 'N/A')}")
        
        # Show statistics
        print("\n" + "=" * 60)
        print("Summary:")
        print(f"  Total matches found: {len(matches)}")
        print(f"  Processing time: {elapsed_time:.4f} seconds")
        print(f"  Throughput: {elapsed_time/max(1, len(matches)):.4f} sec/match")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
