#!/usr/bin/env python3
"""
CitiBike Pattern Detection with Load Shedding Evaluation
CS-E4780 Project: Efficient Pattern Detection over Data Streams

Usage:
  python3 main.py --mode basic -d data.csv
  python3 main.py --mode load-shedding -d data.csv
"""

import sys
import time
import json
import logging
import argparse
import os
from datetime import datetime, timedelta
from typing import Tuple
from dataclasses import dataclass

# Add OpenCEP root directory to path for imports
opencep_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, opencep_root)

from CEP import *
from engine.LoadSheddingCEP import LoadSheddingCEP
from tree.LoadSheddingPatternMatchStorage import LoadSheddingConfig
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from condition.Condition import Variable, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition
from stream.Stream import InputStream, OutputStream
from stream.FileStream import FileInputStream
from plugin.citibike.CitiBikeFormatter import CitiBikeDataFormatter


def setup_logging(log_level=logging.INFO, log_file='citibike_evaluation.log'):
    """Setup logging configuration"""
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    if not os.path.dirname(log_file):
        log_file = os.path.join(logs_dir, log_file)
    
    logging.root.handlers.clear()
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)


@dataclass
class SimulatedLoadSheddingConfig:
    """Configuration for simulated load shedding (unused - kept for compatibility)"""
    enabled: bool = False
    latency_bound_percent: float = 100.0
    utility_threshold: float = 0.5
    max_partial_matches: int = 10000
    shedding_strategy: str = "utility"

@dataclass
class TestResult:
    """Container for test results"""
    mode: str
    events_processed: int
    processing_time: float
    wall_clock_time: float
    matches_found: int
    throughput: float
    recall_rate: float = 1.0
    load_shedding_triggered: bool = False
    baseline_matches: int = 0
    dropped_matches: int = 0


class ThrottledFileInputStream(InputStream):
    """Input stream that throttles ingestion to simulate burst patterns."""
    def __init__(self, file_path: str, ingest_rate: float = 0.0, burst_seq: list = None):
        super().__init__()
        
        if (not burst_seq) and (not ingest_rate or ingest_rate <= 0):
            with open(file_path, "r") as f:
                for line in f:
                    self._stream.put(line)
            self.close()
            return
            
        if (not burst_seq) and ingest_rate and ingest_rate > 0:
            interval = 1.0 / ingest_rate
            with open(file_path, "r") as f:
                for line in f:
                    self._stream.put(line)
                    if interval > 0:
                        time.sleep(interval)
            self.close()
            return
            
        with open(file_path, "r") as f:
            seg_idx = 0
            rate, dur = burst_seq[0]
            seg_end = time.time() + dur
            for line in f:
                now = time.time()
                if now >= seg_end:
                    seg_idx = (seg_idx + 1) % len(burst_seq)
                    rate, dur = burst_seq[seg_idx]
                    seg_end = now + dur
                self._stream.put(line)
                if rate > 0:
                    time.sleep(1.0 / rate)
        self.close()


class CitiBikeEvaluator:
    """Main evaluator class that handles all testing modes"""
    
    def __init__(self, data_file_path: str, log_level=logging.INFO, target_stations=None, ingest_rate: float = 0.0, burst_seq: str = ""):
        self.data_file_path = data_file_path
        self.logger = setup_logging(log_level)
        self.results = []
        self.target_stations = target_stations or ["7", "8", "9"]
        self.ingest_rate = float(ingest_rate or 0.0)
        self.burst_seq = self._parse_burst_seq(burst_seq)
        
        if not os.path.exists(data_file_path):
            raise FileNotFoundError(f"Data file not found: {data_file_path}")
        
        self.logger.info(f"Initialized with data file: {data_file_path}")
    
    def _parse_burst_seq(self, burst_seq_str: str):
        """Parse burst sequence string to list of (rate, duration) tuples."""
        if not burst_seq_str:
            return []
        try:
            parts = [p.strip() for p in burst_seq_str.split(',') if p.strip()]
            return [(float(r), float(d)) for r, d in (p.split(':') for p in parts)]
        except Exception:
            self.logger.warning("Invalid burst-seq format, ignoring")
            return []
    
    def create_hot_path_pattern(self, target_stations=None):
        """Create hot path pattern: SEQ(BikeTrip+ a[], BikeTrip b) with chaining and same bike.
        
        WHERE a[i+1].bike = a[i].bike AND a[i+1].start = a[i].end
        AND a[last].bike = b.bike AND b.end in {target_stations}
        WITHIN 1h
        """
        if target_stations is None:
            target_stations = self.target_stations
        pattern = Pattern(
            SeqOperator(
                # BikeTrip+ a[] - One or more chained trips by same bike
                KleeneClosureOperator(
                    PrimitiveEventStructure("BikeTrip", "a")
                ),
                # BikeTrip b - Final trip ending at target stations  
                PrimitiveEventStructure("BikeTrip", "b")
            ),
            # Full required conditions
            AndCondition(
                BinaryCondition(
                    Variable("a", lambda events: [e["bikeid"] for e in events] if isinstance(events, list) else events["bikeid"]),
                    Variable("a", lambda events: [e["bikeid"] for e in events] if isinstance(events, list) else events["bikeid"]),
                    relation_op=lambda bikes, _: all(bikes[i] == bikes[i+1] for i in range(len(bikes)-1)) if isinstance(bikes, list) and len(bikes) > 1 else True
                ),
                BinaryCondition(
                    Variable("a", lambda events: [(e["start station id"], e["end station id"]) for e in events] if isinstance(events, list) else [(events["start station id"], events["end station id"])]),
                    Variable("a", lambda events: [(e["start station id"], e["end station id"]) for e in events] if isinstance(events, list) else [(events["start station id"], events["end station id"])]),
                    relation_op=lambda stations, _: all(stations[i][1] == stations[i+1][0] for i in range(len(stations)-1)) if isinstance(stations, list) and len(stations) > 1 else True
                ),
                BinaryCondition(
                    Variable("a", lambda events: events[-1]["bikeid"] if isinstance(events, list) else events["bikeid"]),
                    Variable("b", lambda x: x["bikeid"]),
                    relation_op=lambda a_bike, b_bike: a_bike == b_bike if a_bike is not None and b_bike is not None else False
                ),
                SimpleCondition(
                    Variable("b", lambda x: x["end station id"]),
                    relation_op=lambda end_station: str(end_station) in target_stations if end_station is not None else False
                )
            ),
            timedelta(hours=1)
        )
        return pattern
    
    def _run_cep_with_event_logging(self, cep, events, output, formatter, max_events):
        """Run CEP with timeout monitoring."""
        self.logger.info(f"Starting CEP processing for {max_events} events")
        
        import threading
        
        def timeout_handler():
            print(f"WARNING: Still processing after 30s...")
            
        timeout_timer = threading.Timer(30.0, timeout_handler)
        timeout_timer.start()
        
        try:
            processing_time = cep.run(events, output, formatter)
            timeout_timer.cancel()
            print(f"CEP processing completed in {processing_time:.4f}s")
            return processing_time
        except Exception as e:
            timeout_timer.cancel()
            self.logger.error(f"CEP processing failed: {e}")
            raise
    
    def create_event_stream(self, max_events: int):
        """Create event stream from data file."""
        import tempfile
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
        
        with open(self.data_file_path, 'r') as f:
            for i, line in enumerate(f):
                if i >= max_events:
                    break
                temp_file.write(line)
        
        temp_file.close()
        
        if self.burst_seq or (self.ingest_rate and self.ingest_rate > 0):
            return ThrottledFileInputStream(temp_file.name, self.ingest_rate, self.burst_seq)
        else:
            file_input = FileInputStream(temp_file.name)
            os.unlink(temp_file.name)
            return file_input
    
    
    def run_basic_test(self, max_events=100):
        """Basic integration test."""
        print("=" * 60)
        print("BASIC INTEGRATION TEST")
        print("=" * 60)
        print(f"Events: {max_events}")
        print("-" * 60)
        
        try:
            print("Creating hot path pattern...")
            pattern = self.create_hot_path_pattern()
            
            load_shedding_config = LoadSheddingConfig(
                enabled=True,
                max_partial_matches=50,
                shedding_strategy="utility",
                aggressive_shedding=True,
                target_stations=set(self.target_stations)
            )
            
            cep = LoadSheddingCEP([pattern], load_shedding_config)
            events = self.create_event_stream(max_events)
            output = OutputStream()
            formatter = CitiBikeDataFormatter()
            
            print(f"\nProcessing {max_events} events...")
            start_time = time.time()
            processing_time = self._run_cep_with_event_logging(cep, events, output, formatter, max_events)
            wall_clock_time = time.time() - start_time
            
            matches = []
            if output.count() > 0:
                try:
                    internal_queue = output._stream.queue
                    for item in internal_queue:
                        if item is None:
                            break
                        matches.append(item)
                except Exception as e:
                    print(f"Error collecting matches: {e}")
            
            # Create result
            result = TestResult(
                mode="basic",
                events_processed=max_events,
                processing_time=processing_time,
                wall_clock_time=wall_clock_time,
                matches_found=len(matches),
                throughput=max_events / wall_clock_time if wall_clock_time > 0 else 0
            )
            
            print(f"\nTest PASSED")
            print(f"Events: {max_events}, Matches: {len(matches)}, Time: {processing_time:.4f}s")
            print(f"Throughput: {result.throughput:.1f} events/s")
            
            if matches:
                print(f"\nSample matches (showing first 3):")
                for i, match in enumerate(matches[:3]):
                    events_in_match = match.events
                    if len(events_in_match) >= 2:
                        # Get bike ID from any event that has it
                        bike_id = None
                        for event in events_in_match:
                            if event.payload.get('bikeid'):
                                bike_id = event.payload.get('bikeid')
                                break
                        
                        # For pattern SEQ(BikeTrip+ a[], BikeTrip b):
                        # a[] = events_in_match[:-1], b = events_in_match[-1]
                        a_events = events_in_match[:-1]  # All but last
                        b_event = events_in_match[-1]     # Last event
                        
                        # Return tuple format: (a[1].start, a[last].end, b.end)
                        if a_events:
                            # Debug: check what's actually in payload
                            if i == 0:  # Only for first match
                                print(f"    DEBUG: a_events[0].payload keys: {list(a_events[0].payload.keys())[:10]}")
                                print(f"    DEBUG: a_events[0].payload sample: {dict(list(a_events[0].payload.items())[:3])}")
                            a1_start = a_events[0].payload.get('start station id')
                            ai_end = a_events[-1].payload.get('end station id')
                        else:
                            a1_start = ai_end = None
                        b_end = b_event.payload.get('end station id')
                        
                        # Format output
                        def fmt(val):
                            return str(val) if val is not None else 'N/A'
                        
                        print(f"  Match {i+1}: Bike {fmt(bike_id)}, {len(events_in_match)} trips: ({fmt(a1_start)}, {fmt(ai_end)}, {fmt(b_end)})")
            
            self.results.append(result)
            return result
            
        except Exception as e:
            print(f"Integration test FAILED: {e}")
            self.logger.error(f"Basic test failed: {e}")
            return None
    
    def run_performance_test(self, test_sizes=[50, 100, 200, 500]):
        """Mode 2: Performance baseline measurement"""
        print("=" * 60)
        print("PERFORMANCE BASELINE TEST")
        print("=" * 60)
        print("Purpose: Measure performance scaling with different event counts")
        print(f"Test sizes: {test_sizes}")
        print("-" * 60)
        
        self.logger.info("Starting performance baseline test")
        results = []
        
        for size in test_sizes:
            print(f"\nTesting with {size} events...")
            
            try:
                # Create pattern and CEP engine
                pattern = self.create_hot_path_pattern()
                cep = CEP([pattern])
                
                # Create event stream
                events = self.create_event_stream(size)
                output = OutputStream()
                formatter = CitiBikeDataFormatter()
                
                # Run detection
                start_time = time.time()
                processing_time = cep.run(events, output, formatter)
                wall_clock_time = time.time() - start_time
                
                # Collect matches
                matches = []
                try:
                    for match in output:
                        if match is None:
                            break
                        matches.append(match)
                except StopIteration:
                    pass
                
                # Create result
                result = TestResult(
                    mode="performance",
                    events_processed=size,
                    processing_time=processing_time,
                    wall_clock_time=wall_clock_time,
                    matches_found=len(matches),
                    throughput=size / wall_clock_time if wall_clock_time > 0 else 0
                )
                
                print(f"  Results: {len(matches)} matches, {result.throughput:.1f} events/s")
                results.append(result)
                self.results.append(result)
                
            except Exception as e:
                print(f"  ERROR: {e}")
                self.logger.error(f"Performance test failed for size {size}: {e}")
        
        # Summary table
        if results:
            print(f"\nPerformance Summary:")
            print("Events\tTime(s)\tMatches\tThroughput(e/s)")
            print("-" * 40)
            for r in results:
                print(f"{r.events_processed}\t{r.wall_clock_time:.2f}\t{r.matches_found}\t{r.throughput:.1f}")
        
        return results
    
    def _run_detection(self, pattern, max_events: int, load_shedding_config: LoadSheddingConfig = None):
        """Run CEP on given pattern and return (processing_time, wall_time, matches_list)."""
        if load_shedding_config is None:
            cep = CEP([pattern])
        else:
            cep = LoadSheddingCEP([pattern], load_shedding_config)
        events = self.create_event_stream(max_events)
        output = OutputStream()
        formatter = CitiBikeDataFormatter()
        start_time = time.time()
        processing_time = cep.run(events, output, formatter)
        wall_time = time.time() - start_time
        # Collect matches
        matches = []
        try:
            for item in output:
                if item is None:
                    break
                matches.append(item)
        except StopIteration:
            pass
        return processing_time, wall_time, matches
    
    def _search_shedding_for_latency(self, pattern, max_events: int, target_wall_time: float, tol: float = 0.10):
        """Binary search over max_partial_matches to meet target latency. Returns best tuple."""
        low, high = 10, 2000
        best = None  # (max_pm, proc, wall, kept_matches, dropped_count)
        # Ensure we pass target stations into config
        target_station_set = set(self.target_stations)
        for _ in range(6):
            mid = (low + high) // 2
            cfg = LoadSheddingConfig(
                enabled=True,
                max_partial_matches=mid,
                shedding_strategy="utility",
                aggressive_shedding=True,
                target_stations=target_station_set
            )
            proc, wall, kept = self._run_detection(pattern, max_events, cfg)
            # Track best by (within target) or closest above target
            if best is None or abs(wall - target_wall_time) < abs(best[2] - target_wall_time):
                best = (mid, proc, wall, kept, 0)
            if wall > target_wall_time * (1 + tol):
                # Too slow -> shed more -> lower capacity
                high = max(low, mid - 1)
            elif wall < target_wall_time * (1 - tol):
                # Too fast -> can allow more partials for recall
                low = min(high, mid + 1)
            else:
                # Within tolerance
                return (mid, proc, wall, kept, 0)
        return best
    
    def simulate_load_shedding(self, matches_found: int, processing_time: float, 
                             config: SimulatedLoadSheddingConfig) -> Tuple[int, float, int]:
        """Simulate advanced load shedding with utility-based dropping"""
        if not config.enabled or config.latency_bound_percent >= 100.0:
            return matches_found, 1.0, 0  # No shedding
        
        self.logger.info(f"Applying load shedding: target {config.latency_bound_percent}% of baseline latency")
        
        # Calculate required shedding based on latency constraint
        target_latency = processing_time * (config.latency_bound_percent / 100.0)
        latency_reduction_needed = processing_time - target_latency
        
        if latency_reduction_needed <= 0:
            return matches_found, 1.0, 0
        
        # Estimate shedding factor based on latency reduction needed
        # More aggressive shedding for tighter latency bounds
        shedding_intensity = min(1.0, latency_reduction_needed / processing_time)
        
        if config.shedding_strategy == "utility":
            # Utility-based shedding: prioritize longer chains near target stations
            # Simulate dropping lower-utility partial matches
            utility_scores = self._simulate_utility_scores(matches_found)
            sorted_by_utility = sorted(enumerate(utility_scores), key=lambda x: x[1], reverse=True)
            
            # Keep high-utility matches, drop low-utility ones
            matches_to_keep = int(matches_found * (1.0 - shedding_intensity))
            remaining_matches = max(0, matches_to_keep)
            
        elif config.shedding_strategy == "random":
            # Random dropping
            remaining_matches = int(matches_found * (1.0 - shedding_intensity))
            
        elif config.shedding_strategy == "oldest":
            # Drop oldest partial matches (FIFO)
            remaining_matches = int(matches_found * (1.0 - shedding_intensity))
            
        else:
            # Default: proportional dropping
            remaining_matches = int(matches_found * (1.0 - shedding_intensity))
        
        remaining_matches = max(0, remaining_matches)
        dropped_matches = matches_found - remaining_matches
        recall_rate = remaining_matches / max(matches_found, 1)
        
        self.logger.info(f"Load shedding applied: kept {remaining_matches}/{matches_found} matches")
        self.logger.info(f"Shedding strategy: {config.shedding_strategy}, recall: {recall_rate:.3f}")
        
        return remaining_matches, recall_rate, dropped_matches
    
    def _simulate_utility_scores(self, num_matches: int) -> list:
        """Simulate utility scores for matches (higher = more valuable)"""
        import random
        # Simulate utility based on:
        # - Chain length (longer chains = higher utility)
        # - Proximity to target stations 7,8,9 
        # - Remaining window time
        utility_scores = []
        for i in range(num_matches):
            # Random utility with bias toward higher values for simulation
            base_utility = random.uniform(0.3, 1.0)
            # Boost utility for matches closer to completion
            completion_bonus = random.uniform(0.0, 0.3)
            utility_scores.append(min(1.0, base_utility + completion_bonus))
        return utility_scores
    
    def run_load_shedding_test(self, latency_bounds=[90, 70, 50, 30, 10], test_events=500):
        """Mode 3: Load shedding evaluation with measured recall and latency bounds."""
        print("=" * 60)
        print("LOAD SHEDDING EVALUATION TEST")
        print("=" * 60)
        print("Purpose: Evaluate load shedding impact on latency vs recall")
        print(f"Test events: {test_events}")
        print(f"Target stations: {self.target_stations}")
        print(f"Latency bounds: {latency_bounds}% of baseline (PROJECT REQUIREMENT)")
        print("-" * 60)
        print(f"NOTE: Pattern requires chained bike trips ending at target stations.")
        print(f"      For meaningful results, ensure: (1) Enough events (100+)")
        print(f"      (2) Valid target stations (use scripts/find_targets.py)")
        print("-" * 60)
        
        self.logger.info("Starting load shedding evaluation as per project requirements")
        
        # Step 1: Baseline without load shedding
        print("\nStep 1: Measuring baseline performance (no load shedding)...")
        try:
            pattern = self.create_hot_path_pattern()
            base_proc, base_wall, base_matches = self._run_detection(pattern, test_events, load_shedding_config=None)
            baseline_result = TestResult(
                mode="baseline",
                events_processed=test_events,
                processing_time=base_proc,
                wall_clock_time=base_wall,
                matches_found=len(base_matches),
                throughput=test_events / base_wall if base_wall > 0 else 0,
                baseline_matches=len(base_matches)
            )
            print(f"  Baseline: {len(base_matches)} matches, {baseline_result.throughput:.1f} events/s")
            print(f"  Processing time: {base_proc:.4f}s")
            
            if len(base_matches) == 0:
                print(f"  WARNING: No matches found in baseline!")
                print(f"  This pattern requires chained trips on same bike ending at stations {self.target_stations}")
                print(f"  Try: (1) More events (--max-events 100+), (2) Find actual targets (scripts/find_targets.py)")
                print(f"  Recall metrics will be meaningless with 0 baseline matches.")
            
            self.results.append(baseline_result)
        except Exception as e:
            print(f"  ERROR in baseline: {e}")
            self.logger.error(f"Baseline measurement failed: {e}")
            return []
        
        # Step 2: Measured recall under latency bounds via search over max_partial_matches
        print("\nStep 2: Measuring recall at latency bounds...")
        load_shedding_results = []
        for bound in latency_bounds:
            target_time = baseline_result.wall_clock_time * (bound / 100.0)
            print(f"\nTesting {bound}% latency bound (target <= {target_time:.4f}s)...")
            try:
                best = self._search_shedding_for_latency(pattern, test_events, target_time)
                if not best:
                    print("  Could not achieve target latency; reporting closest run.")
                    continue
                max_pm, proc, wall, kept, dropped = best
                
                # Calculate recall properly - handle zero baseline case
                if baseline_result.matches_found > 0:
                    recall_rate = len(kept) / baseline_result.matches_found
                else:
                    # No baseline matches - recall is undefined, but if we found any it's a bonus
                    recall_rate = 1.0 if len(kept) == 0 else float('inf')
                
                result = TestResult(
                    mode=f"load_shedding_{bound}%",
                    events_processed=test_events,
                    processing_time=proc,
                    wall_clock_time=wall,
                    matches_found=len(kept),
                    throughput=test_events / wall if wall > 0 else 0,
                    recall_rate=recall_rate if baseline_result.matches_found > 0 else 0.0,
                    load_shedding_triggered=True,
                    baseline_matches=baseline_result.matches_found,
                    dropped_matches=max(0, baseline_result.matches_found - len(kept))
                )
                print(f"  max_partial_matches={max_pm}")
                print(f"  Matches: {len(kept)} (baseline: {baseline_result.matches_found})")
                if baseline_result.matches_found > 0:
                    print(f"  Recall: {recall_rate:.3f} ({recall_rate*100:.1f}%)")
                else:
                    print(f"  Recall: N/A (no baseline matches)")
                print(f"  Measured latency: {wall:.4f}s ({(wall / baseline_result.wall_clock_time)*100:.1f}% of baseline)")
                load_shedding_results.append(result)
                self.results.append(result)
            except Exception as e:
                print(f"  ERROR: {e}")
                self.logger.error(f"Load shedding test failed for {bound}%: {e}")
        
        # Summary
        if load_shedding_results:
            print(f"\nLoad Shedding Summary:")
            print(f"Baseline matches: {baseline_result.matches_found}")
            print("Latency Bound\tRecall Rate\tMatches Kept\tBaseline")
            print("-" * 60)
            for r in load_shedding_results:
                bound = r.mode.split('_')[2]
                if baseline_result.matches_found > 0:
                    print(f"{bound}\t\t{r.recall_rate:.2f}\t\t{r.matches_found}\t\t{r.baseline_matches}")
                else:
                    print(f"{bound}\t\tN/A\t\t{r.matches_found}\t\t{r.baseline_matches}")
        
        return load_shedding_results
    
    def save_results(self, output_file='citibike_evaluation_results.json'):
        """Save all results to JSON file"""
        # Create logs directory if it doesn't exist
        logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        # Ensure output file is in logs directory
        if not os.path.dirname(output_file):
            output_file = os.path.join(logs_dir, output_file)
        
        report = {
            "evaluation_summary": {
                "total_tests": len(self.results),
                "data_file": self.data_file_path,
                "timestamp": datetime.now().isoformat()
            },
            "results": []
        }
        
        for result in self.results:
            report["results"].append({
                "mode": result.mode,
                "events_processed": result.events_processed,
                "processing_time": result.processing_time,
                "wall_clock_time": result.wall_clock_time,
                "matches_found": result.matches_found,
                "throughput": result.throughput,
                "recall_rate": result.recall_rate,
                "load_shedding_triggered": result.load_shedding_triggered
            })
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\nResults saved to: {output_file}")
        return report


def main():
    """Main function with mode selection"""
    parser = argparse.ArgumentParser(description='CitiBike Pattern Detection Evaluation')
    
    parser.add_argument('--mode', choices=['basic', 'performance', 'load-shedding', 'all'],
                       default='all', help='Evaluation mode to run')
    parser.add_argument('--data-file', '-d', required=True,
                       help='Path to the CitiBike CSV data file')
    parser.add_argument('--output-file', '-o', default='citibike_evaluation_results.json',
                       help='Output file for results')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')
    parser.add_argument('--max-events', type=int, default=10, help='Maximum events to process (default: 10 due to Kleene closure complexity)')
    parser.add_argument('--target-stations', nargs='+', default=["7", "8", "9"],
                       help='Target station IDs for pattern matching (default: 7 8 9 per project spec)')
    parser.add_argument('--ingest-rate', type=float, default=0.0,
                       help='Throttle ingestion to N events/second (simulates load)')
    parser.add_argument('--burst-seq', type=str, default='',
                       help='Burst sequence as rate:duration segments, e.g., "50:2,200:1"')
    
    args = parser.parse_args()
    
    # Setup logging level
    log_level = getattr(logging, args.log_level.upper())
    
    print("=" * 80)
    print("CITIBIKE PATTERN DETECTION WITH LOAD SHEDDING EVALUATION")
    print("=" * 80)
    print(f"Mode: {args.mode}")
    print(f"Data file: {args.data_file}")
    print(f"Max events: {args.max_events}")
    print(f"Target stations: {args.target_stations}")
    print("=" * 80)
    
    try:
        # Initialize evaluator
        evaluator = CitiBikeEvaluator(args.data_file, log_level, args.target_stations, args.ingest_rate, args.burst_seq)
        
        # Run selected mode(s)
        if args.mode == 'basic' or args.mode == 'all':
            evaluator.run_basic_test(args.max_events)
        
        if args.mode == 'performance' or args.mode == 'all':
            test_sizes = [50, 100, 200, min(500, args.max_events)]
            if args.max_events > 500:
                test_sizes.append(args.max_events)
            evaluator.run_performance_test(test_sizes)
        
        if args.mode == 'load-shedding' or args.mode == 'all':
            evaluator.run_load_shedding_test(test_events=args.max_events)
        
        # Save results
        evaluator.save_results(args.output_file)
        
        print(f"\nEvaluation completed successfully!")
        print(f"Log file: logs/citibike_evaluation.log")
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please check that the data file path is correct.")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)