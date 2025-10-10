#!/usr/bin/env python3
"""
CitiBike Load Shedding Evaluation System
CS-E4780 Project: Efficient Pattern Detection over Data Streams

Features:
- Load shedding evaluation with latency bounds
- Scalability testing across workloads  
- Performance analysis with CPU/memory monitoring
- Stage-by-stage breakdown analysis
- Visualizations and comprehensive metrics

Usage:
  python3 main.py --mode load-shedding -d data.csv --max-events 20
  python3 main.py --mode scalability -d data.csv
  python3 main.py --mode all -d data.csv
"""

import sys
import time
import json
import logging
import argparse
import os
import psutil
import threading
import subprocess
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

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
from stream.Stream import InputStream, OutputStream
from stream.FileStream import FileInputStream
from plugin.citibike.CitiBikeFormatter import CitiBikeDataFormatter


@dataclass
class SystemMetrics:
    """System resource utilization metrics."""
    cpu_percent: float
    memory_percent: float
    memory_used_gb: float
    cpu_cores_used: int
    timestamp: float
    stage: str = ""


@dataclass
class StagePerformance:
    """Performance metrics for a specific stage."""
    stage_name: str
    duration: float
    cpu_avg: float
    cpu_peak: float
    memory_avg_gb: float
    memory_peak_gb: float
    events_processed: int
    matches_generated: int
    throughput: float


@dataclass
class EnhancedTestResult:
    """Enhanced test result with comprehensive metrics."""
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
    
    # Enhanced metrics
    cpu_utilization_avg: float = 0.0
    cpu_utilization_peak: float = 0.0
    memory_usage_avg_gb: float = 0.0
    memory_usage_peak_gb: float = 0.0
    cpu_cores_utilized: int = 0
    stage_breakdown: List[StagePerformance] = None
    
    # Scalability metrics
    events_per_second: float = 0.0
    matches_per_second: float = 0.0
    memory_efficiency: float = 0.0  # matches per MB


class SystemMonitor:
    """Monitor system resource usage during CEP processing."""
    
    def __init__(self):
        self.metrics: List[SystemMetrics] = []
        self.monitoring = False
        self.monitor_thread = None
        self.current_stage = "init"
        
    def start_monitoring(self, stage: str = "processing"):
        """Start system monitoring."""
        self.current_stage = stage
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
    def stop_monitoring(self):
        """Stop system monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)
            
    def set_stage(self, stage: str):
        """Update current processing stage."""
        self.current_stage = stage
        
    def _monitor_loop(self):
        """Main monitoring loop."""
        while self.monitoring:
            try:
                # Get CPU usage
                cpu_percent = psutil.cpu_percent(interval=0.1)
                
                # Get memory usage
                memory = psutil.virtual_memory()
                
                # Get CPU count being used (approximation)
                cpu_count = psutil.cpu_count()
                cpu_cores_used = max(1, int(cpu_percent / 100 * cpu_count))
                
                metric = SystemMetrics(
                    cpu_percent=cpu_percent,
                    memory_percent=memory.percent,
                    memory_used_gb=memory.used / (1024**3),
                    cpu_cores_used=cpu_cores_used,
                    timestamp=time.time(),
                    stage=self.current_stage
                )
                
                self.metrics.append(metric)
                time.sleep(0.1)  # Monitor every 100ms
                
            except Exception as e:
                logging.error(f"Monitoring error: {e}")
                break
                
    def get_stage_metrics(self, stage: str) -> List[SystemMetrics]:
        """Get metrics for a specific stage."""
        return [m for m in self.metrics if m.stage == stage]
        
    def get_summary_stats(self) -> Dict:
        """Get summary statistics."""
        if not self.metrics:
            return {}
            
        cpu_values = [m.cpu_percent for m in self.metrics]
        memory_values = [m.memory_used_gb for m in self.metrics]
        
        return {
            "cpu_avg": np.mean(cpu_values),
            "cpu_peak": np.max(cpu_values),
            "memory_avg_gb": np.mean(memory_values),
            "memory_peak_gb": np.max(memory_values),
            "cpu_cores_avg": np.mean([m.cpu_cores_used for m in self.metrics]),
            "duration": self.metrics[-1].timestamp - self.metrics[0].timestamp if len(self.metrics) > 1 else 0
        }


class EnhancedCitiBikeEvaluator:
    """Enhanced evaluator with comprehensive performance analysis."""
    
    def __init__(self, data_file_path: str, log_level=logging.INFO, target_stations=None, auto_targets: bool = False, target_select: str = "balanced"):
        self.data_file_path = data_file_path
        self.logger = self._setup_logging(log_level)
        self.results: List[EnhancedTestResult] = []
        self.target_stations = target_stations or ["7", "8", "9"]
        self.monitor = SystemMonitor()
        self.auto_targets = bool(auto_targets)
        self.target_select = target_select or "balanced"
        
        if not os.path.exists(data_file_path):
            raise FileNotFoundError(f"Data file not found: {data_file_path}")

    def _auto_detect_targets(self, max_events: int, top: int = 3, mode: str = None) -> List[str]:
        """Auto-detect target stations from first N events using scripts/find_targets.py.
        Returns a list of station IDs as strings, or empty list on failure.
        """
        try:
            script_path = os.path.join(opencep_root, 'scripts', 'find_targets.py')
            if not os.path.exists(script_path):
                self.logger.warning("find_targets.py script not found; cannot auto-detect targets")
                return []
            sel_mode = mode or self.target_select or 'balanced'
            cmd = [sys.executable, script_path, self.data_file_path, '--max-events', str(max_events), '--top', str(top), '--mode', sel_mode]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if proc.returncode != 0:
                self.logger.warning(f"Auto-detect targets failed: {proc.stderr.strip()}")
                return []
            stations_line = proc.stdout.strip()
            stations = [s for s in stations_line.split() if s]
            if len(stations) != top:
                self.logger.warning(f"Auto-detect returned {len(stations)} targets, expected {top}")
            return stations
        except Exception as e:
            self.logger.warning(f"Exception during auto-detect targets: {e}")
            return []
    
    def _setup_logging(self, log_level):
        """Setup enhanced logging."""
        logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        log_file = os.path.join(logs_dir, 'enhanced_evaluation.log')
        
        logging.root.handlers.clear()
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        return logging.getLogger(__name__)
    
    def create_hot_path_pattern(self, target_stations=None):
        """Create hot path pattern matching project specification.
        
        PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
        WHERE a[i+1].bike = a[i].bike AND b.end in {target_stations}
        AND a[last].bike = b.bike AND a[i+1].start = a[i].end
        WITHIN 1h
        RETURN (a[1].start, a[i].end, b.end)
        """
        if target_stations is None:
            target_stations = self.target_stations
            
        pattern = Pattern(
            SeqOperator(
                KleeneClosureOperator(PrimitiveEventStructure("BikeTrip", "a")),
                PrimitiveEventStructure("BikeTrip", "b")
            ),
            AndCondition(
                # a[i+1].bike = a[i].bike (same bike throughout chain)
                BinaryCondition(
                    Variable("a", lambda events: [e["bikeid"] for e in events] if isinstance(events, list) else [events["bikeid"]]),
                    Variable("a", lambda events: [e["bikeid"] for e in events] if isinstance(events, list) else [events["bikeid"]]),
                    relation_op=lambda bikes, _: all(bikes[i] == bikes[i+1] for i in range(len(bikes)-1)) if isinstance(bikes, list) and len(bikes) > 1 else True
                ),
                # a[i+1].start = a[i].end (chain continuity)
                BinaryCondition(
                    Variable("a", lambda events: [(e["start station id"], e["end station id"]) for e in events] if isinstance(events, list) else [(events["start station id"], events["end station id"])]),
                    Variable("a", lambda events: [(e["start station id"], e["end station id"]) for e in events] if isinstance(events, list) else [(events["start station id"], events["end station id"])]),
                    relation_op=lambda stations, _: all(stations[i][1] == stations[i+1][0] for i in range(len(stations)-1)) if isinstance(stations, list) and len(stations) > 1 else True
                ),
                # a[last].bike = b.bike (same bike connecting a[] to b)
                BinaryCondition(
                    Variable("a", lambda events: events[-1]["bikeid"] if isinstance(events, list) and events else events["bikeid"]),
                    Variable("b", lambda x: x["bikeid"]),
                    relation_op=lambda a_bike, b_bike: a_bike == b_bike if a_bike is not None and b_bike is not None else False
                ),
                # b.end in {target_stations}
                SimpleCondition(
                    Variable("b", lambda x: x["end station id"]),
                    relation_op=lambda end_station: str(end_station) in target_stations if end_station is not None else False
                )
            ),
            timedelta(hours=1)
        )
        return pattern
    
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
        
        file_input = FileInputStream(temp_file.name)
        os.unlink(temp_file.name)
        return file_input
    
    def _run_detection_with_monitoring(self, pattern, max_events: int, 
                                     load_shedding_config: LoadSheddingConfig = None) -> EnhancedTestResult:
        """Run CEP with comprehensive monitoring."""
        
        # Initialize monitoring
        self.monitor = SystemMonitor()
        stage_performances = []
        
        # Stage 1: Setup
        self.monitor.start_monitoring("setup")
        start_time = time.time()
        
        if load_shedding_config is None:
            cep = CEP([pattern])
            mode = "baseline"
        else:
            cep = LoadSheddingCEP([pattern], load_shedding_config)
            mode = f"load_shedding_{load_shedding_config.max_partial_matches}"
            
        setup_time = time.time() - start_time
        setup_metrics = self.monitor.get_summary_stats()
        stage_performances.append(StagePerformance(
            stage_name="setup",
            duration=setup_time,
            cpu_avg=setup_metrics.get("cpu_avg", 0),
            cpu_peak=setup_metrics.get("cpu_peak", 0),
            memory_avg_gb=setup_metrics.get("memory_avg_gb", 0),
            memory_peak_gb=setup_metrics.get("memory_peak_gb", 0),
            events_processed=0,
            matches_generated=0,
            throughput=0
        ))
        
        # Stage 2: Event Stream Creation
        self.monitor.set_stage("stream_creation")
        stream_start = time.time()
        events = self.create_event_stream(max_events)
        output = OutputStream()
        formatter = CitiBikeDataFormatter()
        stream_time = time.time() - stream_start
        
        stream_metrics = self.monitor.get_summary_stats()
        stage_performances.append(StagePerformance(
            stage_name="stream_creation",
            duration=stream_time,
            cpu_avg=stream_metrics.get("cpu_avg", 0),
            cpu_peak=stream_metrics.get("cpu_peak", 0),
            memory_avg_gb=stream_metrics.get("memory_avg_gb", 0),
            memory_peak_gb=stream_metrics.get("memory_peak_gb", 0),
            events_processed=max_events,
            matches_generated=0,
            throughput=max_events / stream_time if stream_time > 0 else 0
        ))
        
        # Stage 3: Pattern Processing
        self.monitor.set_stage("pattern_processing")
        processing_start = time.time()
        processing_time = cep.run(events, output, formatter)
        wall_time = time.time() - processing_start
        
        # Stage 4: Results Collection
        self.monitor.set_stage("results_collection")
        collection_start = time.time()
        matches = []
        try:
            for item in output:
                if item is None:
                    break
                matches.append(item)
        except StopIteration:
            pass
        collection_time = time.time() - collection_start
        
        self.monitor.stop_monitoring()
        
        # Calculate comprehensive metrics
        overall_metrics = self.monitor.get_summary_stats()
        processing_metrics = self.monitor.get_stage_metrics("pattern_processing")
        
        processing_stage_metrics = {}
        if processing_metrics:
            cpu_values = [m.cpu_percent for m in processing_metrics]
            memory_values = [m.memory_used_gb for m in processing_metrics]
            processing_stage_metrics = {
                "cpu_avg": np.mean(cpu_values),
                "cpu_peak": np.max(cpu_values),
                "memory_avg_gb": np.mean(memory_values),
                "memory_peak_gb": np.max(memory_values)
            }
        
        stage_performances.append(StagePerformance(
            stage_name="pattern_processing",
            duration=processing_time,
            cpu_avg=processing_stage_metrics.get("cpu_avg", 0),
            cpu_peak=processing_stage_metrics.get("cpu_peak", 0),
            memory_avg_gb=processing_stage_metrics.get("memory_avg_gb", 0),
            memory_peak_gb=processing_stage_metrics.get("memory_peak_gb", 0),
            events_processed=max_events,
            matches_generated=len(matches),
            throughput=max_events / processing_time if processing_time > 0 else 0
        ))
        
        stage_performances.append(StagePerformance(
            stage_name="results_collection",
            duration=collection_time,
            cpu_avg=overall_metrics.get("cpu_avg", 0),
            cpu_peak=overall_metrics.get("cpu_peak", 0),
            memory_avg_gb=overall_metrics.get("memory_avg_gb", 0),
            memory_peak_gb=overall_metrics.get("memory_peak_gb", 0),
            events_processed=0,
            matches_generated=len(matches),
            throughput=len(matches) / collection_time if collection_time > 0 else 0
        ))
        
        # Calculate efficiency metrics
        memory_efficiency = len(matches) / max(overall_metrics.get("memory_avg_gb", 0.1), 0.1)
        
        result = EnhancedTestResult(
            mode=mode,
            events_processed=max_events,
            processing_time=processing_time,
            wall_clock_time=wall_time,
            matches_found=len(matches),
            throughput=max_events / wall_time if wall_time > 0 else 0,
            cpu_utilization_avg=overall_metrics.get("cpu_avg", 0),
            cpu_utilization_peak=overall_metrics.get("cpu_peak", 0),
            memory_usage_avg_gb=overall_metrics.get("memory_avg_gb", 0),
            memory_usage_peak_gb=overall_metrics.get("memory_peak_gb", 0),
            cpu_cores_utilized=int(overall_metrics.get("cpu_cores_avg", 1)),
            stage_breakdown=stage_performances,
            events_per_second=max_events / wall_time if wall_time > 0 else 0,
            matches_per_second=len(matches) / wall_time if wall_time > 0 else 0,
            memory_efficiency=memory_efficiency
        )
        
        return result
    
    def run_scalability_test(self, event_sizes=[5, 10, 15, 20, 25], max_partial_matches=100):
        """Run scalability tests across different workload sizes."""
        print("\n" + "="*80)
        print("SCALABILITY ANALYSIS")
        print("="*80)
        
        scalability_results = []
        pattern = self.create_hot_path_pattern()
        
        for event_count in event_sizes:
            print(f"\nTesting with {event_count} events...")
            
            # Test baseline
            baseline_result = self._run_detection_with_monitoring(pattern, event_count, None)
            baseline_result.mode = f"baseline_{event_count}events"
            
            # Test load shedding
            config = LoadSheddingConfig(
                enabled=True,
                max_partial_matches=max_partial_matches,
                shedding_strategy="utility",
                aggressive_shedding=True,
                target_stations=set(self.target_stations)
            )
            
            shedding_result = self._run_detection_with_monitoring(pattern, event_count, config)
            shedding_result.mode = f"load_shedding_{event_count}events"
            shedding_result.recall_rate = shedding_result.matches_found / max(baseline_result.matches_found, 1)
            
            scalability_results.extend([baseline_result, shedding_result])
            
            print(f"  Baseline: {baseline_result.matches_found} matches, {baseline_result.throughput:.1f} events/s")
            print(f"  Load Shedding: {shedding_result.matches_found} matches, {shedding_result.throughput:.1f} events/s, {shedding_result.recall_rate:.2f} recall")
            
        self.results.extend(scalability_results)
        self._generate_scalability_visualizations(scalability_results, event_sizes)
        return scalability_results
    
    def run_enhanced_load_shedding_test(self, latency_bounds=[90, 70, 50, 30, 10], test_events=15):
        """Run enhanced load shedding evaluation."""
        print("\n" + "="*80)
        print("ENHANCED LOAD SHEDDING EVALUATION")
        print("="*80)
        
        # Use a more reasonable event count to avoid exponential explosion
        pattern = self.create_hot_path_pattern()
        
        # Baseline measurement
        print(f"\nStep 1: Baseline measurement ({test_events} events)...")
        baseline_result = self._run_detection_with_monitoring(pattern, test_events, None)
        baseline_result.mode = "baseline"

        # If baseline has zero matches, auto-detect targets and rerun baseline
        if baseline_result.matches_found == 0 and self.auto_targets:
            print("  Baseline found 0 matches. Auto-detecting target stations for this window...")
            detected = self._auto_detect_targets(test_events, top=3)
            if detected:
                print(f"  Auto-detected target stations: {detected}")
                self.target_stations = detected
                # Recreate pattern with new targets and rerun baseline
                pattern = self.create_hot_path_pattern(self.target_stations)
                baseline_result = self._run_detection_with_monitoring(pattern, test_events, None)
                baseline_result.mode = "baseline"
            else:
                print("  Auto-detection failed; continuing with original targets.")
        
        self.results.append(baseline_result)
        
        print(f"  Matches: {baseline_result.matches_found}")
        print(f"  Processing time: {baseline_result.processing_time:.4f}s")
        print(f"  CPU usage: {baseline_result.cpu_utilization_avg:.1f}% avg, {baseline_result.cpu_utilization_peak:.1f}% peak")
        print(f"  Memory usage: {baseline_result.memory_usage_avg_gb:.2f}GB avg, {baseline_result.memory_usage_peak_gb:.2f}GB peak")
        
        # Load shedding tests with different aggressiveness levels
        shedding_results = []
        
        for bound in latency_bounds:
            target_time = baseline_result.wall_clock_time * (bound / 100.0)
            print(f"\nStep 2.{bound}: Testing {bound}% latency bound (target ≤ {target_time:.4f}s)...")
            
            # Binary search for optimal max_partial_matches
            best_config = self._find_optimal_shedding_config(pattern, test_events, target_time, baseline_result.matches_found)
            
            if best_config:
                config, result = best_config
                result.mode = f"load_shedding_{bound}%"
                result.recall_rate = result.matches_found / max(baseline_result.matches_found, 1)
                result.baseline_matches = baseline_result.matches_found
                result.dropped_matches = max(0, baseline_result.matches_found - result.matches_found)
                result.load_shedding_triggered = True
                
                shedding_results.append(result)
                self.results.append(result)
                
                print(f"  Matches: {result.matches_found} (recall: {result.recall_rate:.2f})")
                print(f"  Wall time: {result.wall_clock_time:.4f}s ({(result.wall_clock_time/baseline_result.wall_clock_time)*100:.1f}% of baseline)")
                print(f"  CPU: {result.cpu_utilization_avg:.1f}% avg, Memory: {result.memory_usage_avg_gb:.2f}GB avg")
        
        self._generate_load_shedding_visualizations(baseline_result, shedding_results)
        return shedding_results
    
    def _find_optimal_shedding_config(self, pattern, max_events: int, target_time: float, baseline_matches: int) -> Optional[Tuple]:
        """Find optimal shedding configuration using binary search."""
        low, high = 5, 200
        best_result = None
        best_config = None
        
        for iteration in range(5):  # Limited iterations for performance
            mid = (low + high) // 2
            
            config = LoadSheddingConfig(
                enabled=True,
                max_partial_matches=mid,
                shedding_strategy="utility",
                aggressive_shedding=True,
                target_stations=set(self.target_stations)
            )
            
            result = self._run_detection_with_monitoring(pattern, max_events, config)
            
            if best_result is None or abs(result.wall_clock_time - target_time) < abs(best_result.wall_clock_time - target_time):
                best_result = result
                best_config = config
            
            if result.wall_clock_time > target_time * 1.1:
                high = max(low, mid - 1)  # Too slow, need more aggressive shedding
            elif result.wall_clock_time < target_time * 0.9:
                low = min(high, mid + 1)  # Too fast, can be less aggressive
            else:
                break  # Close enough
        
        return (best_config, best_result) if best_result else None
    
    def _generate_scalability_visualizations(self, results: List[EnhancedTestResult], event_sizes: List[int]):
        """Generate scalability analysis visualizations."""
        try:
            # Set up the plot style
            plt.style.use('seaborn-v0_8')
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('CitiBike CEP Scalability Analysis', fontsize=16)
            
            # Separate baseline and load shedding results
            baseline_results = [r for r in results if 'baseline' in r.mode]
            shedding_results = [r for r in results if 'load_shedding' in r.mode]
            
            # Plot 1: Throughput vs Event Count
            axes[0, 0].plot([r.events_processed for r in baseline_results], 
                          [r.throughput for r in baseline_results], 
                          'b-o', label='Baseline', linewidth=2)
            axes[0, 0].plot([r.events_processed for r in shedding_results], 
                          [r.throughput for r in shedding_results], 
                          'r-s', label='Load Shedding', linewidth=2)
            axes[0, 0].set_xlabel('Event Count')
            axes[0, 0].set_ylabel('Throughput (events/sec)')
            axes[0, 0].set_title('Throughput Scalability')
            axes[0, 0].legend()
            axes[0, 0].grid(True, alpha=0.3)
            
            # Plot 2: CPU Utilization vs Event Count
            axes[0, 1].plot([r.events_processed for r in baseline_results], 
                          [r.cpu_utilization_avg for r in baseline_results], 
                          'b-o', label='Baseline Avg', linewidth=2)
            axes[0, 1].plot([r.events_processed for r in baseline_results], 
                          [r.cpu_utilization_peak for r in baseline_results], 
                          'b--o', label='Baseline Peak', alpha=0.7)
            axes[0, 1].plot([r.events_processed for r in shedding_results], 
                          [r.cpu_utilization_avg for r in shedding_results], 
                          'r-s', label='Load Shedding Avg', linewidth=2)
            axes[0, 1].set_xlabel('Event Count')
            axes[0, 1].set_ylabel('CPU Utilization (%)')
            axes[0, 1].set_title('CPU Resource Usage')
            axes[0, 1].legend()
            axes[0, 1].grid(True, alpha=0.3)
            
            # Plot 3: Memory Usage vs Event Count
            axes[1, 0].plot([r.events_processed for r in baseline_results], 
                          [r.memory_usage_avg_gb for r in baseline_results], 
                          'b-o', label='Baseline', linewidth=2)
            axes[1, 0].plot([r.events_processed for r in shedding_results], 
                          [r.memory_usage_avg_gb for r in shedding_results], 
                          'r-s', label='Load Shedding', linewidth=2)
            axes[1, 0].set_xlabel('Event Count')
            axes[1, 0].set_ylabel('Memory Usage (GB)')
            axes[1, 0].set_title('Memory Resource Usage')
            axes[1, 0].legend()
            axes[1, 0].grid(True, alpha=0.3)
            
            # Plot 4: Matches Found vs Event Count
            axes[1, 1].plot([r.events_processed for r in baseline_results], 
                          [r.matches_found for r in baseline_results], 
                          'b-o', label='Baseline', linewidth=2)
            axes[1, 1].plot([r.events_processed for r in shedding_results], 
                          [r.matches_found for r in shedding_results], 
                          'r-s', label='Load Shedding', linewidth=2)
            axes[1, 1].set_xlabel('Event Count')
            axes[1, 1].set_ylabel('Matches Found')
            axes[1, 1].set_title('Pattern Detection Effectiveness')
            axes[1, 1].legend()
            axes[1, 1].grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            # Save the plot
            plots_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'plots')
            os.makedirs(plots_dir, exist_ok=True)
            plt.savefig(os.path.join(plots_dir, 'scalability_analysis.png'), dpi=300, bbox_inches='tight')
            print(f"\nScalability visualization saved to logs/plots/scalability_analysis.png")
            
            # Don't show plot in terminal environment
            plt.close()
            
        except Exception as e:
            self.logger.warning(f"Could not generate scalability visualizations: {e}")
    
    def _generate_load_shedding_visualizations(self, baseline: EnhancedTestResult, shedding_results: List[EnhancedTestResult]):
        """Generate load shedding analysis visualizations."""
        try:
            plt.style.use('seaborn-v0_8')
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('CitiBike Load Shedding Analysis', fontsize=16)
            
            bounds = [int(r.mode.split('_')[2].rstrip('%')) for r in shedding_results]
            recall_rates = [r.recall_rate for r in shedding_results]
            latencies = [r.wall_clock_time for r in shedding_results]
            cpu_usage = [r.cpu_utilization_avg for r in shedding_results]
            memory_usage = [r.memory_usage_avg_gb for r in shedding_results]
            
            # Plot 1: Recall vs Latency Bound
            axes[0, 0].plot(bounds, recall_rates, 'g-o', linewidth=2, markersize=8)
            axes[0, 0].axhline(y=1.0, color='r', linestyle='--', alpha=0.7, label='Perfect Recall')
            axes[0, 0].set_xlabel('Latency Bound (% of Baseline)')
            axes[0, 0].set_ylabel('Recall Rate')
            axes[0, 0].set_title('Recall vs Latency Trade-off')
            axes[0, 0].grid(True, alpha=0.3)
            axes[0, 0].legend()
            axes[0, 0].set_ylim(0, 1.1)
            
            # Plot 2: Actual Latency vs Target
            baseline_latency = baseline.wall_clock_time
            target_latencies = [baseline_latency * (b/100) for b in bounds]
            axes[0, 1].plot(bounds, target_latencies, 'b--', label='Target Latency', linewidth=2)
            axes[0, 1].plot(bounds, latencies, 'r-o', label='Actual Latency', linewidth=2, markersize=8)
            axes[0, 1].axhline(y=baseline_latency, color='g', linestyle='-', alpha=0.7, label='Baseline Latency')
            axes[0, 1].set_xlabel('Latency Bound (% of Baseline)')
            axes[0, 1].set_ylabel('Latency (seconds)')
            axes[0, 1].set_title('Latency Control Effectiveness')
            axes[0, 1].legend()
            axes[0, 1].grid(True, alpha=0.3)
            
            # Plot 3: Resource Usage Comparison
            x_pos = np.arange(len(bounds))
            width = 0.35
            axes[1, 0].bar(x_pos - width/2, cpu_usage, width, label='CPU %', alpha=0.8)
            axes[1, 0].bar(x_pos + width/2, [m*10 for m in memory_usage], width, label='Memory (GB×10)', alpha=0.8)
            axes[1, 0].axhline(y=baseline.cpu_utilization_avg, color='r', linestyle='--', alpha=0.7, label='Baseline CPU')
            axes[1, 0].set_xlabel('Latency Bound (% of Baseline)')
            axes[1, 0].set_ylabel('Resource Usage')
            axes[1, 0].set_title('Resource Utilization')
            axes[1, 0].set_xticks(x_pos)
            axes[1, 0].set_xticklabels([f'{b}%' for b in bounds])
            axes[1, 0].legend()
            axes[1, 0].grid(True, alpha=0.3)
            
            # Plot 4: Efficiency Metrics
            efficiency = [r.memory_efficiency for r in shedding_results]
            throughput = [r.throughput for r in shedding_results]
            
            ax4_twin = axes[1, 1].twinx()
            line1 = axes[1, 1].plot(bounds, efficiency, 'b-o', linewidth=2, label='Memory Efficiency')
            line2 = ax4_twin.plot(bounds, throughput, 'r-s', linewidth=2, label='Throughput')
            
            axes[1, 1].set_xlabel('Latency Bound (% of Baseline)')
            axes[1, 1].set_ylabel('Memory Efficiency (matches/GB)', color='b')
            ax4_twin.set_ylabel('Throughput (events/sec)', color='r')
            axes[1, 1].set_title('System Efficiency Metrics')
            axes[1, 1].grid(True, alpha=0.3)
            
            # Combine legends
            lines1, labels1 = axes[1, 1].get_legend_handles_labels()
            lines2, labels2 = ax4_twin.get_legend_handles_labels()
            axes[1, 1].legend(lines1 + lines2, labels1 + labels2, loc='upper left')
            
            plt.tight_layout()
            
            # Save the plot
            plots_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'plots')
            os.makedirs(plots_dir, exist_ok=True)
            plt.savefig(os.path.join(plots_dir, 'load_shedding_analysis.png'), dpi=300, bbox_inches='tight')
            print(f"\nLoad shedding visualization saved to logs/plots/load_shedding_analysis.png")
            
            plt.close()
            
        except Exception as e:
            self.logger.warning(f"Could not generate load shedding visualizations: {e}")
    
    def save_comprehensive_results(self, output_file='enhanced_evaluation_results.json'):
        """Save comprehensive evaluation results."""
        logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        if not os.path.dirname(output_file):
            output_file = os.path.join(logs_dir, output_file)
        
        # Convert results to serializable format
        serializable_results = []
        for result in self.results:
            result_dict = asdict(result)
            # Handle non-serializable stage_breakdown
            if result_dict['stage_breakdown'] and result_dict['stage_breakdown'] is not None:
                result_dict['stage_breakdown'] = [asdict(stage) if hasattr(stage, '__dataclass_fields__') else stage for stage in result_dict['stage_breakdown']]
            serializable_results.append(result_dict)
        
        report = {
            "evaluation_summary": {
                "total_tests": len(self.results),
                "data_file": self.data_file_path,
                "target_stations": self.target_stations,
                "timestamp": datetime.now().isoformat(),
                "system_info": {
                    "cpu_count": psutil.cpu_count(),
                    "memory_total_gb": psutil.virtual_memory().total / (1024**3),
                    "python_version": sys.version
                }
            },
            "performance_summary": self._calculate_performance_summary(),
            "results": serializable_results
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        return report
    
    def _calculate_performance_summary(self) -> Dict:
        """Calculate overall performance summary."""
        if not self.results:
            return {}
        
        baseline_results = [r for r in self.results if 'baseline' in r.mode]
        shedding_results = [r for r in self.results if 'load_shedding' in r.mode]
        
        summary = {
            "baseline_performance": {},
            "load_shedding_performance": {},
            "scalability_metrics": {}
        }
        
        if baseline_results:
            baseline_avg = {
                "avg_throughput": np.mean([r.throughput for r in baseline_results]),
                "avg_cpu_usage": np.mean([r.cpu_utilization_avg for r in baseline_results]),
                "avg_memory_usage_gb": np.mean([r.memory_usage_avg_gb for r in baseline_results]),
                "avg_processing_time": np.mean([r.processing_time for r in baseline_results])
            }
            summary["baseline_performance"] = baseline_avg
        
        if shedding_results:
            shedding_avg = {
                "avg_recall_rate": np.mean([r.recall_rate for r in shedding_results]),
                "avg_throughput": np.mean([r.throughput for r in shedding_results]),
                "avg_cpu_usage": np.mean([r.cpu_utilization_avg for r in shedding_results]),
                "avg_memory_usage_gb": np.mean([r.memory_usage_avg_gb for r in shedding_results]),
                "latency_reduction_achieved": len([r for r in shedding_results if r.wall_clock_time < max([b.wall_clock_time for b in baseline_results], default=0)])
            }
            summary["load_shedding_performance"] = shedding_avg
        
        # Scalability metrics
        if len(baseline_results) > 1:
            event_counts = [r.events_processed for r in baseline_results]
            throughputs = [r.throughput for r in baseline_results]
            
            if len(event_counts) >= 2:
                # Calculate scalability coefficient (throughput growth rate)
                scalability_coeff = np.polyfit(event_counts, throughputs, 1)[0] if len(event_counts) > 1 else 0
                summary["scalability_metrics"] = {
                    "throughput_scalability_coefficient": scalability_coeff,
                    "max_events_tested": max(event_counts),
                    "scalability_rating": "Linear" if scalability_coeff > 0.8 else "Sub-linear" if scalability_coeff > 0.3 else "Poor"
                }
        
        return summary


def main():
    """Main function with enhanced evaluation modes."""
    parser = argparse.ArgumentParser(description='Enhanced CitiBike Load Shedding Evaluation - CS-E4780 Project')
    
    parser.add_argument('--mode', choices=['load-shedding', 'scalability', 'all'],
                       default='load-shedding', help='Evaluation mode')
    parser.add_argument('--data-file', '-d', required=True,
                       help='Path to the CitiBike CSV data file')
    parser.add_argument('--output-file', '-o', default='enhanced_evaluation_results.json',
                       help='Output file for results')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')
    parser.add_argument('--max-events', type=int, default=15, help='Maximum events for load shedding test')
    parser.add_argument('--target-stations', nargs='+', default=["7", "8", "9"],
                       help='Target station IDs for pattern matching')
    parser.add_argument('--event-sizes', nargs='+', type=int, default=[5, 10, 15, 20],
                       help='Event sizes for scalability testing')
    parser.add_argument('--auto-targets', action='store_true', help='Auto-detect target stations for the window if baseline has zero matches')
    parser.add_argument('--target-select', choices=['balanced','common','rare'], default='balanced', help='Selection strategy for auto-detected targets')
    
    args = parser.parse_args()
    
    # Setup logging level
    log_level = getattr(logging, args.log_level.upper())
    
    print("="*90)
    print("CITIBIKE LOAD SHEDDING EVALUATION - CS-E4780")
    print("="*90)
    print(f"Mode: {args.mode}")
    print(f"Data file: {os.path.basename(args.data_file)}")
    print(f"Target stations: {args.target_stations}")
    print(f"System: {psutil.cpu_count()} CPUs, {psutil.virtual_memory().total/(1024**3):.1f}GB RAM")
    print("="*90)
    
    try:
        # Initialize evaluator
        evaluator = EnhancedCitiBikeEvaluator(args.data_file, log_level, args.target_stations, auto_targets=args.auto_targets, target_select=args.target_select)
        
        if args.mode == 'load-shedding':
            evaluator.run_enhanced_load_shedding_test(test_events=args.max_events)
        elif args.mode == 'scalability':
            evaluator.run_scalability_test(event_sizes=args.event_sizes)
        else:  # all
            evaluator.run_scalability_test(event_sizes=args.event_sizes)
            evaluator.run_enhanced_load_shedding_test(test_events=args.max_events)
        
        # Save comprehensive results
        report = evaluator.save_comprehensive_results(args.output_file)
        
        # Print summary
        print("\n" + "="*90)
        print("EVALUATION COMPLETE - COMPREHENSIVE RESULTS")
        print("="*90)
        print(f"Total tests: {report['evaluation_summary']['total_tests']}")
        print(f"Results file: logs/{args.output_file}")
        print(f"Visualizations: logs/plots/")
        
        if report.get('performance_summary'):
            summary = report['performance_summary']
            if 'baseline_performance' in summary:
                baseline = summary['baseline_performance']
                print(f"\nBaseline Performance:")
                print(f"  Average throughput: {baseline.get('avg_throughput', 0):.1f} events/sec")
                print(f"  Average CPU usage: {baseline.get('avg_cpu_usage', 0):.1f}%")
                print(f"  Average memory: {baseline.get('avg_memory_usage_gb', 0):.2f}GB")
            
            if 'load_shedding_performance' in summary:
                shedding = summary['load_shedding_performance']
                print(f"\nLoad Shedding Performance:")
                print(f"  Average recall rate: {shedding.get('avg_recall_rate', 0):.3f}")
                print(f"  Average throughput: {shedding.get('avg_throughput', 0):.1f} events/sec")
                print(f"  Latency reductions achieved: {shedding.get('latency_reduction_achieved', 0)}")
                
            if 'scalability_metrics' in summary:
                scalability = summary['scalability_metrics']
                print(f"\nScalability Analysis:")
                print(f"  Scalability rating: {scalability.get('scalability_rating', 'Unknown')}")
                print(f"  Max events tested: {scalability.get('max_events_tested', 0)}")
        
        print("="*90)
        
        return 0
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please check that the data file path is correct.")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)