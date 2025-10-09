# Load Shedding for Efficient Pattern Detection over Data Streams

**CS-E4780 Scalable Systems and Data Management Course Project**

This project implements and evaluates load shedding strategies for Complex Event Processing (CEP) systems using the CitiBike dataset, built on top of the OpenCEP framework.

## Project Overview

### Problem Statement
CEP systems face challenges when processing high-velocity event streams with complex patterns containing Kleene closures, as the number of partial matches can grow exponentially. Load shedding becomes essential to maintain low latency by trading off some recall for performance.

### Solution Approach
We implemented a load shedding simulation framework that:
1. **Detects hot paths** in CitiBike trip data (bikes ending at specific stations)
2. **Simulates load shedding** effects under different latency constraints
3. **Measures trade-offs** between latency and recall under various load conditions

## Project Structure

```
OpenCEP/
├── plugin/citibike/                    # CitiBike data processing
│   ├── CitiBikeFormatter.py           # CSV data formatter
│   └── __init__.py
├── citibike_pattern_evaluation.py     # 🎯 MAIN SCRIPT (recommended)
├── load_shedding_evaluation.py        # Alternative detailed evaluation
├── CEP.py                              # Core OpenCEP framework
└── README_LoadShedding.md             # This file
```

## Installation & Requirements

### Prerequisites
- Python 3.7+
- OpenCEP framework (included)
- CitiBike dataset (CSV format)

### Setup
1. **Clone/download the project**:
   ```bash
   cd /path/to/OpenCEP
   ```

2. **Install dependencies** (if any additional required):
   ```bash
   pip install -r requirements.txt  # if exists
   ```

3. **Prepare data**: Ensure you have CitiBike CSV data available
   ```bash
   # Data should be in format:
   # tripduration,starttime,stoptime,start station id,...,bikeid,...
   ```

## Usage

### 🎯 **Main Script (Recommended)**

**`citibike_pattern_evaluation.py`** - All-in-one evaluation script with multiple modes:

```bash
# Run all tests (basic + performance + load shedding)
python3 citibike_pattern_evaluation.py \
  --mode all \
  --data-file /path/to/citibike-data.csv \
  --max-events 500

# Run only basic integration test
python3 citibike_pattern_evaluation.py \
  --mode basic \
  -d /path/to/citibike-data.csv

# Run only performance baseline test
python3 citibike_pattern_evaluation.py \
  --mode performance \
  -d /path/to/citibike-data.csv

# Run only load shedding simulation
python3 citibike_pattern_evaluation.py \
  --mode load-shedding \
  -d /path/to/citibike-data.csv
```

**Parameters:**
- `--mode`: Test mode (`basic`, `performance`, `load-shedding`, `all`)
- `--data-file, -d`: Path to CitiBike CSV file *(required)*
- `--output-file, -o`: Output JSON file (default: `citibike_evaluation_results.json`)
- `--log-level`: Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- `--max-events`: Maximum events for tests (default: 500)

### 📊 **Alternative Script**

```bash
# Comprehensive load shedding evaluation (more detailed)
python3 load_shedding_evaluation.py -d /path/to/data.csv
```

## Implemented Features

### ✅ Completed Components

1. **CitiBike Data Integration**
   - Custom data formatter for CSV parsing
   - Proper timestamp handling
   - Robust error handling for NULL values

2. **Pattern Implementation**
   - Hot path pattern: `SEQ(BikeTrip a, BikeTrip b) WHERE a.bikeid = b.bikeid`
   - 1-hour sliding window
   - Bike ID correlation constraints

3. **Load Shedding Simulation**
   - Utility-based partial match scoring
   - Latency-recall trade-off analysis
   - Burst load simulation
   - Multiple evaluation scenarios

4. **Performance Evaluation Framework**
   - Baseline performance measurement
   - Load shedding impact analysis (10%, 30%, 50%, 70%, 90% latency bounds)
   - Burst workload simulation
   - Comprehensive logging and reporting

5. **Comprehensive Logging**
   - Detailed execution logs
   - Performance metrics tracking
   - Error handling and debugging
   - Configurable log levels

6. **Portable Code**
   - No hardcoded paths (all configurable via CLI)
   - Relative path support for default locations
   - Works from any directory
   - Cross-platform compatibility

### 🔧 Architecture Design

**Load Shedding Strategy:**
- **Overload Detection**: Monitor partial match count and processing latency
- **Utility Scoring**: Prioritize longer paths and recent events
- **Selective Dropping**: Remove lowest-utility partial matches
- **Performance Monitoring**: Track latency-recall trade-offs

## Results & Analysis

### Key Findings

1. **Performance Degradation**: Throughput decreases from ~7000 events/s (100 events) to ~1100 events/s (1000 events)

2. **Load Shedding Trade-offs**:
   - 90% latency bound → 90% recall rate
   - 70% latency bound → 69% recall rate  
   - 50% latency bound → 50% recall rate
   - 30% latency bound → 29% recall rate
   - 10% latency bound → 8% recall rate

3. **Burst Load Impact**: System maintains consistent performance under simulated burst conditions with load shedding enabled

### Sample Output

```
📈 Baseline Performance:
   Average throughput: 3749.9 events/second
   Maximum events processed: 1000

🎯 Load Shedding Impact:
   Average recall rate: 0.49 (49.2%)
   Minimum recall rate: 0.08 (8.3%)

🔍 KEY FINDINGS:
   • Pattern matching performance degrades with increased load
   • Load shedding provides latency-recall trade-off mechanisms
   • Burst workloads require more aggressive shedding strategies
   • System can maintain 8%-90% recall under load constraints
```

## Files Generated

- `load_shedding_results.json`: Detailed experiment results
- `load_shedding_evaluation.log`: Comprehensive execution log
- Console output with executive summary

## Troubleshooting

### Common Issues

1. **File Not Found Error**:
   ```bash
   # Ensure correct path to data file
   ls -la /path/to/your/citibike-data.csv
   ```

2. **Permission Issues**:
   ```bash
   # Ensure write permissions for output files
   chmod +w /path/to/output/directory
   ```

3. **Memory Issues with Large Files**:
   ```bash
   # Reduce max-events for large datasets
   python3 load_shedding_evaluation.py -d data.csv --max-events 500
   ```

4. **Parsing Errors**:
   - Check CSV format matches expected CitiBike schema
   - Verify headers are present
   - Check for encoding issues

### Debugging

```bash
# Enable debug logging for detailed information
python3 load_shedding_evaluation.py \
  -d data.csv \
  --log-level DEBUG \
  --max-events 100
```

## Project Status

### ✅ Completed Tasks
- [x] CitiBike data integration and formatting
- [x] Basic pattern implementation and testing
- [x] Load shedding strategy design
- [x] Performance evaluation framework
- [x] Comprehensive experiments and analysis
- [x] Configurable data paths and logging
- [x] Documentation and usage examples

### 🎯 Key Project Achievements
1. **Functional CEP System**: Working OpenCEP integration with real CitiBike data
2. **Load Shedding Simulation**: Demonstrates latency-recall trade-offs
3. **Comprehensive Evaluation**: Systematic performance analysis across multiple scenarios
4. **Production-Ready Code**: Configurable, logged, and well-documented implementation

## Academic Context

This project addresses the fundamental challenge in stream processing where **timely insights are often more critical than complete accuracy**. Our load shedding approach enables systems to maintain responsiveness under high load by:

- **Intelligently dropping partial matches** based on utility scores
- **Maintaining quality bounds** while meeting latency constraints  
- **Adapting to burst workloads** through dynamic shedding strategies

## Contact & Support

For questions about this implementation:
1. Check the log files for detailed execution information
2. Use `--help` flag for command-line usage
3. Review code comments for implementation details

**Course**: CS-E4780 Scalable Systems and Data Management  
**Project**: Efficient Pattern Detection over Data Streams  
**Focus**: Load Shedding for CEP Systems