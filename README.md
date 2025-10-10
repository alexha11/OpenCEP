# OpenCEP: Complex Event Processing Framework

OpenCEP is a Python-based complex event processing (CEP) framework that processes streaming data to detect sophisticated patterns in real-time. This project includes both the core CEP framework and a CS-E4780 academic project demonstrating load shedding techniques with CitiBike data.

## Quick Start

### 1. Download Data

The project uses CitiBike trip data from 2014. Run the download script to automatically fetch and extract the required dataset:

```bash
./download_data.sh
```

This will download and extract the 2014 CitiBike dataset to `data/2014-citibike-tripdata/`.

### 2. Install Dependencies

The project requires the following Python packages:

```bash
pip3 install psutil matplotlib seaborn pandas numpy
```

### 3. Run the System

The easiest way to get started is using the `run.sh` script:

```bash
# Run with defaults (load shedding mode, auto-detected target stations)
./run.sh

# Run specific evaluation mode
./run.sh --mode load-shedding --max-events 50

# Run all evaluation modes
./run.sh --mode all --max-events 100
```

You can also run the main script directly:

```bash
# Basic load shedding evaluation
python3 main.py --mode load-shedding --data-file data/2014-citibike-tripdata/1_January/201401-citibike-tripdata_1.csv --max-events 20

# Scalability testing
python3 main.py --mode scalability --data-file data/2014-citibike-tripdata/1_January/201401-citibike-tripdata_1.csv

# Performance analysis
python3 main.py --mode performance --data-file data/2014-citibike-tripdata/1_January/201401-citibike-tripdata_1.csv

# Run all evaluation modes
python3 main.py --mode all --data-file data/2014-citibike-tripdata/1_January/201401-citibike-tripdata_1.csv
```

## Project Structure

### Core Components

- **`CEP.py`** - Main orchestrating class that coordinates pattern detection
- **`main.py`** - Primary evaluation script with comprehensive performance analysis
- **`base/`** - Core pattern definition system and event structures
- **`condition/`** - Pattern conditions and variable handling
- **`tree/`** - Tree-based evaluation mechanisms and pattern matching
- **`stream/`** - Stream processing abstractions and data formatters
- **`engine/`** - Load shedding CEP engine
- **`plugin/citibike/`** - CitiBike data formatter and processing logic

### Evaluation Modes

1. **Basic Mode** (`--mode basic`)
   - Standard pattern evaluation without load shedding
   - Baseline performance measurement

2. **Load Shedding Mode** (`--mode load-shedding`)
   - Demonstrates load shedding techniques with utility-based dropping
   - Configurable latency bounds and thresholds

3. **Performance Mode** (`--mode performance`)
   - Detailed CPU and memory monitoring
   - Stage-by-stage breakdown analysis

4. **Scalability Mode** (`--mode scalability`)
   - Tests performance across different workload sizes
   - Throughput and efficiency metrics

5. **All Mode** (`--mode all`)
   - Runs all evaluation modes sequentially

### Data Format

The system expects CitiBike CSV data with the following columns:
- `tripduration` - Trip duration in seconds
- `starttime` - Start time (YYYY-MM-DD HH:MM:SS format)
- `stoptime` - Stop time
- `start station id` - Starting station ID
- `end station id` - Ending station ID
- `bikeid` - Bike identifier
- Additional columns are ignored

## Usage Examples

### Running Tests

```bash
# Run all tests
python3 -m pytest test/

# Run unit tests specifically
python3 -m pytest test/UnitTests/

# Run specific test files
python3 test/tests.py
python3 test/NegationTests.py
python3 test/NestedTests.py
```

### Advanced Usage

```bash
# Specify target stations manually
python3 main.py --mode load-shedding --target-stations 526 270 519 --max-events 100

# Enable debug logging
python3 main.py --mode basic --log-level DEBUG --max-events 50

# Use different data file
python3 main.py --mode performance --data-file path/to/your/data.csv

# Auto-detect target stations with different selection strategy
./run.sh --mode scalability --max-events 200 --target-select popular
```

### Target Station Detection

The system can automatically detect optimal target stations from your data:

```bash
# Auto-detect top 3 balanced stations from first 50 events
python3 scripts/find_targets.py data.csv --max-events 50 --top 3 --mode balanced

# Find most popular stations
python3 scripts/find_targets.py data.csv --max-events 100 --top 5 --mode popular

# Find stations with highest diversity
python3 scripts/find_targets.py data.csv --max-events 200 --top 3 --mode diverse
```

## Pattern Language

OpenCEP patterns consist of four components:

1. **Structure** - Tree of operators over primitive events
2. **Conditions** - Relationships between event attributes  
3. **Time Window** - Temporal constraints
4. **Consumption Policies** - Match selection control

### Example Pattern

```python
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.Condition import Variable, BinaryCondition
from datetime import timedelta

# Define a pattern for bike reuse within 30 minutes
pattern = Pattern(
    SeqOperator(
        PrimitiveEventStructure("BikeReturn", "return1"),
        PrimitiveEventStructure("BikePickup", "pickup1")
    ),
    BinaryCondition(
        Variable("return1", lambda x: x["bikeid"]),
        Variable("pickup1", lambda x: x["bikeid"]),
        lambda x, y: x == y  # Same bike
    ),
    timedelta(minutes=30)
)
```

## Performance Features

- **System Monitoring** - Real-time CPU and memory usage tracking
- **Stage Breakdown** - Performance analysis by processing stage
- **Load Shedding** - Utility-based partial match dropping with configurable thresholds
- **Multi-Pattern Optimization** - Computation sharing across patterns
- **Adaptive Evaluation** - Dynamic optimization based on stream characteristics

## Output

Results are logged to the `logs/` directory in JSON format, including:
- Performance metrics (throughput, latency, resource usage)
- Pattern match statistics
- Load shedding effectiveness
- Scalability analysis
- Detailed timing breakdowns

## Troubleshooting

### Common Issues

1. **Data file not found**
   - Ensure you've run `./download_data.sh`
   - Check that the data file path exists

2. **Import errors**
   - The system adds the root directory to Python path automatically
   - Ensure all required dependencies are installed

3. **Memory issues with large datasets**
   - Use `--max-events` to limit the number of events processed
   - Enable load shedding mode for memory-constrained environments

4. **Target station detection fails**
   - The system will fall back to default stations (526, 270, 519)
   - You can specify target stations manually with `--target-stations`

### Dependencies

Required Python packages:
- `psutil` - System monitoring
- `matplotlib` - Visualization
- `seaborn` - Statistical plots
- `pandas` - Data manipulation
- `numpy` - Numerical operations

All other dependencies are part of the Python standard library.

## Architecture

OpenCEP follows a modular architecture:

- **Pattern Processing** - Compile patterns into executable evaluation trees
- **Stream Management** - Handle various input formats and real-time streams
- **Evaluation Engine** - Efficient pattern matching with optimization support
- **Load Shedding** - Intelligent partial match dropping under resource constraints
- **Monitoring** - Comprehensive performance and resource tracking

For detailed architecture information, see `WARP.md`.