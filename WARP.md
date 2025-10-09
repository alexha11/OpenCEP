# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

OpenCEP is a Python-based complex event processing (CEP) framework that processes streaming data to detect sophisticated patterns in real-time. The project includes both the core CEP framework and a CS-E4780 academic project demonstrating load shedding techniques with CitiBike data.

## Common Development Commands

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

### Running the Main Application
```bash
# Basic CitiBike evaluation
python3 src/citibike_pattern_evaluation.py --mode basic --data-file /path/to/citibike_data.csv

# Performance scaling test
python3 src/citibike_pattern_evaluation.py --mode performance --data-file /path/to/citibike_data.csv

# Load shedding evaluation
python3 src/citibike_pattern_evaluation.py --mode load-shedding --data-file /path/to/citibike_data.csv

# Run all evaluation modes
python3 src/citibike_pattern_evaluation.py --mode all --data-file /path/to/citibike_data.csv
```

### Running Individual Tests
```bash
# Run a specific test pattern
python3 -c "
import sys; sys.path.insert(0, '.');
from test.tests import *;
testGooglePattern()
"
```

### Development Commands
```bash
# Check imports and basic syntax (no formal linter configured)
python3 -m py_compile CEP.py

# Run with debug logging
python3 src/citibike_pattern_evaluation.py --mode basic --data-file data.csv --log-level DEBUG
```

## Architecture Overview

OpenCEP follows a modular architecture designed for scalable complex event processing:

### Core Components

**CEP Engine (`src/CEP.py`, `CEP.py`)**
- Main orchestrating class that coordinates pattern detection
- Handles pattern preprocessing, evaluation mechanism selection, and parallel execution
- Provides API for running patterns against event streams and retrieving matches

**Pattern Definition System (`base/Pattern.py`, `base/PatternStructure.py`)**
- Patterns define the structure (SEQ, AND, NOT, Kleene closure), conditions, and time windows
- Pattern structures support complex operators including sequential, parallel, and negation patterns
- Condition system handles complex relationships between events using variables and comparison operators

**Evaluation Engine (`tree/`, `evaluation/`)**
- Tree-based evaluation mechanisms that compile patterns into execution trees
- Support for various optimizations including multi-pattern sharing and adaptive evaluation
- Negation algorithms for handling NOT operators efficiently

**Stream Processing (`stream/`)**
- InputStream and OutputStream abstractions for event processing
- Data formatters for different input sources (CitiBike, Twitter, Metastock)
- Support for both file-based and real-time streaming data

### Advanced Features

**Multi-Pattern Optimization (`plan/multi/`)**
- Algorithms for sharing computation across multiple patterns:
  - `TRIVIAL_SHARING_LEAVES`: shares equivalent leaves
  - `TREE_PLAN_SUBTREES_UNION`: shares equivalent subtrees
  - `TREE_PLAN_LOCAL_SEARCH`: local search algorithm for subtree sharing

**Negation Processing (`plan/negation/`)**
- Multiple negation algorithms for performance optimization:
  - Naive negation (default)
  - Statistical negation (recommended for high negative event volumes)
  - Lowest position negation (for bounded negative events)

**Parallel Processing (`parallel/`)**
- Data parallel algorithms including Hirzel, RIP, and HyperCube
- Threading-based parallel execution platform
- Configurable calculation units for scalability

**Adaptive Evaluation (`adaptive/`)**
- Statistics collectors for monitoring arrival rates and selectivity
- Optimizers that dynamically rebuild evaluation plans based on stream characteristics
- Time-window based adaptation with configurable deviation thresholds

### Pattern Language

OpenCEP patterns consist of:

1. **Structure**: Tree of operators over primitive events
   - `SEQ(A,B,C)`: Sequential pattern
   - `AND(A,B)`: Parallel pattern  
   - `NOT(A)`: Negation
   - `KleeneClosureOperator(A)`: Repetition (A+)

2. **Conditions**: Relationships between event attributes
   - Variable extraction: `Variable("a", lambda x: x["Price"])`
   - Comparisons: `SmallerThanCondition`, `EqCondition`
   - Complex conditions: `BinaryCondition` with lambda functions

3. **Time Window**: `timedelta` specifying temporal constraints

4. **Consumption Policies**: Fine-grained control over match selection
   - `MATCH_SINGLE`: Each event appears in at most one match
   - `MATCH_NEXT`: Only one partial match per event
   - Contiguity constraints and freeze policies

## Key Development Patterns

### Creating Patterns
Always define patterns with all three required components (structure, condition, time window):

```python
pattern = Pattern(
    SeqOperator(
        PrimitiveEventStructure("Type", "name"),
        PrimitiveEventStructure("Type2", "name2")
    ),
    SomeCondition(...),
    timedelta(minutes=5)
)
```

### Complex Conditions
For complex event relationships, use `BinaryCondition` or `SimpleCondition` with lambda functions rather than nesting multiple atomic conditions.

### Performance Optimization
- For multi-pattern workloads, always specify sharing algorithms via `TreeBasedEvaluationMechanismParameters`
- For patterns with negation, choose appropriate negation algorithms based on data characteristics
- Use `TreeStorageParameters` with sorted storage and attribute priorities for better performance
- Consider adaptive evaluation for changing stream characteristics

### Testing Patterns
The project includes comprehensive test suites that demonstrate proper pattern usage:
- `test/tests.py`: Basic pattern testing
- `test/NegationTests.py`: Negation operator testing
- `test/NestedTests.py`: Complex nested pattern testing

### CitiBike Project Structure
The CS-E4780 academic project demonstrates load shedding with:
- Hot path detection patterns (bike reuse within time windows)
- Utility-based partial match dropping with configurable thresholds
- Comprehensive performance metrics and evaluation modes
- Real CitiBike data processing with proper CSV formatting

## Important Notes

- The system uses Python 3 and requires proper path configuration for imports
- Complex patterns (especially with Kleene closure) can cause exponential state space growth
- The CitiBike data format expects specific CSV columns including bikeid, station ids, and timestamps
- Load shedding implementation uses utility scoring based on timestamp, completeness, and station diversity
- All evaluation results are logged to `logs/` directory with JSON format output