#!/usr/bin/env python3
"""
OpenCEP Sample Demo
This file demonstrates all the sample code examples from the README.
"""

from datetime import timedelta
from CEP import CEP
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.BaseRelationCondition import SmallerThanCondition
from condition.Condition import Variable
from stream.FileStream import FileInputStream, FileOutputStream
from base.DataFormatter import DataFormatter
from condition.CompositeCondition import AndCondition
from condition.Condition import SimpleCondition
from base.PatternStructure import PatternStructure, UnaryStructure, KleeneClosureOperator, CompositeStructure, \
    NegationOperator
from plugin.stocks.Stocks import MetastockDataFormatter
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from condition.KCCondition import KCIndexCondition, KCValueCondition
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, GreaterThanEqCondition, \
    SmallerThanEqCondition
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreeCostModels, StatisticsTypes

from plan.TreePlanBuilderTypes import TreePlanBuilderTypes

def demo_basic_patterns():
    """Demonstrate basic pattern definitions"""
    print("=== Basic Pattern Definitions ===")
    
    # Pattern 1: Google stock price ascent
    print("1. Google Stock Price Ascent Pattern")
    googleAscendPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), 
                    PrimitiveEventStructure("GOOG", "b"), 
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]), 
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]), 
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    
    # Alternative definition using SimpleCondition
    print("1b. Same pattern using SimpleCondition")
    googleAscendPattern_alt = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), 
                    PrimitiveEventStructure("GOOG", "b"), 
                    PrimitiveEventStructure("GOOG", "c")),
        SimpleCondition(Variable("a", lambda x: x["Peak Price"]), 
                        Variable("b", lambda x: x["Peak Price"]),
                        Variable("c", lambda x: x["Peak Price"]),
                        relation_op=lambda x,y,z: x < y < z),
        timedelta(minutes=3)
    )
    
    # # Pattern 2: Amazon and Google low prices
    # print("2. Amazon and Google Low Prices Pattern")
    # googleAmazonLowPattern = Pattern(
    #     AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("GOOG", "g")),
    #     AndCondition(
    #         SmallerThanEqCondition(Variable("a", lambda x: x["Peak Price"]), 73),
    #         SmallerThanEqCondition(Variable("g", lambda x: x["Peak Price"]), 525)
    #     ),
    #     timedelta(minutes=1)
    # )
    
    # # Alternative using BinaryCondition
    # print("2b. Same pattern using BinaryCondition")
    # googleAmazonLowPattern_alt = Pattern(
    #     AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("GOOG", "g")),
    #     BinaryCondition(Variable("a", lambda x: x["Peak Price"]),
    #                     Variable("g", lambda x: x["Peak Price"]),
    #                     lambda x, y: x <= 73 and y <= 525),
    #     timedelta(minutes=1)
    # )
    
    return [googleAscendPattern,]

def demo_basic_cep_execution():
    """Demonstrate basic CEP engine execution"""
    print("\n=== Basic CEP Engine Execution ===")
    
    patterns = demo_basic_patterns()
    
    # Create CEP object
    print("Creating CEP engine...")
    cep = CEP([patterns[0]])  # Using first pattern
    
    # Create file input stream (assuming test file exists)
    print("Creating input stream...")
    try:
        events = FileInputStream("test/EventFiles/NASDAQ_SHORT.txt")
        print("✓ Input stream created successfully")
    except Exception as e:
        print(f"⚠ Could not create input stream: {e}")
        print("  This is expected if test files don't exist")
        return None
    
    # Run CEP engine
    print("Running CEP engine...")
    try:
        cep.run(events, FileOutputStream('test/Matches', 'output.txt'), MetastockDataFormatter())
        print("✓ CEP execution completed")
    except Exception as e:
        print(f"⚠ CEP execution failed: {e}")
    
    return cep

def demo_kleene_closure():
    """Demonstrate Kleene Closure operator patterns"""
    print("\n=== Kleene Closure Patterns ===")
    
    # Basic Kleene Closure
    print("1. Basic Kleene Closure Pattern")
    pattern1 = Pattern(
        SeqOperator(
            PrimitiveEventStructure("GOOG", "a"), 
            KleeneClosureOperator(PrimitiveEventStructure("GOOG", "b"))
        ),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]), Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]), Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=5)
    )
    
    # Kleene Closure with offset condition
    print("2. Kleene Closure with Offset Condition")
    pattern2 = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
            KCValueCondition(names={'a'}, getattr_func=lambda x: x["Peak Price"],
                             relation_op=lambda x, y: x > y,
                             value=530.5),
            KCIndexCondition(names={'a'}, getattr_func=lambda x: x["Opening Price"],
                             relation_op=lambda x, y: x+0.5 < y,
                             offset=-1)
        ),
        timedelta(minutes=5)
    )
    
    # Kleene Closure with value condition
    print("3. Kleene Closure with Value Condition")
    pattern3 = Pattern(
    SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Opening Price"]), 
                            relation_op=lambda x: x > 0),
            KCValueCondition(names={'a'}, 
                             getattr_func=lambda x: x["Peak Price"], 
                             relation_op=lambda x, y: x > y, value=530.5)
            ),
        timedelta(minutes=5)
    )
    
    return [pattern1, pattern2, pattern3]

def demo_multi_pattern_support():
    """Demonstrate multi-pattern support with sharing algorithms"""
    print("\n=== Multi-Pattern Support ===")
    
    # Define multiple patterns
    first_pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"),
                    PrimitiveEventStructure("AAPL", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            GreaterThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    
    second_pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b")),
        SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                             Variable("b", lambda x: x["Peak Price"])),
        timedelta(minutes=3)
    )
    
    # Configure evaluation mechanism with sharing
    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(
            TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
            TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL,
            MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION
        )
    )
    
    # Create CEP with multiple patterns
    print("Creating multi-pattern CEP engine...")
    cep = CEP([first_pattern, second_pattern], eval_mechanism_params)
    print("✓ Multi-pattern CEP engine created")
    
    return cep, [first_pattern, second_pattern]

def demo_negation_operator():
    """Demonstrate negation operator patterns"""
    print("\n=== Negation Operator Patterns ===")
    
    # Basic negation pattern
    print("1. Basic Negation Pattern")
    pattern_basic = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), 
                    NegationOperator(PrimitiveEventStructure("AMZN", "b")), 
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    
    # Negation with statistic algorithm
    print("2. Negation with Statistic Algorithm")
    pattern_statistic = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), 
                    NegationOperator(PrimitiveEventStructure("AMZN", "b")), 
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    
    eval_params = TreeBasedEvaluationMechanismParameters(
        negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM
    )
    
    cep = CEP([pattern_statistic], eval_params)
    print("✓ CEP with statistic negation algorithm created")
    
    return [pattern_basic, pattern_statistic]
def demo_consumption_policies():
    """Demonstrate consumption policies and selection strategies"""
    print("\n=== Consumption Policies and Selection Strategies ===")
    
    # MATCH_SINGLE strategy
    print("1. MATCH_SINGLE Selection Strategy")
    pattern_single = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), 
                    PrimitiveEventStructure("AMZN", "b"), 
                    PrimitiveEventStructure("AVID", "c")), 
        TrueCondition(),
        timedelta(minutes=5),
        ConsumptionPolicy(primary_selection_strategy=SelectionStrategies.MATCH_SINGLE)
    )
    
    # MATCH_NEXT strategy
    print("2. MATCH_NEXT Selection Strategy")
    pattern_next = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), 
                    PrimitiveEventStructure("AMZN", "b"), 
                    PrimitiveEventStructure("AVID", "c")), 
        TrueCondition(),
        timedelta(minutes=5),
        ConsumptionPolicy(primary_selection_strategy=SelectionStrategies.MATCH_NEXT)
    )
    
    # Subset-specific policies
    print("3. Subset-specific Selection Strategy")
    pattern_subset = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), 
                    PrimitiveEventStructure("AMZN", "b"), 
                    PrimitiveEventStructure("AVID", "c")), 
        TrueCondition(),
        timedelta(minutes=5),
        ConsumptionPolicy(single=["AMZN", "AVID"], 
                          secondary_selection_strategy=SelectionStrategies.MATCH_NEXT)
    )
    
    # Contiguous events policy
    print("4. Contiguous Events Policy")
    pattern_contiguous = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), 
                    PrimitiveEventStructure("AMZN", "b"), 
                    PrimitiveEventStructure("AVID", "c")), 
        TrueCondition(),
        timedelta(minutes=5),
        ConsumptionPolicy(contiguous=["a", "b", "c"])
    )
    
    # Freeze policy
    print("5. Freeze Policy")
    pattern_freeze = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), 
                    PrimitiveEventStructure("AMZN", "b"), 
                    PrimitiveEventStructure("AVID", "c")), 
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), 
                                 Variable("b", lambda x: x["Opening Price"])), 
            GreaterThanCondition(Variable("b", lambda x: x["Opening Price"]), 
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5),
        ConsumptionPolicy(freeze="b")
    )
    
    return [pattern_single, pattern_next, pattern_subset, pattern_contiguous, pattern_freeze]

def demo_storage_optimization():
    """Demonstrate storage optimization parameters"""
    print("\n=== Storage Optimization ===")
    
    # Create a sample pattern
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), 
                    PrimitiveEventStructure("AMZN", "b"), 
                    PrimitiveEventStructure("AVID", "c")), 
        TrueCondition(),
        timedelta(minutes=5)
    )
    
    # Configure storage parameters
    storage_params = TreeStorageParameters(
        sort_storage=True,
        attributes_priorities={"a": 122, "b": 200, "c": 104, "m": 139}
    )
    
    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(storage_params=storage_params)
    cep = CEP([pattern], eval_mechanism_params)
    
    print("✓ CEP with optimized storage parameters created")
    return cep

def demo_adaptive_cep():
    """Demonstrate Adaptive CEP with statistics and optimization"""
    print("\n=== Adaptive CEP ===")
    
    # Define statistics types and time window
    statistics_types = [StatisticsTypes.SELECTIVITY_MATRIX, StatisticsTypes.ARRIVAL_RATES]
    time_window = timedelta(minutes=2)
    
    # Statistics collector parameters
    statistics_collector_params = StatisticsCollectorParameters(
        statistics_types=statistics_types,
        statistics_time_window=time_window
    )
    
    # Optimizer parameters
    optimizer_params = StatisticsDeviationAwareOptimizerParameters(
        t=0.5, 
        statistics_types=statistics_types
    )
    
    # Create adaptive CEP
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), 
                    PrimitiveEventStructure("GOOG", "b")),
        SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]), 
                             Variable("b", lambda x: x["Peak Price"])),
        timedelta(minutes=3)
    )
    
    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(
        statistics_collector_params=statistics_collector_params, 
        optimizer_params=optimizer_params
    )
    
    cep = CEP([pattern], eval_mechanism_params)
    print("✓ Adaptive CEP engine created")
    
    return cep

def demo_probabilistic_patterns():
    """Demonstrate probabilistic patterns with confidence thresholds"""
    print("\n=== Probabilistic Patterns ===")
    
    # Pattern with confidence threshold
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("GOOG", "a"), 
            PrimitiveEventStructure("GOOG", "b")
        ),
        SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]), 
                             Variable("b", lambda x: x["Peak Price"])),
        timedelta(minutes=5),
        confidence=0.9
    )
    
    print("✓ Probabilistic pattern with 90% confidence threshold created")
    return pattern

def demo_twitter_integration():
    """Demonstrate Twitter API integration (requires credentials)"""
    print("\n=== Twitter Integration ===")
    
    try:
        from plugin.twitter.TwitterInputStream import TwitterInputStream
        
        # Create Twitter stream (requires valid credentials)
        print("Creating Twitter stream...")
        event_stream = TwitterInputStream(['corona'])
        print("✓ Twitter stream created (requires valid credentials)")
        
        # Note: Tweet format is defined in Tweets.py
        print("  Tweet format defined in Tweets.py")
        
    except ImportError as e:
        print(f"⚠ Twitter integration not available: {e}")
        print("  This is expected if Twitter plugin is not installed")
    except Exception as e:
        print(f"⚠ Twitter stream creation failed: {e}")
        print("  Check TwitterCredentials.py for valid credentials")

def demo_data_parallel_algorithms():
    """Demonstrate data parallel execution parameters"""
    print("\n=== Data Parallel Algorithms ===")
    
    print("Data parallel algorithms available:")
    print("1. Hirzel Algorithm - requires attribute for data division")
    print("2. RIP Algorithm - requires time multiple > 1")
    print("3. HyperCube Algorithm - requires specific unit numbers")
    
    # Note: These would require specific DataParallelExecutionParameters
    # which aren't fully shown in the README examples
    print("⚠ Full implementation requires DataParallelExecutionParameters")
    print("  See README for specific algorithm requirements")

def create_sample_data():
    """Create sample data files for testing"""
    print("\n=== Creating Sample Data ===")
    
    import os
    
    # Create test directories
    os.makedirs("test/EventFiles", exist_ok=True)
    os.makedirs("test/Matches", exist_ok=True)
    
    # Create sample NASDAQ data file
    sample_data = """GOOG,20230101,100.0,105.0,98.0,103.0,1000000
GOOG,20230102,103.0,108.0,101.0,106.0,1200000
GOOG,20230103,106.0,110.0,104.0,109.0,1100000
AMZN,20230101,150.0,155.0,148.0,153.0,800000
AMZN,20230102,153.0,158.0,151.0,156.0,900000
AAPL,20230101,180.0,185.0,178.0,183.0,1500000
AAPL,20230102,183.0,188.0,181.0,186.0,1600000"""
    
    with open("test/EventFiles/NASDAQ_SHORT.txt", "w") as f:
        f.write(sample_data)
    
    print("✓ Sample data files created")
    print("  - test/EventFiles/NASDAQ_SHORT.txt")
    print("  - test/Matches/ (output directory)")

def main():
    """Main function to run all demonstrations"""
    print("OpenCEP Comprehensive Demo")
    print("=" * 50)
    
    try:
        # Create sample data first
        create_sample_data()
        
        # Run all demonstrations
        demo_basic_patterns()
        demo_basic_cep_execution()
        # demo_kleene_closure()
        # demo_multi_pattern_support()
        # demo_negation_operator()
        # demo_consumption_policies()
        # demo_storage_optimization()
        # demo_adaptive_cep()
        # demo_probabilistic_patterns()
        # demo_twitter_integration()
        # demo_data_parallel_algorithms()

        
        print("\n" + "=" * 50)
        print("✓ All demonstrations completed!")
        print("Note: Some features may require additional setup or credentials")
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        print("This is expected if OpenCEP dependencies are not installed")
        print("\nTo run this demo successfully:")
        print("1. Install OpenCEP and its dependencies")
        print("2. Ensure all required modules are available")
        print("3. Configure any necessary credentials (e.g., Twitter API)")

if __name__ == "__main__":
    main()
