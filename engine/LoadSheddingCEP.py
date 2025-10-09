#!/usr/bin/env python3
"""
Load Shedding CEP Engine
CS-E4780 Project: Efficient Pattern Detection over Data Streams

This module integrates runtime load shedding into the OpenCEP framework.
It wraps the standard CEP engine and modifies node creation to use 
load shedding storage wrappers.
"""

import time
from typing import List

from CEP import CEP
from base.Pattern import Pattern
from tree.LoadSheddingPatternMatchStorage import LoadSheddingPatternMatchStorage, LoadSheddingConfig
from tree.PatternMatchStorage import PatternMatchStorage, SortedPatternMatchStorage, UnsortedPatternMatchStorage


class LoadSheddingCEP(CEP):
    """
    CEP Engine with integrated runtime load shedding.
    
    This extends the standard CEP class to wrap all pattern match storage
    with load shedding capabilities, enabling bounded latency even with
    complex patterns like Kleene closures.
    """
    
    def __init__(self, patterns: List[Pattern], load_shedding_config: LoadSheddingConfig = None):
        """
        Initialize CEP engine with load shedding capabilities.
        
        Args:
            patterns: List of patterns to detect
            load_shedding_config: Configuration for load shedding behavior
        """
        self.load_shedding_config = load_shedding_config or LoadSheddingConfig(
            enabled=True,
            max_partial_matches=20,  # Very aggressive limit for Kleene closure
            utility_threshold=0.3,
            shedding_strategy="utility",
            aggressive_shedding=True
        )
        
        # Store load shedding statistics
        self.load_shedding_stats = {
            "total_storages_wrapped": 0,
            "total_dropped_matches": 0,
            "shedding_events": 0,
            "start_time": time.time()
        }
        
        # Initialize parent CEP with patterns
        super().__init__(patterns)
        
        # After initialization, wrap all storage units with load shedding
        self._apply_load_shedding_to_tree()
        
    def _apply_load_shedding_to_tree(self):
        """
        Apply load shedding to all nodes in the evaluation tree.
        
        This method traverses the evaluation tree and wraps each node's
        pattern match storage with a LoadSheddingPatternMatchStorage wrapper.
        """
        if not hasattr(self, '_CEP__evaluation_manager'):
            print("Warning: No evaluation manager found - load shedding cannot be applied")
            return
            
        eval_mechanism = self._CEP__evaluation_manager._SequentialEvaluationManager__eval_mechanism
        
        # Handle different evaluation mechanism types
        all_nodes = []
        
        if hasattr(eval_mechanism, '_tree'):
            # TreeBasedEvaluationMechanism - has _tree attribute
            tree = eval_mechanism._tree
            
            # Get all nodes by traversing the tree
            if hasattr(tree, '_Tree__root'):
                root_node = tree._Tree__root
                all_nodes = self._traverse_tree_nodes(root_node)
                
        elif hasattr(eval_mechanism, '_tree_plan'):
            # Some other mechanism with tree plan
            tree_plan = eval_mechanism._tree_plan
            if hasattr(tree_plan, '_output_nodes'):
                all_nodes.extend(tree_plan._output_nodes)
            if hasattr(tree_plan, '_leaves'):
                all_nodes.extend(tree_plan._leaves)
            if hasattr(tree_plan, '_internal_nodes'):
                all_nodes.extend(tree_plan._internal_nodes)
        else:
            print("Warning: Unknown evaluation mechanism structure - cannot apply load shedding")
            return
            
        # Wrap storage in each node
        wrapped_count = 0
        for i, node in enumerate(all_nodes):
            if hasattr(node, '_partial_matches') and node._partial_matches is not None:
                self._wrap_node_storage(node)
                wrapped_count += 1
                
        print(f"Load Shedding Applied: Wrapped {wrapped_count} storage units")
        
    def _traverse_tree_nodes(self, root_node):
        """Recursively traverse tree to find all nodes with storage"""
        nodes = []
        visited = set()
        
        def visit_node(node):
            # Prevent infinite recursion
            node_id = id(node)
            if node_id in visited:
                return
            visited.add(node_id)
            
            nodes.append(node)
            
            # Check for child nodes - be more careful about structure
            if hasattr(node, '_subtrees') and node._subtrees:
                # Binary node with left/right subtrees
                for subtree in node._subtrees:
                    if subtree:
                        visit_node(subtree)
            elif hasattr(node, '_subtree') and node._subtree:
                # Unary node with single subtree
                visit_node(node._subtree)
            # Don't recursively call get_leaves() - that's for the root only
        
        # Start with root node
        if hasattr(root_node, 'get_leaves'):
            # This is a tree root - get all leaves directly
            nodes.append(root_node)  # Include the root itself
            for leaf in root_node.get_leaves():
                visit_node(leaf)
        else:
            # This is a regular node - traverse normally
            visit_node(root_node)
            
        return nodes
        
    def _wrap_node_storage(self, node):
        """Wrap a node's storage with load shedding capability"""
        original_storage = node._partial_matches
        
        if isinstance(original_storage, LoadSheddingPatternMatchStorage):
            # Already wrapped
            return
            
        # Create load shedding wrapper
        load_shedding_storage = LoadSheddingPatternMatchStorage(
            original_storage, 
            self.load_shedding_config
        )
        
        # Replace the node's storage
        node._partial_matches = load_shedding_storage
        
        self.load_shedding_stats['total_storages_wrapped'] += 1
        
    def run(self, events, matches, data_formatter):
        """
        Run CEP with load shedding and collect statistics.
        """
        start_time = time.time()
        
        print(f"Starting Load Shedding CEP with max {self.load_shedding_config.max_partial_matches} partial matches per node")
        print(f"Strategy: {self.load_shedding_config.shedding_strategy}, Aggressive: {self.load_shedding_config.aggressive_shedding}")
        
        # Run the actual CEP processing
        processing_time = super().run(events, matches, data_formatter)
        
        # Collect load shedding statistics
        self._collect_load_shedding_stats()
        
        wall_time = time.time() - start_time
        
        # Report load shedding results
        self._report_load_shedding_results(processing_time, wall_time)
        
        return processing_time
        
    def _collect_load_shedding_stats(self):
        """Collect statistics from all load shedding storage units"""
        total_dropped = 0
        total_current_size = 0
        shedding_events = 0
        
        # Get all nodes using the same logic as _apply_load_shedding_to_tree
        if not hasattr(self, '_CEP__evaluation_manager'):
            return
            
        eval_mechanism = self._CEP__evaluation_manager._SequentialEvaluationManager__eval_mechanism
        
        # Handle different evaluation mechanism types (same as apply method)
        all_nodes = []
        
        if hasattr(eval_mechanism, '_tree'):
            tree = eval_mechanism._tree
            if hasattr(tree, '_Tree__root'):
                root_node = tree._Tree__root
                all_nodes = self._traverse_tree_nodes(root_node)
        elif hasattr(eval_mechanism, '_tree_plan'):
            tree_plan = eval_mechanism._tree_plan
            if hasattr(tree_plan, '_output_nodes'):
                all_nodes.extend(tree_plan._output_nodes)
            if hasattr(tree_plan, '_leaves'):
                all_nodes.extend(tree_plan._leaves)
            if hasattr(tree_plan, '_internal_nodes'):
                all_nodes.extend(tree_plan._internal_nodes)
                
        for node in all_nodes:
            if (hasattr(node, '_partial_matches') and 
                isinstance(node._partial_matches, LoadSheddingPatternMatchStorage)):
                
                stats = node._partial_matches.get_load_shedding_stats()
                total_dropped += stats['dropped_matches']
                total_current_size += stats['current_size']
                if stats['dropped_matches'] > 0:
                    shedding_events += 1
                    
        self.load_shedding_stats['total_dropped_matches'] = total_dropped
        self.load_shedding_stats['shedding_events'] = shedding_events
        self.load_shedding_stats['final_storage_size'] = total_current_size
        
    def _report_load_shedding_results(self, processing_time: float, wall_time: float):
        """Report load shedding performance results"""
        stats = self.load_shedding_stats
        
        print("\n" + "=" * 60)
        print("LOAD SHEDDING RESULTS")
        print("=" * 60)
        
        if not self.load_shedding_config.enabled:
            print("Load shedding: DISABLED")
            return
            
        print(f"Load shedding: ENABLED")
        print(f"Storage nodes wrapped: {stats['total_storages_wrapped']}")
        print(f"Total partial matches dropped: {stats['total_dropped_matches']}")
        print(f"Load shedding events: {stats['shedding_events']}")
        print(f"Final storage size: {stats.get('final_storage_size', 0)}")
        print(f"Strategy: {self.load_shedding_config.shedding_strategy}")
        print(f"Processing time: {processing_time:.4f}s")
        print(f"Wall clock time: {wall_time:.4f}s")
        print("=" * 60)
        
    def get_load_shedding_summary(self):
        """Get detailed load shedding summary for reporting"""
        return {
            "config": {
                "enabled": self.load_shedding_config.enabled,
                "max_partial_matches": self.load_shedding_config.max_partial_matches,
                "strategy": self.load_shedding_config.shedding_strategy,
                "aggressive_shedding": self.load_shedding_config.aggressive_shedding
            },
            "results": self.load_shedding_stats,
            "effectiveness": {
                "shedding_triggered": self.load_shedding_stats['total_dropped_matches'] > 0,
                "storage_nodes_protected": self.load_shedding_stats['total_storages_wrapped'],
                "partial_matches_saved": self.load_shedding_stats['total_dropped_matches']
            }
        }


# Convenience function to create load shedding CEP with different configurations
def create_load_shedding_cep(patterns: List[Pattern], max_partial_matches: int = 100, 
                           strategy: str = "utility", aggressive: bool = True) -> LoadSheddingCEP:
    """Create a LoadSheddingCEP with specified configuration"""
    config = LoadSheddingConfig(
        enabled=True,
        max_partial_matches=max_partial_matches,
        shedding_strategy=strategy,
        aggressive_shedding=aggressive
    )
    return LoadSheddingCEP(patterns, config)


# Configuration presets for different use cases
CONSERVATIVE_CONFIG = LoadSheddingConfig(
    enabled=True,
    max_partial_matches=50,   # Very low limit
    shedding_strategy="utility",
    aggressive_shedding=True
)

MODERATE_CONFIG = LoadSheddingConfig(
    enabled=True,
    max_partial_matches=200,  # Moderate limit
    shedding_strategy="utility",
    aggressive_shedding=True
)

PERFORMANCE_CONFIG = LoadSheddingConfig(
    enabled=True,
    max_partial_matches=500,  # Higher limit for better recall
    shedding_strategy="utility",
    aggressive_shedding=False
)