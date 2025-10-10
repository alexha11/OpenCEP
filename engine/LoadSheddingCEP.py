#!/usr/bin/env python3
"""
Load Shedding CEP Engine
CS-E4780 Project: Efficient Pattern Detection over Data Streams

Integrates runtime load shedding by wrapping pattern match storage.
"""

import time
from typing import List

from CEP import CEP
from base.Pattern import Pattern
from tree.LoadSheddingPatternMatchStorage import LoadSheddingPatternMatchStorage, LoadSheddingConfig


class LoadSheddingCEP(CEP):
    """CEP engine with runtime load shedding for bounded latency."""
    
    def __init__(self, patterns: List[Pattern], load_shedding_config: LoadSheddingConfig = None):
        self.load_shedding_config = load_shedding_config or LoadSheddingConfig(
            enabled=True,
            max_partial_matches=20,
            utility_threshold=0.3,
            shedding_strategy="utility",
            aggressive_shedding=True
        )
        
        self.load_shedding_stats = {
            "total_storages_wrapped": 0,
            "total_dropped_matches": 0,
            "shedding_events": 0,
            "start_time": time.time()
        }
        
        super().__init__(patterns)
        self._apply_load_shedding_to_tree()
        
    def _apply_load_shedding_to_tree(self):
        """Wrap all tree nodes with load shedding storage."""
        if not hasattr(self, '_CEP__evaluation_manager'):
            print("Warning: No evaluation manager found")
            return
            
        eval_mechanism = self._CEP__evaluation_manager._SequentialEvaluationManager__eval_mechanism
        all_nodes = []
        
        if hasattr(eval_mechanism, '_tree'):
            tree = eval_mechanism._tree
            if hasattr(tree, '_Tree__root'):
                all_nodes = self._traverse_tree_nodes(tree._Tree__root)
                
        elif hasattr(eval_mechanism, '_tree_plan'):
            tree_plan = eval_mechanism._tree_plan
            if hasattr(tree_plan, '_output_nodes'):
                all_nodes.extend(tree_plan._output_nodes)
            if hasattr(tree_plan, '_leaves'):
                all_nodes.extend(tree_plan._leaves)
            if hasattr(tree_plan, '_internal_nodes'):
                all_nodes.extend(tree_plan._internal_nodes)
        else:
            print("Warning: Unknown evaluation mechanism")
            return
            
        wrapped_count = 0
        for node in all_nodes:
            if hasattr(node, '_partial_matches') and node._partial_matches is not None:
                self._wrap_node_storage(node)
                wrapped_count += 1
                
        print(f"Load shedding: wrapped {wrapped_count} nodes")
        
    def _traverse_tree_nodes(self, root_node):
        """Traverse tree to find all nodes."""
        nodes = []
        visited = set()
        
        def visit_node(node):
            node_id = id(node)
            if node_id in visited:
                return
            visited.add(node_id)
            nodes.append(node)
            
            if hasattr(node, '_subtrees') and node._subtrees:
                for subtree in node._subtrees:
                    if subtree:
                        visit_node(subtree)
            elif hasattr(node, '_subtree') and node._subtree:
                visit_node(node._subtree)
        
        if hasattr(root_node, 'get_leaves'):
            nodes.append(root_node)
            for leaf in root_node.get_leaves():
                visit_node(leaf)
        else:
            visit_node(root_node)
            
        return nodes
        
    def _wrap_node_storage(self, node):
        """Wrap node storage with load shedding."""
        if isinstance(node._partial_matches, LoadSheddingPatternMatchStorage):
            return
            
        node._partial_matches = LoadSheddingPatternMatchStorage(
            node._partial_matches, 
            self.load_shedding_config
        )
        self.load_shedding_stats['total_storages_wrapped'] += 1
        
    def run(self, events, matches, data_formatter):
        start_time = time.time()
        print(f"Max partial matches: {self.load_shedding_config.max_partial_matches}, Strategy: {self.load_shedding_config.shedding_strategy}")
        
        processing_time = super().run(events, matches, data_formatter)
        self._collect_load_shedding_stats()
        wall_time = time.time() - start_time
        self._report_load_shedding_results(processing_time, wall_time)
        
        return processing_time
        
    def _collect_load_shedding_stats(self):
        """Collect load shedding statistics."""
        total_dropped = 0
        total_current_size = 0
        shedding_events = 0
        
        if not hasattr(self, '_CEP__evaluation_manager'):
            return
            
        eval_mechanism = self._CEP__evaluation_manager._SequentialEvaluationManager__eval_mechanism
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
        """Report load shedding results."""
        stats = self.load_shedding_stats
        print("\n" + "=" * 50)
        print("LOAD SHEDDING RESULTS")
        print("=" * 50)
        
        if not self.load_shedding_config.enabled:
            print("Status: DISABLED")
            return
            
        print(f"Wrapped nodes: {stats['total_storages_wrapped']}")
        print(f"Dropped matches: {stats['total_dropped_matches']}")
        print(f"Shedding events: {stats['shedding_events']}")
        print(f"Final storage: {stats.get('final_storage_size', 0)}")
        print(f"Processing time: {processing_time:.4f}s, Wall time: {wall_time:.4f}s")
        print("=" * 50)
        
    def get_load_shedding_summary(self):
        """Get load shedding summary."""
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


def create_load_shedding_cep(patterns: List[Pattern], max_partial_matches: int = 100, 
                           strategy: str = "utility", aggressive: bool = True) -> LoadSheddingCEP:
    """Create LoadSheddingCEP with specified configuration."""
    config = LoadSheddingConfig(
        enabled=True,
        max_partial_matches=max_partial_matches,
        shedding_strategy=strategy,
        aggressive_shedding=aggressive
    )
    return LoadSheddingCEP(patterns, config)


# Configuration presets
CONSERVATIVE_CONFIG = LoadSheddingConfig(
    enabled=True,
    max_partial_matches=50,
    shedding_strategy="utility",
    aggressive_shedding=True
)

MODERATE_CONFIG = LoadSheddingConfig(
    enabled=True,
    max_partial_matches=200,
    shedding_strategy="utility",
    aggressive_shedding=True
)

PERFORMANCE_CONFIG = LoadSheddingConfig(
    enabled=True,
    max_partial_matches=500,
    shedding_strategy="utility",
    aggressive_shedding=False
)
