from abc import ABC
from typing import Dict
from base.DataFormatter import DataFormatter
from base.Event import Event
from plan.TreePlan import TreePlan
from stream.Stream import InputStream, OutputStream
from misc.Utils import *
from tree.nodes.LeafNode import LeafNode
from tree.PatternMatchStorage import TreeStorageParameters
from evaluation.EvaluationMechanism import EvaluationMechanism
from misc.ConsumptionPolicy import *
from tree.MultiPatternTree import MultiPatternTree
from adaptive.statistics import StatisticsCollector
from tree.Tree import Tree
from datetime import timedelta, datetime
from adaptive.optimizer import Optimizer
import time


class TreeBasedEvaluationMechanism(EvaluationMechanism, ABC):
    """
    An implementation of the tree-based evaluation mechanism.
    """
    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                 storage_params: TreeStorageParameters,
                 statistics_collector: StatisticsCollector = None,
                 optimizer: Optimizer = None,
                 statistics_update_time_window: timedelta = None):
        self.__is_multi_pattern_mode = len(pattern_to_tree_plan_map) > 1
        if self.__is_multi_pattern_mode:
            # TODO: support statistic collection in the multi-pattern mode
            self._tree = MultiPatternTree(pattern_to_tree_plan_map, storage_params)
        else:
            pattern = list(pattern_to_tree_plan_map)[0]
            pattern.condition.set_statistics_collector(statistics_collector)
            self._tree = Tree(list(pattern_to_tree_plan_map.values())[0],
                              list(pattern_to_tree_plan_map)[0], storage_params)

        self.__storage_params = storage_params
        self.__statistics_collector = statistics_collector
        self.__optimizer = optimizer

        self._event_types_listeners = {}
        self.__statistics_update_time_window = statistics_update_time_window

        # The remainder of the initialization process is only relevant for the freeze map feature. This feature can
        # only be enabled in single-pattern mode.
        self._pattern = list(pattern_to_tree_plan_map)[0] if not self.__is_multi_pattern_mode else None
        self.__freeze_map = {}
        self.__active_freezers = []

        if not self.__is_multi_pattern_mode and self._pattern.consumption_policy is not None and \
                self._pattern.consumption_policy.freeze_names is not None:
            self.__init_freeze_map()
        
        # Load shedding parameters
        self.__load_shedding_enabled = False  # Can be toggled
        self.__max_partial_matches = 10000  # Threshold to trigger shedding
        self.__target_partial_matches = 5000  # Target after shedding
        self.__latency_threshold = 0.1  # 100ms max per event (seconds)
        
        # Monitoring statistics
        self.__event_count = 0
        self.__total_latency = 0.0
        self.__shedding_events = []  # Track when shedding occurs
        self.__partial_match_counts = []  # Track storage size over time

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Activates the tree evaluation mechanism on the input event stream and reports all found pattern matches to the
        given output stream.
        """
        self._event_types_listeners = self._register_event_listeners(self._tree)
        last_statistics_refresh_time = None

        for raw_event in events:
            start_time = time.time()
            
            event = Event(raw_event, data_formatter)
            # Skip if event has empty payload (e.g., header line)
            if not event.payload or len(event.payload) == 0:
                continue
            if event.type not in self._event_types_listeners:
                continue
            self.__remove_expired_freezers(event)

            # Check for overload and perform load shedding if needed
            if self.__load_shedding_enabled:
                self.__check_and_shed_load(event.timestamp)

            if not self.__is_multi_pattern_mode and self.__statistics_collector is not None:
                # TODO: support multi-pattern mode
                last_statistics_refresh_time = self.__perform_reoptimization(last_statistics_refresh_time, event)

            self._play_new_event_on_tree(event, matches)
            self._get_matches(matches)
            
            # Track latency
            latency = time.time() - start_time
            self.__total_latency += latency
            self.__event_count += 1

        # Now that we finished the input stream, if there were some pending matches somewhere in the tree, we will
        # collect them now
        self._get_last_pending_matches(matches)
        matches.close()

    def __perform_reoptimization(self, last_statistics_refresh_time: timedelta, last_event: Event):
        """
        If needed, reoptimizes the evaluation mechanism to reflect the current statistical properties of the
        input event stream.
        """
        self.__statistics_collector.handle_event(last_event)
        if not self._should_try_reoptimize(last_statistics_refresh_time, last_event):
            # it is not yet time to recalculate the statistics
            return last_statistics_refresh_time
        new_statistics = self.__statistics_collector.get_statistics()
        if self.__optimizer.should_optimize(new_statistics, self._pattern):
            new_tree_plan = self.__optimizer.build_new_plan(new_statistics, self._pattern)
            new_tree = Tree(new_tree_plan, self._pattern, self.__storage_params)
            self._tree_update(new_tree, last_event.max_timestamp)
        # this is the new last statistic refresh time
        return last_event.max_timestamp

    def _should_try_reoptimize(self, last_statistics_refresh_time: timedelta, last_event: Event):
        """
        Returns True if statistic recalculation and a reoptimization attempt can now be performed and False otherwise.
        The default implementation merely checks whether enough time has passed since the last reoptimization attempt.
        """
        if last_statistics_refresh_time is None:
            return True
        return last_event.max_timestamp - last_statistics_refresh_time > self.__statistics_update_time_window

    def _get_last_pending_matches(self, matches):
        """
        Collects the pending matches from the tree
        """
        for match in self._tree.get_last_matches():
            matches.add_item(match)

    def _play_new_event(self, event: Event, event_types_listeners):
        """
        Lets the tree handle the event
        """
        for leaf in event_types_listeners[event.type]:
            if self._should_ignore_events_on_leaf(leaf, event_types_listeners):
                continue
            self.__try_register_freezer(event, leaf)
            leaf.handle_event(event)

    def _get_matches(self, matches: OutputStream):
        """
        Collects the ready matches from the tree and adds them to the evaluation matches.
        """
        for match in self._tree.get_matches():
            matches.add_item(match)
            self._remove_matched_freezers(match.events)

    @staticmethod
    def _register_event_listeners(tree: Tree):
        """
        Given tree, register leaf listeners for event types.
        """
        event_types_listeners = {}
        for leaf in tree.get_leaves():
            event_type = leaf.get_event_type()
            if event_type in event_types_listeners:
                event_types_listeners[event_type].append(leaf)
            else:
                event_types_listeners[event_type] = [leaf]
        return event_types_listeners

    def __init_freeze_map(self):
        """
        For each event type specified by the user to be a 'freezer', that is, an event type whose appearance blocks
        initialization of new sequences until it is either matched or expires, this method calculates the list of
        leaves to be disabled.
        """
        sequences = self._pattern.extract_flat_sequences()
        for freezer_event_name in self._pattern.consumption_policy.freeze_names:
            current_event_name_set = set()
            for sequence in sequences:
                if freezer_event_name not in sequence:
                    continue
                for name in sequence:
                    current_event_name_set.add(name)
                    if name == freezer_event_name:
                        break
            if len(current_event_name_set) > 0:
                self.__freeze_map[freezer_event_name] = current_event_name_set

    def _should_ignore_events_on_leaf(self, leaf: LeafNode, event_types_listeners):
        """
        If the 'freeze' consumption policy is enabled, checks whether the given event should be dropped based on it.
        """
        if len(self.__freeze_map) == 0:
            # freeze option disabled
            return False
        for freezer in self.__active_freezers:
            for freezer_leaf in event_types_listeners[freezer.type]:
                if freezer_leaf.get_event_name() not in self.__freeze_map:
                    continue
                if leaf.get_event_name() in self.__freeze_map[freezer_leaf.get_event_name()]:
                    return True
        return False

    def __try_register_freezer(self, event: Event, leaf: LeafNode):
        """
        Check whether the current event is a freezer event, and, if positive, register it.
        """
        if leaf.get_event_name() in self.__freeze_map.keys():
            self.__active_freezers.append(event)

    def _remove_matched_freezers(self, match_events: List[Event]):
        """
        Removes the freezers that have been matched.
        """
        if len(self.__freeze_map) == 0:
            # freeze option disabled
            return False
        self.__active_freezers = [freezer for freezer in self.__active_freezers if freezer not in match_events]

    def __remove_expired_freezers(self, event: Event):
        """
        Removes the freezers that have been expired.
        """
        if len(self.__freeze_map) == 0:
            # freeze option disabled
            return False
        self.__active_freezers = [freezer for freezer in self.__active_freezers
                                  if event.max_timestamp - freezer.min_timestamp <= self._pattern.window]

    def get_structure_summary(self):
        return self._tree.get_structure_summary()

    def __repr__(self):
        return self.get_structure_summary()

    def _tree_update(self, new_tree: Tree, event: Event):
        """
        Registers a new tree in the evaluation mechanism.
        """
        raise NotImplementedError()

    def _play_new_event_on_tree(self, event: Event, matches: OutputStream):
        """
        Lets the tree handle the event.
        """
        raise NotImplementedError()
    
    # Load Shedding Methods
    
    def enable_load_shedding(self, max_partial_matches=10000, target_partial_matches=5000, latency_threshold=0.1):
        """
        Enable load shedding with specified thresholds.
        
        Args:
            max_partial_matches: Trigger shedding when storage exceeds this
            target_partial_matches: Reduce storage to this size after shedding
            latency_threshold: Trigger shedding if average latency exceeds this (seconds)
        """
        self.__load_shedding_enabled = True
        self.__max_partial_matches = max_partial_matches
        self.__target_partial_matches = target_partial_matches
        self.__latency_threshold = latency_threshold
        print(f"Load shedding enabled: max={max_partial_matches}, target={target_partial_matches}, latency_threshold={latency_threshold}s")
    
    def disable_load_shedding(self):
        """Disable load shedding."""
        self.__load_shedding_enabled = False
        print("Load shedding disabled")
    
    def __check_and_shed_load(self, current_time):
        """
        Check if system is overloaded and trigger load shedding if needed.
        """
        # Count total partial matches across all storages
        total_partial_matches = self.__count_partial_matches()
        self.__partial_match_counts.append(total_partial_matches)
        
        # Calculate average latency
        avg_latency = self.__total_latency / max(1, self.__event_count)
        
        # Check if overloaded
        is_overloaded = (total_partial_matches > self.__max_partial_matches) or \
                       (avg_latency > self.__latency_threshold)
        
        if is_overloaded:
            dropped = self.__perform_load_shedding(current_time)
            self.__shedding_events.append({
                'time': current_time,
                'event_count': self.__event_count,
                'partial_matches_before': total_partial_matches,
                'dropped': dropped,
                'avg_latency': avg_latency
            })
            print(f"Load shedding at event {self.__event_count}: dropped {dropped} partial matches (latency={avg_latency:.4f}s)")

    
    def __count_partial_matches(self):
        """
        Count total number of partial matches in all storages.
        """
        total = 0
        try:
            for node in self._tree.get_all_nodes():
                if hasattr(node, '_partial_matches') and node._partial_matches is not None:
                    storage = node._partial_matches
                    # Get the internal buffer (list of partial matches)
                    matches = storage.get_internal_buffer() if hasattr(storage, 'get_internal_buffer') else []
                    total += len(matches)
        except Exception as e:
            pass  # Silently handle if structure differs
        return total
    
    def __perform_load_shedding(self, current_time):
        """
        Perform load shedding across all storage nodes.
        
        Returns:
            Total number of partial matches dropped
        """
        total_dropped = 0
        try:
            for node in self._tree.get_all_nodes():
                if hasattr(node, '_partial_matches') and node._partial_matches is not None:
                    storage = node._partial_matches
                    if hasattr(storage, 'shed_load'):
                        dropped = storage.shed_load(
                            self.__target_partial_matches // 2,  # Distribute target across nodes
                            current_time
                        )
                        total_dropped += dropped
        except Exception as e:
            print(f"Error during load shedding: {e}")
        return total_dropped
    
    def get_load_shedding_stats(self):
        """
        Get statistics about load shedding performance.
        
        Returns:
            Dictionary with monitoring statistics
        """
        avg_latency = self.__total_latency / max(1, self.__event_count) if self.__event_count > 0 else 0
        
        return {
            'event_count': self.__event_count,
            'avg_latency': avg_latency,
            'total_latency': self.__total_latency,
            'shedding_events': len(self.__shedding_events),
            'shedding_details': self.__shedding_events,
            'partial_match_counts': self.__partial_match_counts,
            'max_partial_matches': max(self.__partial_match_counts) if self.__partial_match_counts else 0,
            'avg_partial_matches': sum(self.__partial_match_counts) / len(self.__partial_match_counts) if self.__partial_match_counts else 0
        }
