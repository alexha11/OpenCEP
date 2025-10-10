"""
CEP engine for pattern detection over event streams.
"""
from base.DataFormatter import DataFormatter
from parallel.EvaluationManagerFactory import EvaluationManagerFactory
from parallel.ParallelExecutionParameters import ParallelExecutionParameters
from stream.Stream import InputStream, OutputStream
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from typing import List
from datetime import datetime
from transformation.PatternPreprocessingParameters import PatternPreprocessingParameters
from transformation.PatternPreprocessor import PatternPreprocessor
import logging


class CEP:
    """CEP engine for pattern detection with configurable evaluation and parallelization."""
    def __init__(self, patterns: Pattern or List[Pattern], eval_mechanism_params: EvaluationMechanismParameters = None,
                 parallel_execution_params: ParallelExecutionParameters = None,
                 pattern_preprocessing_params: PatternPreprocessingParameters = None):
        actual_patterns = PatternPreprocessor(pattern_preprocessing_params).transform_patterns(patterns)
        self.__evaluation_manager = EvaluationManagerFactory.create_evaluation_manager(actual_patterns,
                                                                                       eval_mechanism_params,
                                                                                       parallel_execution_params)

    def run(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """Detect patterns in event stream and return elapsed time."""
        start = datetime.now()
        self.__evaluation_manager.eval(events, matches, data_formatter)
        end_time = datetime.now()
        return (end_time - start).total_seconds()

    def get_pattern_match(self):
        """Return one match from output stream."""
        try:
            return self.get_pattern_match_stream().get_item()
        except StopIteration:
            return None

    def get_pattern_match_stream(self):
        """Return output stream of detected matches."""
        return self.__evaluation_manager.get_pattern_match_stream()

    def get_evaluation_mechanism_structure_summary(self):
        """Return evaluation mechanism structure summary."""
        return self.__evaluation_manager.get_structure_summary()
