"""
Wrapper class to add detailed logging to event processing
"""
import logging
from base.DataFormatter import DataFormatter
from base.Event import Event
from stream.Stream import InputStream, OutputStream


class LoggingEvaluationWrapper:
    """Wrapper around evaluation mechanism to add detailed logging"""
    
    def __init__(self, evaluation_mechanism):
        self.evaluation_mechanism = evaluation_mechanism
        self.logger = logging.getLogger(__name__)
        self.event_count = 0
        
    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """Evaluate events with detailed logging"""
        self.logger.info("Starting event-by-event processing...")
        self.event_count = 0
        
        # Create our own event processing loop with logging
        for raw_event in events:
            self.event_count += 1
            
            try:
                # Parse event
                event = Event(raw_event, data_formatter)
                
                # Log event processing
                if self.event_count % 5 == 0 or self.event_count <= 10:
                    self.logger.info(f"Processing event {self.event_count}: type={event.type}, "
                                   f"timestamp={event.timestamp}")
                    if hasattr(event, 'payload') and 'bikeid' in event.payload:
                        self.logger.info(f"  -> BikeID: {event.payload.get('bikeid')}, "
                                       f"Start station: {event.payload.get('start station id')}, "
                                       f"End station: {event.payload.get('end station id')}")
                
                # Skip header events
                if event.type == "Header":
                    self.logger.info(f"  -> Skipping header event")
                    continue
                
                # Process the event through the original mechanism
                # We'll need to access internal methods for this
                if hasattr(self.evaluation_mechanism, '_event_types_listeners'):
                    if event.type not in self.evaluation_mechanism._event_types_listeners:
                        self.logger.debug(f"  -> Event type {event.type} not in listeners, skipping")
                        continue
                    
                    self.logger.debug(f"  -> Event matches pattern listeners, processing...")
                    # Call the internal event processing method
                    self.evaluation_mechanism._play_new_event_on_tree(event, matches)
                    self.evaluation_mechanism._get_matches(matches)
                    
            except Exception as e:
                self.logger.error(f"Error processing event {self.event_count}: {e}")
                raise
        
        # Get final matches
        if hasattr(self.evaluation_mechanism, '_get_last_pending_matches'):
            self.evaluation_mechanism._get_last_pending_matches(matches)
        
        matches.close()
        self.logger.info(f"Completed processing {self.event_count} events")
