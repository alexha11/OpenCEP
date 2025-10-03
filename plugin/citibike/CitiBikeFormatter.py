"""
Citi Bike data formatter for OpenCEP
Handles CSV format from Citi Bike trip data
"""
from datetime import datetime
from typing import Any, Dict, Optional
from base.DataFormatter import DataFormatter, EventTypeClassifier


class CitiBikeByBikeIdEventTypeClassifier(EventTypeClassifier):
    """
    Classifier that treats all events as same type (BikeTrip)
    """
    def get_event_type(self, event_payload: dict):
        return "BikeTrip"


class CitiBikeDataFormatter(DataFormatter):
    """
    Formatter for Citi Bike CSV data.
    
    Expected CSV format:
    tripduration,starttime,stoptime,start station id,start station name,
    start station latitude,start station longitude,end station id,end station name,
    end station latitude,end station longitude,bikeid,usertype,birth year,gender
    """
    
    def __init__(self, event_type_classifier: EventTypeClassifier = CitiBikeByBikeIdEventTypeClassifier()):
        super().__init__(event_type_classifier)
        self.header_parsed = False
        self.headers = []
    
    def parse_event(self, raw_data: str):
        """
        Parses a Citi Bike CSV line into an event dictionary.
        """
        # Skip empty lines
        if not raw_data.strip():
            return {}
        
        # Handle header line (first line)
        if not self.header_parsed:
            self.headers = [h.strip().strip('"') for h in raw_data.split(',')]
            self.header_parsed = True
            return {}  # Return empty dict to skip header
        
        # Parse data line
        try:
            # Split by comma (handle quoted fields)
            values = []
            current = []
            in_quotes = False
            
            for char in raw_data:
                if char == '"':
                    in_quotes = not in_quotes
                elif char == ',' and not in_quotes:
                    values.append(''.join(current).strip().strip('"'))
                    current = []
                else:
                    current.append(char)
            values.append(''.join(current).strip().strip('"'))
            
            # Create event dictionary
            event_dict = {}
            for i, header in enumerate(self.headers):
                if i < len(values):
                    value = values[i]
                    # Handle NULL values
                    if value in ['\\N', 'NULL', '']:
                        event_dict[header] = None
                    elif header in ['tripduration', 'bikeid', 'start station id', 'end station id', 'birth year', 'gender']:
                        # Convert to int
                        try:
                            event_dict[header] = int(value)
                        except ValueError:
                            event_dict[header] = None
                    elif header in ['start station latitude', 'start station longitude', 
                                   'end station latitude', 'end station longitude']:
                        # Convert to float
                        try:
                            event_dict[header] = float(value)
                        except ValueError:
                            event_dict[header] = None
                    else:
                        event_dict[header] = value
            
            return event_dict
            
        except Exception as e:
            print(f"Error parsing line: {e}")
            return None
    
    def get_event_timestamp(self, event_payload: dict):
        """
        Extract timestamp from stoptime (when trip ended).
        Format: "2013-07-01 00:10:34"
        """
        try:
            timestamp_str = event_payload.get('stoptime', event_payload.get('starttime'))
            if timestamp_str:
                return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            else:
                return datetime.now()
        except Exception as e:
            print(f"Error parsing timestamp: {e}")
            return datetime.now()
    
    def get_probability(self, event_payload: Dict[str, Any]) -> Optional[float]:
        """Not used for Citi Bike data"""
        return None
