"""
CitiBike Data Formatter for OpenCEP
Parses CitiBike CSV data into events for pattern detection
"""
from base.DataFormatter import DataFormatter, EventTypeClassifier
from datetime import datetime
import csv
import io


class EventData(dict):
    """Wrapper class to make dictionary compatible with .get() method access"""
    def __init__(self, data_dict):
        super().__init__(data_dict)
    
    def get(self, key, default=None):
        return super().get(key, default)


class CitiBikeEventTypeClassifier(EventTypeClassifier):
    """Event type classifier for CitiBike events"""
    
    def get_event_type(self, event_payload: dict):
        """All CitiBike events are of type 'BikeTrip', except headers which are 'Header'"""
        if event_payload and event_payload.get("_header", False):
            return "Header"  # This type won't match any pattern
        return "BikeTrip"


class CitiBikeDataFormatter(DataFormatter):
    """
    A data formatter for CitiBike trip data.
    Converts CSV rows to events with the required attributes.
    """
    
    def __init__(self):
        """Initialize the formatter"""
        super().__init__(CitiBikeEventTypeClassifier())
        self.headers = None
    
    def parse_event(self, raw_data: str):
        """
        Converts a raw CSV line to a structured event.
        Expected format:
        "tripduration","starttime","stoptime","start station id","start station name",
        "start station latitude","start station longitude","end station id","end station name",
        "end station latitude","end station longitude","bikeid","usertype","birth year","gender"
        """
        if raw_data.strip() == "":
            return None
            
        # Handle CSV header
        if self.headers is None:
            # First line should be headers
            reader = csv.reader([raw_data])
            self.headers = next(reader)
            print(f"Parsed headers: {self.headers}")
            # Return an empty event with special marker - will be filtered out by event type
            return {"_header": True, "_skip": True}
        
        try:
            # Parse CSV row
            reader = csv.reader([raw_data])
            row = next(reader)
            
            if len(row) != len(self.headers):
                return None
                
            # Create event data dictionary
            event_data = {}
            for i, header in enumerate(self.headers):
                value = row[i].strip('"') if row[i] else None
                
                # Convert specific fields to appropriate types
                if header == "bikeid" and value and value != "NULL":
                    try:
                        event_data[header] = int(value)
                    except ValueError:
                        event_data[header] = None
                elif header in ["start station id", "end station id"] and value and value != "NULL":
                    try:
                        event_data[header] = int(value)
                    except ValueError:
                        event_data[header] = None
                elif header in ["start station latitude", "start station longitude", 
                               "end station latitude", "end station longitude"] and value and value != "NULL":
                    try:
                        event_data[header] = float(value)
                    except ValueError:
                        event_data[header] = None
                elif header == "tripduration" and value and value != "NULL":
                    try:
                        event_data[header] = int(value)
                    except ValueError:
                        event_data[header] = None
                elif header in ["starttime", "stoptime"] and value and value != "NULL":
                    # Parse timestamp
                    try:
                        event_data[header] = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        event_data[header] = value
                else:
                    event_data[header] = value
                    
            # Add get method to make object compatible with pattern variables
            event_data = EventData(event_data)
            
            return event_data
            
        except Exception as e:
            print(f"Error parsing row: {raw_data[:100]}... Error: {e}")
            return None
    
    def get_event_timestamp(self, event_data):
        """
        Returns the timestamp for the given event.
        Uses start time as the event timestamp.
        """
        if event_data is None:
            return None
        # Special handling for header events
        if event_data.get("_header", False):
            return datetime.now()  # Use current time for header events
        if "starttime" in event_data and isinstance(event_data["starttime"], datetime):
            return event_data["starttime"]
        return datetime.now()  # Default fallback
