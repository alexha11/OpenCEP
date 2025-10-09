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
    Supports both legacy schema (2017–2019) and modern schema (2020+).
    """
    
    def __init__(self):
        """Initialize the formatter"""
        super().__init__(CitiBikeEventTypeClassifier())
        self.headers = None
        self.modern_schema = False  # True if headers look like 2020+ (started_at, ended_at, etc.)
    
    def _parse_dt(self, v: str):
        if not v:
            return None
        v = v.strip().replace('"', '')
        # Try common formats
        for fmt in ("%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                continue
        # Fallback to fromisoformat (may not handle Z)
        try:
            v2 = v.rstrip('Z')
            return datetime.fromisoformat(v2)
        except Exception:
            return None
    
    def parse_event(self, raw_data: str):
        """
        Converts a raw CSV line to a structured event.
        Legacy expected format:
        "tripduration","starttime","stoptime","start station id","start station name",
        "start station latitude","start station longitude","end station id","end station name",
        "end station latitude","end station longitude","bikeid","usertype","birth year","gender"
        Modern format (2020+ typical):
        ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,
        end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual[,bike_id]
        """
        if raw_data.strip() == "":
            return None
            
        # Handle CSV header
        if self.headers is None:
            reader = csv.reader([raw_data])
            self.headers = next(reader)
            # Detect modern schema by key names
            header_set = {h.strip().lower() for h in self.headers}
            self.modern_schema = ("started_at" in header_set and "ended_at" in header_set)
            # Return an empty event with special marker - will be filtered out by event type
            return {"_header": True, "_skip": True}
        
        try:
            reader = csv.reader([raw_data])
            row = next(reader)
            if len(row) != len(self.headers):
                return None
            
            # Normalize into legacy-style keys expected by the engine
            norm = {}
            if self.modern_schema:
                # Build a dict of raw header -> value
                raw = {self.headers[i]: (row[i].strip('"') if row[i] else None) for i in range(len(self.headers))}
                # Timestamps
                st = self._parse_dt(raw.get("started_at") or raw.get("Started at"))
                et = self._parse_dt(raw.get("ended_at") or raw.get("Ended at"))
                norm["starttime"] = st
                norm["stoptime"] = et
                # Trip duration seconds
                if st and et:
                    td = int(max(0, (et - st).total_seconds()))
                else:
                    td = None
                norm["tripduration"] = td
                
                # Station IDs (cast to int when possible)
                def to_int(x):
                    if x is None or x == "" or str(x).upper() == "NULL":
                        return None
                    try:
                        return int(str(x))
                    except ValueError:
                        return None
                norm["start station id"] = to_int(raw.get("start_station_id") or raw.get("Start station id"))
                norm["end station id"] = to_int(raw.get("end_station_id") or raw.get("End station id"))
                
                # Bike ID (may be missing in modern schema). Try bike_id; otherwise None
                bike_val = raw.get("bike_id") or raw.get("bikeid")
                try:
                    norm["bikeid"] = int(bike_val) if bike_val not in (None, "", "NULL") else None
                except ValueError:
                    # Keep as string if not numeric, else None
                    norm["bikeid"] = bike_val if bike_val else None
                
                # User type mapping from member_casual
                mc = (raw.get("member_casual") or "").strip().lower()
                if mc == "member":
                    norm["usertype"] = "Subscriber"
                elif mc == "casual":
                    norm["usertype"] = "Customer"
                else:
                    norm["usertype"] = None
                
                # Optional fields absent in modern schema
                norm["birth year"] = None
                norm["gender"] = None
            else:
                # Legacy schema: map directly
                for i, header in enumerate(self.headers):
                    h = header
                    v = row[i].strip('"') if row[i] else None
                    if h in ("tripduration",):
                        try:
                            norm[h] = int(v) if v and v.upper() != "NULL" else None
                        except ValueError:
                            norm[h] = None
                    elif h in ("start station id", "end station id"):
                        try:
                            norm[h] = int(v) if v and v.upper() != "NULL" else None
                        except ValueError:
                            norm[h] = None
                    elif h in ("starttime", "stoptime"):
                        norm[h] = self._parse_dt(v)
                    elif h == "bikeid":
                        try:
                            norm[h] = int(v) if v and v.upper() != "NULL" else None
                        except ValueError:
                            norm[h] = None
                    else:
                        norm[h] = v
            
            # Wrap for .get compatibility
            event_data = EventData(norm)
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
