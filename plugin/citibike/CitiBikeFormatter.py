"""
CitiBike Data Formatter for OpenCEP
"""
from base.DataFormatter import DataFormatter, EventTypeClassifier
from datetime import datetime
import csv


class EventData(dict):
    """Dictionary wrapper for event data."""
    def __init__(self, data_dict):
        super().__init__(data_dict)
    
    def get(self, key, default=None):
        return super().get(key, default)


class CitiBikeEventTypeClassifier(EventTypeClassifier):
    """Event type classifier for CitiBike."""  
    def get_event_type(self, event_payload: dict):
        if event_payload and event_payload.get("_header", False):
            return "Header"
        return "BikeTrip"


class CitiBikeDataFormatter(DataFormatter):
    """CitiBike trip data formatter supporting legacy and modern schemas."""
    
    def __init__(self):
        super().__init__(CitiBikeEventTypeClassifier())
        self.headers = None
        self.modern_schema = False
    
    def _parse_dt(self, v: str):
        if not v:
            return None
        v = v.strip().replace('"', '')
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(v.rstrip('Z'))
        except Exception:
            return None
    
    def parse_event(self, raw_data: str):
        """Convert CSV line to event, supporting legacy and modern schemas."""
        if raw_data.strip() == "":
            return None
            
        if self.headers is None:
            reader = csv.reader([raw_data])
            self.headers = next(reader)
            header_set = {h.strip().lower() for h in self.headers}
            self.modern_schema = ("started_at" in header_set and "ended_at" in header_set)
            return {"_header": True, "_skip": True}
        
        try:
            reader = csv.reader([raw_data])
            row = next(reader)
            if len(row) != len(self.headers):
                return None
            
            norm = {}
            if self.modern_schema:
                raw = {self.headers[i]: (row[i].strip('"') if row[i] else None) for i in range(len(self.headers))}
                st = self._parse_dt(raw.get("started_at") or raw.get("Started at"))
                et = self._parse_dt(raw.get("ended_at") or raw.get("Ended at"))
                norm["starttime"] = st
                norm["stoptime"] = et
                norm["tripduration"] = int(max(0, (et - st).total_seconds())) if st and et else None
                
                def to_int(x):
                    if x is None or x == "" or str(x).upper() == "NULL":
                        return None
                    try:
                        return int(str(x))
                    except ValueError:
                        return None
                        
                norm["start station id"] = to_int(raw.get("start_station_id") or raw.get("Start station id"))
                norm["end station id"] = to_int(raw.get("end_station_id") or raw.get("End station id"))
                
                bike_val = raw.get("bike_id") or raw.get("bikeid")
                try:
                    norm["bikeid"] = int(bike_val) if bike_val not in (None, "", "NULL") else None
                except ValueError:
                    norm["bikeid"] = bike_val if bike_val else None
                
                mc = (raw.get("member_casual") or "").strip().lower()
                norm["usertype"] = "Subscriber" if mc == "member" else ("Customer" if mc == "casual" else None)
                norm["birth year"] = None
                norm["gender"] = None
            else:
                for i, header in enumerate(self.headers):
                    v = row[i].strip('"') if row[i] else None
                    if header in ("tripduration", "start station id", "end station id", "bikeid"):
                        try:
                            norm[header] = int(v) if v and v.upper() != "NULL" else None
                        except ValueError:
                            norm[header] = None
                    elif header in ("starttime", "stoptime"):
                        norm[header] = self._parse_dt(v)
                    else:
                        norm[header] = v
            
            return EventData(norm)
            
        except Exception as e:
            print(f"Error parsing row: {raw_data[:100]}... Error: {e}")
            return None
    
    def get_event_timestamp(self, event_data):
        """Return event timestamp (starttime)."""
        if event_data is None:
            return None
        if event_data.get("_header", False):
            return datetime.now()
        if "starttime" in event_data and isinstance(event_data["starttime"], datetime):
            return event_data["starttime"]
        return datetime.now()
