from datetime import datetime
from base.DataFormatter import DataFormatter, EventTypeClassifier

class CitiBikeEventTypeClassifier(EventTypeClassifier):
    """
    CitiBikeEventTypeClassifier inherits from EventTypeClassifier.
    """
    def get_event_type(self, event_payload: dict):
        """
        Returns the event type based on the rideable_type field.
        """
        return event_payload.get("rideable_type", "unknown")


class CitiBikeDataFormatter(DataFormatter):
    """
    Handles CitiBike trip data in CSV format.
    """
    
    def __init__(self, event_type_classifier: EventTypeClassifier = None):
        """
        Initialize the formatter with an optional event type classifier.
        If none provided, defaults to CitiBikeEventTypeClassifier.
        """
        if event_type_classifier is None:
            event_type_classifier = CitiBikeEventTypeClassifier()
        super().__init__(event_type_classifier)
        self._headers = None
    
    def set_headers(self, headers):
        """
        Set CSV headers for parsing. This should be called when reading the CSV file.
        """
        self._headers = headers
    
    def parse_event(self, raw_data: str):
        """
        Parses a CSV line into an event payload dictionary.
        """
        raw_data = raw_data.strip()
        values = raw_data.split(',')
        
        if not self._headers:
            if self.is_header_row(values):
                self._headers = values
                return None
            else:
                raise Exception("Headers must be set before parsing events")
        
        if values == self._headers:
            return None

        if len(values) != len(self._headers):
            raise Exception(f"Data mismatch: expected {len(self._headers)} values but got {len(values)}")
        
        payload = {}
        for i, header in enumerate(self._headers):
            value = values[i]
            # Try to convert numeric fields
            if header in ['start station latitude', 'start station longitude', 'end station latitude', 'end station longitude']:
                try:
                    payload[header] = float(value)
                except ValueError:
                    payload[header] = value
            else:
                payload[header] = value
        
        return payload
    
    def is_header_row(self, values):
        """
        Check if a row is likely to be a header row based on expected column names.
        """
        expected_headers = ["tripduration","starttime","stoptime","start station id",
                            "start station name","start station latitude","start station longitude",
                            "end station id","end station name","end station latitude",
                            "end station longitude","bikeid","usertype","birth year","gender"]
        
        matching_headers = sum(1 for header in values if header in expected_headers)
        return matching_headers >= 5
    
    def get_event_timestamp(self, event_payload: dict):
        """
        Extracts timestamp from the starttime field.
        Format: YYYY-MM-DD HH:MM:SS.mmm
        """
        timestamp_str = event_payload.get("starttime")
        if not timestamp_str:
            raise Exception("No starttime timestamp found in event")
        
        try:
            # Parse timestamp with milliseconds
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
            # Convert to Unix timestamp in milliseconds
            return int(dt.timestamp() * 1000)
        except ValueError:
            # Try without milliseconds
            try:
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                return int(dt.timestamp() * 1000)
            except ValueError:
                raise Exception(f"Invalid timestamp format: {timestamp_str}")