import json
from datetime import datetime


class TimestampSerializer:
    # Define the acceptable formats for the timestamp
    TIMESTAMP_FORMATS = [
        "%Y/%m/%d %H:%M:%S",  # yyyy/MM/dd HH:mm:ss
        "%Y-%m-%d %H:%M:%S"  # yyyy-MM-dd HH:mm:ss
    ]

    @classmethod
    def parse_timestamp(cls, timestamp_str):
        # Try to parse the timestamp using the known formats
        for fmt in cls.TIMESTAMP_FORMATS:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                # If the format doesn't match, continue to try the next format
                continue
        raise ValueError(f"Timestamp format not recognized: {timestamp_str}")

    @classmethod
    def serialize(cls, data):

        # Process the timestamp field
        if 'timestamp' in data:
            # Parse the timestamp using the custom logic
            parsed_timestamp = cls.parse_timestamp(data['timestamp'])
            # Optionally, reformat the timestamp to a standardized format (Fivetran recommends ISO 8601 format)
            data['timestamp'] = parsed_timestamp.isoformat()

        # Return the dictionary
        return data
