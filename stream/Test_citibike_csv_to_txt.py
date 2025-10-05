# Memo
# streamディレクトリにいる状態でpython3 Test_citibike_csv_to_txt.pyを実行
#!/usr/bin/env python3
"""
CitiBike CSV to TXT Example
Demonstrates how to read CSV files and write them to text files
without pattern matching, just for testing the stream functionality.
"""

import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base.Event import Event
from stream import (
    MultiDirectoryCSVStream,
    CitiBikeDataFormatter,
    CitiBikeEventTypeClassifier,
    FileOutputStream
)

def csv_to_txt_multi_directory():
    """
    Read CSV files from multiple directories and write to a text file
    """
    print("\n=== Multi-Directory CSV to TXT ===")
    
    formatter = CitiBikeDataFormatter(CitiBikeEventTypeClassifier())    
    directories = [
        "../data/2013-citibike-tripdata",
    ]
    input_stream = MultiDirectoryCSVStream(directories, "*.csv", formatter, has_header=True)    
    os.makedirs("./output", exist_ok=True)
    output_path = "./output/citibike_multi_dir_output.txt"

    with open(output_path, 'w') as f:
        count = 0
        current_file = None
        
        for line in input_stream:
            try:
                parsed = formatter.parse_event(line)
                if parsed is None:
                    print("Skipping header row")
                    continue
                event = Event(line, formatter)           
                f.write(f"Event {count + 1}:\n")
                f.write(f"  Type: {event.type}\n")
                f.write(f"  Timestamp: {event.timestamp}\n")
                f.write(f"  Ride ID: {event.payload.get('bikeid', 'N/A')}\n")
                f.write(f"  Start Station: {event.payload.get('start station name', 'N/A')}\n")
                f.write(f"  End Station: {event.payload.get('end station name', 'N/A')}\n")
                f.write("-" * 50 + "\n")
                count += 1
            except Exception as e:
                print(f"Error processing line: {e}")
                print(f"Line data: {line}")
                continue

    print(f"Wrote {count} events to {output_path}")

def csv_to_txt_with_fileoutputstream():
    """
    Use FileOutputStream to write CSV data
    """
    print("\n=== CSV to TXT with FileOutputStream ===")
    
    formatter = CitiBikeDataFormatter(CitiBikeEventTypeClassifier())    
    directories = ["../data/202506-citibike-tripdata"]
    input_stream = MultiDirectoryCSVStream(directories, "*.csv", formatter, has_header=True)
    os.makedirs("./output", exist_ok=True)
    output_stream = FileOutputStream("./output", "citibike_fileoutputstream.txt", is_async=False)
    
    count = 0
    for line in input_stream:
        try:
            # First parse to check if it's a header
            parsed = formatter.parse_event(line)
            if parsed is None:
                print("Skipping header row")
                continue
                
            # Parse the line into an event
            event = Event(line, formatter)        
            output_text = f"Event {count + 1}: Type={event.type}, Station={event.payload.get('start_station_name', 'N/A')}, Time={event.timestamp}\n"        
            output_stream.add_item(output_text)
            count += 1
            if count >= 10:
                break
        except Exception as e:
            print(f"Error processing line: {e}")
            continue
    
    # Close the stream to write all data
    output_stream.close()
    
    print(f"Wrote {count} events using FileOutputStream")


def main():
    """
    Run all examples
    """
    try:
        csv_to_txt_multi_directory()
        csv_to_txt_with_fileoutputstream()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()