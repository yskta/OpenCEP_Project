#!/usr/bin/env python3
# execute "python3 -m adaptive.statistics.Test" at root dir
from datetime import timedelta

from base.Event import Event
from base.Pattern import Pattern
from base.PatternStructure import PrimitiveEventStructure
from base.PatternStructure import SeqOperator, OrOperator
from stream import (
    MultiDirectoryCSVStream,
    CitiBikeDataFormatter,
    CitiBikeEventTypeClassifier,
)
from adaptive.statistics.StatisticsCollectorFactory import (
    StatisticsCollectorFactory,
    StatisticsCollectorParameters
)
from adaptive.statistics.StatisticsTypes import StatisticsTypes

def test_basic_stats():
    """
    Test basic statistics collection using standard StatisticsCollector
    """
    print("\n=== Basic Statistics Collection Test ===")

    # Create a pattern
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("classic_bike", "a"),
            PrimitiveEventStructure("electric_bike", "b")
        ),
        None,
        timedelta(minutes=5)
    )

    # Initialize statistics collector
    params = StatisticsCollectorParameters(
        statistics_time_window=timedelta(minutes=5),
        # Todo:add statistics type
        statistics_types=[StatisticsTypes.ARRIVAL_RATES]
    )

    # 1.StatisticsCollectorFactory use StatisticsFactory
    # 2.StatisticsFactory create ArrivalRatesStatistics instance
    # 3.the instance included into dict in stats_collector
    # 4.StatisticsCollector call update() in handle_event like self.__statistics[statistics_type].update(data)
    stats_collector = StatisticsCollectorFactory.build_statistics_collector(params, [pattern])

    # Setup CitiBike data stream
    formatter = CitiBikeDataFormatter(CitiBikeEventTypeClassifier())    
    directories = ["data/202506-citibike-tripdata"]
    input_stream = MultiDirectoryCSVStream(directories, "*.csv", formatter, has_header=True)

    print("Processing events...")
    event_count = 0

    for line in input_stream:
        try:
            parsed = formatter.parse_event(line)
            if parsed is None: #header skip
                continue
            event = Event(line, formatter)
            stats_collector.handle_event(event)
            event_count += 1    
            if event_count >= 100:
                break

        except Exception as e:
            print(f"Error processing event: {e}")
            continue
    print(f"event count: {event_count}")
    # Get statistics
    stats = stats_collector.get_statistics()
    print(f"\nCollected Statistics:")
    for stat_type, values in stats.items():
        print(f"  {stat_type}: {values}")

def main():
    """
    Run all tests
    """
    print("CitiBike Statistics Testing Suite")
    print("=" * 50)
    
    try:
        test_basic_stats()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()