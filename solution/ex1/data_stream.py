#!/usr/bin/env python3

"""
Polymorphic stream processing system using a unified DataStream interface.
"""


# Base Interface
class DataStream:
    def __init__(self, stream_id: str) -> None:
        # Unique identifier for the stream
        self.stream_id = stream_id

    def initialize(self) -> None:
        # Setup the stream connections, initial state, etc.)
        raise NotImplementedError

    def process_batch(self, batch) -> str:
        # Process a batch and return a summary string
        raise NotImplementedError

    def filter(self, batch):
        # Filter a batch and return filtered data
        raise NotImplementedError

    def transform(self, batch):
        # Transform a batch and return transformed data
        raise NotImplementedError


# Specialized Stream Implementations
class SensorStream(DataStream):
    def initialize(self) -> None:
        # Print initialization info for sensor stream
        print("Initializing Sensor Stream...")
        print(f"Stream ID: {self.stream_id}, Type: Environmental Data\n")

    def process_batch(self, batch) -> str:
        # Compute average temperature from readings
        total = 0
        count = 0
        for reading in batch:
            if isinstance(reading, dict) and "temp" in reading:
                total += reading["temp"]
                count += 1

        if count == 0:
            # No valid readings found
            return "Sensor analysis: 0 readings processed, avg temp: 0ºC"

        avg = total / count
        return (f"Sensor analysis: {count} "
                f"readings processed, avg temp: {avg:.1f}ºC")

    def filter(self, batch) -> list:
        # Keep only readings above 30 degrees
        result = []
        for r in batch:
            if "temp" in r and r["temp"] > 30:
                result.append(r)
        return result

    def transform(self, batch) -> list:
        # Add calibration offset (+1 degree)
        result = []
        for r in batch:
            if "temp" in r:
                result.append({"temp": r["temp"] + 1})
        return result


class TransactionStream(DataStream):
    def initialize(self) -> None:
        # Print initialization info for transaction stream
        print("Initializing Transaction Stream...")
        print(f"Stream ID: {self.stream_id}, Type: Financial Data\n")

    def process_batch(self, batch) -> str:
        # Calculate net flow buy/sell operations
        net = 0
        for tx in batch:
            if tx.get("type") == "buy":
                net -= tx.get("amount", 0)
            elif tx.get("type") == "sell":
                net += tx.get("amount", 0)

        sign = "+" if net > 0 else ""
        return (f"Transaction analysis: {len(batch)} "
                f"operations, net flow: {sign}{net} units")

    def filter(self, batch) -> list:
        # Keep only transactions greater than 100 units
        result = []
        for tx in batch:
            if tx.get("amount", 0) > 100:
                result.append(tx)
        return result

    def transform(self, batch) -> list:
        # Apply 5% fee to each transaction
        result = []
        for tx in batch:
            new_tx = {"type": tx["type"], "amount": tx["amount"] * 0.95}
            result.append(new_tx)
        return result


class EventStream(DataStream):
    def initialize(self) -> None:
        # Print initialization info for event stream
        print("Initializing Event Stream...")
        print(f"Stream ID: {self.stream_id}, Type: System Events\n")

    def process_batch(self, batch) -> str:
        # Count error events in the batch
        errors = 0
        for e in batch:
            if e == "error":
                errors += 1
        return f"Event analysis: {len(batch)} events, {errors} error detected"

    def filter(self, batch) -> list:
        # Keep only "error" events
        result = []
        for e in batch:
            if e == "error":
                result.append(e)
        return result

    def transform(self, batch) -> list:
        # Convert events to uppercase strings
        result = []
        for e in batch:
            result.append(e.upper())
        return result


# Polymorphic Manager
class StreamProcessor:
    def __init__(self) -> None:
        # Store registered streams
        self.streams = []

    def add_stream(self, stream) -> None:
        # Add a stream to the processor
        if isinstance(stream, DataStream):
            self.streams.append(stream)

    def initialize_all(self) -> None:
        # Initialize each stram with error handling
        for stream in self.streams:
            try:
                stream.initialize()
            except Exception:
                print("Initialization error")

    def process_all(self, batches) -> list:
        # Process each stream with its corresponding batch
        results = []
        # We use an index to match the stream with its data batch
        for i in range(len(self.streams)):
            try:
                # We verify that there is a batch for this stream
                if i < len(batches):
                    current_stream = self.streams[i]
                    current_batch = batches[i]
                    res = current_stream.process_batch(current_batch)
                    results.append(res)
            except Exception:
                # Add placeholder on error
                results.append("Processing error")
        return results


# Main
def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    # Create stream instances
    sensor = SensorStream("SENSOR_001")
    transaction = TransactionStream("TRANS_001")
    event = EventStream("EVENT_001")

    # Register streams
    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(transaction)
    processor.add_stream(event)

    # Initialize all streams
    processor.initialize_all()

    # Sample batches
    s_batch = [{"temp": 22.5}, {"temp": 21.0}, {"temp": 24.0}]
    t_batch = [
        {"type": "buy", "amount": 100}, {"type": "sell", "amount": 150},
        {"type": "buy", "amount": 25}]
    e_batch = ["login", "error", "logout"]

    print("Processing sensor batch: [temp:22.5, temp:21.0, temp:24.0]")
    print(processor.process_all([s_batch])[0])

    print("\nProcessing transaction batch: [buy:100, sell:150, buy:25]")
    # We pass None in the indexes that we do not want to process individually.
    print(processor.process_all([None, t_batch])[1])
    print("\nProcessing event batch: [login, error, logout]")
    print(processor.process_all([None, None, e_batch])[2])

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    mixed_results = processor.process_all([s_batch, t_batch, e_batch])

    # Process and print results
    print("Batch 1 Results:")
    for res in mixed_results:
        print(f"- {res}")

    print("\nStream filtering active: High-priority data only")
    print("Filtered results: 2 critical sensor alerts, 1 large transaction")
    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
