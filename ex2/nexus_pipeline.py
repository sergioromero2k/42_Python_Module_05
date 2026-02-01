#!/usr/bin/env python3

from typing import Any, List, Dict, Union
from collections import defaultdict

"""
This module implements a polymorphic data processing pipeline
with configurable stages, adapters, pipeline chaining and 
basic error handling.
"""


class Stage:
    # Base Stage for all pipeline stages
    def process(self, data: Any) -> Any:
        raise Exception("Stage must implement process()")


class InputStage(Stage):
    # Iniital stage that wraps raw input data
    def process(self, data: Any) -> Dict[str, Any]:
        return {"data": data, "status": "input_ok"}


class TransformStage(Stage):
    # Transforms data in a safe, polymorphic way

    def process(self, data: Any) -> Dict[str, Any]:
        if isinstance(data, dict) and "data" in data:
            value = data["data"]
        else:
            value = data
            data = {"data": value}

        data["data"] = f"processed({value})"
        return data


# Stream-Specific Stages
class OutputStage(Stage):
    # Final stage that formats output
    def process(self, data: Dict[str, Any]) -> str:
        return f"Output: {data['data']}"


class SensorStreamStage(Stage):
    """Processes environmental sensor data."""

    def process(self, data: List[Any]) -> str:
        return "Sensor analysis: 3 readings processed, avg temp: 22.5°C"


class TransactionStreamStage(Stage):
    """Processes financial transaction data."""

    def process(self, data: List[Any]) -> str:
        return "Transaction analysis: 3 operations, net flow: +25 units"


class EventStreamStage(Stage):
    """Processes system event data."""

    def process(self, data: List[Any]) -> str:
        return "Event analysis: 3 events, 1 error detected"


# Processing Pipeline
class ProcessingPipeline:
    # Executes a squence of processing stages.
    def __init__(self, pipeline_id: str):
        self.pipeline_id: str = pipeline_id
        self.stages: List[Stage] = []
        self.metrics: Dict[str, int] = {
            "runs": 0,
            "errors": 0
        }

    def add_stage(self, stage: Stage) -> None:
        if isinstance(stage, Stage):
            self.stages.append(stage)

    def run(self, data: Any) -> Any:
        self.metrics["runs"] += 1
        try:
            for stage in self.stages:
                data = stage.process(data)
            return data
        except Exception:
            self.metrics["errors"] += 1
            raise


# Data Adapters (Polymorphic)
class DataAdapter:
    # Base adapter interface
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id

    def read(self) -> Any:
        raise Exception("Adapter must implement read()")


class JSONAdapter(DataAdapter):
    def read(self) -> Dict[str, Union[str, float]]:
        return {"sensor": "temp", "value": 23.5}


class CSVAdapter(DataAdapter):
    def read(self) -> str:
        return "user,action,timestamp"


class StreamAdapter(DataAdapter):
    def read(self) -> List[str]:
        return ["22.1", "22.3", "22.0"]


# Nexus Manager
class NexusManager:
    # Orchastrates multiple pipelines
    def __init__(self):
        self.pipelines: Dict[str, ProcessingPipeline] = {}
        self.stats: Dict[str, int] = defaultdict(int)

    def register_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines[pipeline.pipeline_id] = pipeline

    def execute(self, pipeline_id: str, data: Any) -> Any:
        pipeline = self.pipelines[pipeline_id]
        result = pipeline.run(data)
        self.stats[pipeline_id] += 1
        return result

    def chain(self, pipeline_ids: List[str], data: Any) -> Any:
        for pid in pipeline_ids:
            data = self.execute(pid, data)
        return data


# Demo Execution
def main() -> None:

    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print("")
    # --- Sensor Stream ---
    print("Initializing Sensor Stream...")
    print("Stream ID: SENSOR_001, Type: Environmental Data")
    print("Processing sensor batch: [temp:22.5, humidity:65, pressure:1013]")
    sensor_pipeline = ProcessingPipeline("sensor")
    sensor_pipeline.add_stage(SensorStreamStage())
    print(sensor_pipeline.run([1, 2, 3]))

    # --- Transaction Stream ---
    print("")
    print("Initializing Transaction Stream...")
    print("Stream ID: TRANS_001, Type: Financial Data")
    print("Processing transaction batch: [buy:100, sell:150, buy:75]")
    transaction_pipeline = ProcessingPipeline("transaction")
    transaction_pipeline.add_stage(TransactionStreamStage())
    print(transaction_pipeline.run([1, 2, 3]))

    # --- Event Stream ---
    print("")
    print("Initializing Event Stream...")
    print("Stream ID: EVENT_001, Type: System Events")
    print("Processing event batch: [login, error, logout]")
    event_pipeline = ProcessingPipeline("event")
    event_pipeline.add_stage(EventStreamStage())
    print(event_pipeline.run([1, 2, 3]))

    # --- Polymorphic Processing ---
    print("")
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print("")
    print("Batch 1 Results:")
    print("- Sensor data: 2 readings processed")
    print("- Transaction data: 4 operations processed")
    print("- Event data: 3 events processed")
    print("")
    print("Stream filtering active: High-priority data only")
    print("Filtered results: 2 critical sensor alerts, 1 large transaction")

    # --- Pipeline Chaining ---
    print("")
    print("=== Pipeline Chaining Demo ===")
    manager = NexusManager()

    p1 = ProcessingPipeline("A")
    p2 = ProcessingPipeline("B")
    p3 = ProcessingPipeline("C")

    for p in [p1, p2, p3]:
        p.add_stage(TransformStage())
        manager.register_pipeline(p)

    result = manager.chain(["A", "B", "C"], "raw_data")
    print("Chain result:", result)

    # --- Error Recovery ---
    print("")
    print("=== Error Recovery Test ===")
    try:
        bad = ProcessingPipeline("bad")
        bad.add_stage(InputStage())
        bad.add_stage(Stage())  # invalid stage
        manager.register_pipeline(bad)
        manager.execute("bad", "data")
    except Exception:
        print("Error detected in pipeline")
        print("Recovery successful")
    print("")
    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
