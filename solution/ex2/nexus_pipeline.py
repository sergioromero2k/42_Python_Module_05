#!/usr/bin/env python3

from typing import Any, List, Dict, Union
from collections import defaultdict

"""
Polymorphic Data Processing Pipeline
Complete typing added for Code Nexus Exercise 2
"""


class Stage:
    def process(self, data: Any) -> Any:
        raise Exception("Stage must implement process()")


class InputStage(Stage):
    def process(self, data: Any) -> Dict[str, Any]:
        return {"data": data, "status": "input_ok"}


class TransformStage(Stage):
    def process(self, data: Any) -> Dict[str, Any]:
        if isinstance(data, dict) and "data" in data:
            value = data["data"]
        else:
            value = data
            data = {"data": value}
        data["data"] = f"processed({value})"
        return data


class OutputStage(Stage):
    def process(self, data: Dict[str, Any]) -> str:
        return f"Output: {data['data']}"


class ProcessingPipeline:
    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id: str = pipeline_id
        self.stages: List[Stage] = []
        self.metrics: Dict[str, int] = {"runs": 0, "errors": 0}

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

    def process(self, data: Any) -> Any:
        """Default process uses run()"""
        return self.run(data)


# ======================
# Specialized Adapters
# ======================

class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def read(self) -> Dict[str, Union[str, float]]:
        return {"sensor": "temp", "value": 23.5}

    def process(self, data: Any) -> str:
        # Polymorphic override
        json_data = self.read() if data is None else data
        return f"Processed JSON data: {json_data}"


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def read(self) -> str:
        return "user,action,timestamp"

    def process(self, data: Any) -> str:
        csv_data = self.read() if data is None else data
        return f"Processed CSV data: {csv_data}"


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def read(self) -> List[str]:
        return ["22.1", "22.3", "22.0"]

    def process(self, data: Any) -> str:
        stream_data = self.read() if data is None else data
        return f"Processed Stream data: {stream_data}"


# ======================
# Nexus Manager
# ======================

class NexusManager:
    def __init__(self) -> None:
        self.pipelines: Dict[str, ProcessingPipeline] = {}
        self.stats: Dict[str, int] = defaultdict(int)

    def register_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines[pipeline.pipeline_id] = pipeline

    def execute(self, pipeline_id: str, data: Any) -> Any:
        pipeline: ProcessingPipeline = self.pipelines[pipeline_id]
        result: Any = pipeline.process(data)
        self.stats[pipeline_id] += 1
        return result

    def chain(self, pipeline_ids: List[str], data: Any) -> Any:
        for pid in pipeline_ids:
            data = self.execute(pid, data)
        return data


# ======================
# Demo
# ======================

def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    sensor_pipeline: JSONAdapter = JSONAdapter("sensor")
    print("Initializing Sensor Stream...")
    print(sensor_pipeline.process(None))

    csv_pipeline: CSVAdapter = CSVAdapter("transaction")
    print("\nInitializing Transaction Stream...")
    print(csv_pipeline.process(None))

    stream_pipeline: StreamAdapter = StreamAdapter("event")
    print("\nInitializing Event Stream...")
    print(stream_pipeline.process(None))

    print("\n=== Pipeline Chaining Demo ===")
    manager: NexusManager = NexusManager()
    p1: ProcessingPipeline = ProcessingPipeline("A")
    p2: ProcessingPipeline = ProcessingPipeline("B")
    p3: ProcessingPipeline = ProcessingPipeline("C")
    for p in [p1, p2, p3]:
        p.add_stage(TransformStage())
        manager.register_pipeline(p)
    result: Any = manager.chain(["A", "B", "C"], "raw_data")
    print("Chain result:", result)


if __name__ == "__main__":
    main()
