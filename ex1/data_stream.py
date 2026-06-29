from abc import ABC, abstractmethod
from typing import Any, List, Tuple


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: List[Tuple[int, str]] = []
        self._counter = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> Tuple[int, str]:
        if not self._storage:
            raise IndexError("No data")

        return self._storage.pop(0)

    def get_total_processed(self) -> int:
        return self._counter

    def get_remaining(self) -> int:
        return len(self._storage)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)

        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, (int, float)):
            self._storage.append((self._counter, str(data)))
            self._counter += 1

        elif isinstance(data, list):
            for i in data:
                self._storage.append((self._counter, str(i)))
                self._counter += 1


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True

        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)

        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, str):
            self._storage.append((self._counter, data))
            self._counter += 1

        elif isinstance(data, list):
            for i in data:
                self._storage.append((self._counter, i))
                self._counter += 1


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            if "log_level" in data and "log_message" in data:
                return isinstance(data["log_level"], str) and isinstance(
                    data["log_message"], str
                )

        if isinstance(data, list):
            return all(self.validate(item) for item in data)

        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if isinstance(data, dict):
            level = data["log_level"]
            message = data["log_message"]
            self._storage.append((self._counter, f"{level}: {message}"))
            self._counter += 1

        elif isinstance(data, list):
            for item in data:
                level = item["log_level"]
                message = item["log_message"]
                self._storage.append((self._counter, f"{level}: {message}"))
                self._counter += 1


class DataStream:
    def __init__(self) -> None:
        self.processors: List[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: List[Any]) -> None:
        for data in stream:
            processed = False
            for proc in self.processors:
                if proc.validate(data):
                    proc.ingest(data)
                    processed = True
                    break
            if not processed:
                print(
                    "DataStream error - " "Can't process element "
                    f"in stream: {data}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for proc in self.processors:
            if isinstance(proc, NumericProcessor):
                print(
                    f"Numeric Processor: "
                    f"total {proc.get_total_processed()} "
                    f"items processed, "
                    f"remaining "
                    f"{proc.get_remaining()} "
                    f"on processor"
                )
            elif isinstance(proc, TextProcessor):
                print(
                    f"Text Processor: "
                    f"total {proc.get_total_processed()} "
                    f"items processed, "
                    f"remaining "
                    f"{proc.get_remaining()} "
                    f"on processor"
                )
            elif isinstance(proc, LogProcessor):

                print(
                    f"Log Processor: "
                    f"total {proc.get_total_processed()} "
                    f"items processed, "
                    f"remaining "
                    f"{proc.get_remaining()} "
                    f"on processor"
                )


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")

    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Numeric Processor\n")
    stream.register_processor(NumericProcessor())
    batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": ("Telnet access! " "Use ssh instead"),
            },
            {"log_level": "INFO", "log_message": ("User wil is connected")},
        ],
        42,
        ["Hi", "five"],
    ]
    print("Send first batch of data " f"on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nRegistering other " "data processors")
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())
    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()
    print(
        "\nConsume some elements " "from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for proc in stream.processors:
        if isinstance(proc, NumericProcessor):
            for _ in range(3):
                proc.output()
        elif isinstance(proc, TextProcessor):
            for _ in range(2):
                proc.output()
        elif isinstance(proc, LogProcessor):
            for _ in range(1):
                proc.output()
    stream.print_processors_stats()
