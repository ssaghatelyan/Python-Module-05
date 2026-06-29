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


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    numeric = NumericProcessor()
    print(f"Trying to validate input '42': " f"{numeric.validate(42)}")
    print(f"Trying to validate input 'Hello': " f"{numeric.validate('Hello')}")
    print(
        "Test invalid ingestion of string "
        "'foo' without prior validation:"
        )
    try:
        numeric.ingest("foo")
    except ValueError as error:
        print(f"Got exception: {error}")

    numeric_data = [1, 2, 3, 4, 5]
    print(f"Processing data: {numeric_data}")
    numeric.ingest(numeric_data)
    print("Extracting 3 values...")
    for _ in range(3):
        rank, value = numeric.output()
        print(f"Numeric value {rank}: " f"{value}")

    print("\nTesting Text Processor...")
    text = TextProcessor()
    print(f"Trying to validate input '42': " f"{text.validate(42)}")
    text_data = ["Hello", "Nexus", "World"]
    print(f"Processing data: {text_data}")
    text.ingest(text_data)
    print("Extracting 1 value...")
    rank, value = text.output()
    print(f"Text value {rank}: " f"{value}")

    print("\nTesting Log Processor...")
    log = LogProcessor()
    print(f"Trying to validate input 'Hello': " f"{log.validate('Hello')}")
    log_data = [
        {"log_level": "NOTICE", "log_message": ("Connection to server")},
        {"log_level": "ERROR", "log_message": ("Unauthorized access!!")},
    ]
    print(f"Processing data: {log_data}")
    log.ingest(log_data)
    print("Extracting 2 values...")
    for _ in range(2):
        rank, value = log.output()
        print(f"Log entry {rank}: " f"{value}")
