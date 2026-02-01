#!/usr/bin/env python3

"""
This script demonstrates polymorphic data processing using a common interface
Different processors handle different data types while sharing
the same method names.
"""


class DataProcessor:
    # Base class defining a common processing interface
    def __init__(self, data) -> None:
        self.data = data

    def validate(self) -> str:
        # Default validation behavior
        return "Validation: Data verified"

    def process(self) -> None:
        # Default processing behavior
        return f"Processing data: {self.data}"

    def format_output(self) -> str:
        # Default output formatting behavior
        return f"Output: {self.data}"


class NumericProcessor(DataProcessor):
    # Processor specialized for numeric lists.

    def validate(self) -> str:
        try:
            # check that the data is a list
            if not isinstance(self.data, list):
                return "Error: Validation data"

            # Check that all elements in the list are numeric
            for i in self.data:
                if not isinstance(i, (int, float)):
                    return "Error: Validation data"

            return "Validation: Numeric data verified"
        except (TypeError, ValueError):
            return ("Error: Validation data")

    def format_output(self) -> str:
        try:
            # Compute total and average safely
            total = sum(self.data)
            length = len(self.data)
            average = total / length
            return (
                f"Output: Processed {length} numeric values, "
                f"sum={total}, avg={average:.1f}"
            )
        except (TypeError, ValueError, ZeroDivisionError):
            return "Error: Processed data"


class TextProcessor(DataProcessor):
    # Processor specialized for text strings.

    def validate(self) -> str:
        try:
            if not (isinstance(self.data, str)):
                return "Error: Validation data"
            return ("Validation: Text data verified")
        except (TypeError, ValueError):
            return ("Error: Validation data")

    def format_output(self) -> str:
        try:
            # Count the numebr of characters
            num_chars = len(self.data)

            # Count the number of words
            word_count = 0
            previous_was_space = True

            for char in self.data:
                """
                If current char is not a space and previous was a space
                , a new word starts
                """
                if char != " " and previous_was_space:
                    word_count += 1
                    previous_was_space = False
                elif char == " ":
                    previous_was_space = True

                return (f"Output: Processed text: {num_chars} characters, "
                        f"{word_count} words")
        except (TypeError, ValueError):
            return "Error: Processed data"


class LogProcessor(DataProcessor):
    # Processor specialized for log entries.

    def validate(self) -> str:
        try:
            # Ensure the data is a string
            if not isinstance(self.data, str):
                return "Error: Validation data"

            valid_levels = ["ERROR:", "INFO:", "WARNING:"]
            for level in valid_levels:
                if level in self.data:
                    return "Validation: Log entry verafied"
            return ("Error: Validation data")

        except (TypeError, ValueError):
            return ("Error: Validation data")

    def format_output(self) -> str:
        try:
            # Extract level and message by finding the first colon
            color_index = self.data.find(":")
            if color_index == -1:
                return "Error: Processed data"

            level = self.data[:color_index]
            message = self.data[color_index + 1:].strip()

            # Alert style for error level
            if level == "ERROR":
                return f"Output: [ALERT] ERROR level detected: {message}"

            # Info style for info level
            if level == "INFO":
                return f"Output: [INFO] INFO level detected: {message}"

            # Default style for other levels
            return f"Output: [{level}] {level} level detected: {message}"

        except (TypeError, ValueError):
            return "Error: Processed data"


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    # Numeric example
    print("Initializing Numeric Processor...")
    numeric = NumericProcessor([1, 2, 3, 4, 5])
    print(numeric.process())
    print(numeric.validate())
    print(numeric.format_output())
    print("")

    # Text example
    print("Initializing Text Processor...")
    text = TextProcessor("Hello Nexus World")
    print(text.process())
    print(text.validate())
    print(text.format_output())
    print("")

    # Log example
    print("Initializing Log Processor...")
    log = LogProcessor("ERROR: Connection timeout")
    print(log.process())
    print(log.validate())
    print(log.format_output())
    print("")

    # Polymorphic demo
    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    processors = [
        NumericProcessor([1, 2, 3]),
        TextProcessor("Hello Nexus"),
        LogProcessor("INFO: System ready"),
    ]

    for idx, processor in enumerate(processors, start=1):
        # Same interface used for different data types
        print(f"Result {idx}: {processor.format_output()}")

    print("")
    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
