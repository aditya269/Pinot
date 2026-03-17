#!/usr/bin/env python3

import csv
import json
import sys

from confluent_kafka import Producer

clock = [">", "=>", "==>", "===>", "====>", "=====>", "======>", "=======>"]

INT_FIELDS = {"exam_record_id", "student_id", "max_marks"}
FLOAT_FIELDS = {"marks"}


class KafkaWriter:
    def __init__(self, config: dict, topic: str) -> None:
        self.p = Producer(config)
        self.topic = topic
        self.count = 0

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(
                f"====> Streaming to Kafka {msg.topic()} [{msg.partition()}]  "
                f"{clock[self.count % len(clock)]}            ",
                end="\r",
            )
            self.count += 1

    def write(self, data: dict):
        self.p.produce(
            topic=self.topic,
            key=str(data["exam_record_id"]),
            value=json.dumps(data),
            on_delivery=self.delivery_report,
        )
        self.p.flush()


def convert_row(row: dict) -> dict:
    converted = {}
    for key, value in row.items():
        if value is None:
            converted[key] = None
            continue

        stripped = value.strip()
        if key in INT_FIELDS:
            converted[key] = int(stripped)
        elif key in FLOAT_FIELDS:
            converted[key] = float(stripped)
        else:
            converted[key] = stripped

    return converted


def load_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        return [convert_row(row) for row in reader]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python student_exam_loader.py /path/to/student_exams.csv")

    rows = load_csv(sys.argv[1])
    writer = KafkaWriter({"bootstrap.servers": "kafka:9092"}, topic="student_exam")

    print(f"Loaded {len(rows)} student exam row(s) from {sys.argv[1]}")
    for row in rows:
        writer.write(row)

    print(f"\nSent {writer.count} record(s) to Kafka")
