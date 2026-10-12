
import glob
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092").split(",")
TOPIC = os.getenv("TOPIC", "flight-events")
DATA_DIR = os.getenv("DATA_DIR", "/data")
SOURCE_FILE = os.getenv("SOURCE_FILE", "").strip()
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))
RATE = float(os.getenv("RATE", "100"))
PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "1000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("flight-producer")


def clean_value(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if hasattr(value, "item"):
        try:
            value = value.item()
        except (ValueError, AttributeError):
            pass

    if isinstance(value, float) and not math.isfinite(value):
        return None

    return value


def make_flight_record(row):
    # Use UTC consistently for ingestion timestamps.
    event_time = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    record = {
        "event_time": event_time,
        "flight_date": clean_value(row.get("FlightDate")),
        "airline": clean_value(row.get("Reporting_Airline")),
        "origin": clean_value(row.get("Origin")),
        "dest": clean_value(row.get("Dest")),
        "dep_time_blk": clean_value(row.get("DepTimeBlk")),
        "dep_delay": clean_value(row.get("DepDelay")),
        "arr_delay": clean_value(row.get("ArrDelay")),
        "cancelled": clean_value(row.get("Cancelled")),
        "cancellation_code": clean_value(row.get("CancellationCode")),
        "distance": clean_value(row.get("Distance")),
    }

    for field in ("dep_delay", "arr_delay", "distance"):
        value = record[field]
        if value is not None:
            try:
                record[field] = float(value)
            except (TypeError, ValueError, OverflowError):
                record[field] = None

    value = record["cancelled"]
    if value is not None:
        try:
            record["cancelled"] = int(float(value))
        except (TypeError, ValueError, OverflowError):
            record["cancelled"] = 0
    else:
        record["cancelled"] = 0

    return record


def discover_csv_files():
    # If SOURCE_FILE is specified, process only that file.
    if SOURCE_FILE:
        source = Path(SOURCE_FILE)

        if not source.is_file():
            raise FileNotFoundError(
                f"SOURCE_FILE does not exist: {SOURCE_FILE}"
            )

        return [str(source)]

    # Otherwise, process all CSV files under DATA_DIR.
    files = sorted(
        glob.glob(
            os.path.join(DATA_DIR, "**", "*.csv"),
            recursive=True,
        )
    )

    if not files:
        raise FileNotFoundError(
            f"No CSV files found under {DATA_DIR}"
        )

    return files


def main():
    total_queued = 0
    total_confirmed = 0
    total_failed = 0
    last_progress = time.monotonic()

    files = discover_csv_files()

    logger.info("Found %d CSV file(s).", len(files))

    for path in files:
        logger.info("Input CSV: %s", path)

    logger.info(
        "Kafka bootstrap=%s topic=%s rate=%s records/sec",
        ",".join(BOOTSTRAP_SERVERS),
        TOPIC,
        RATE,
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
        ).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=20,
        batch_size=65536,
        max_block_ms=120000,
        request_timeout_ms=60000,
    )

    def on_success(_metadata):
        nonlocal total_confirmed
        total_confirmed += 1

    def on_error(exc):
        nonlocal total_failed
        total_failed += 1
        logger.error("Kafka delivery failed: %s", exc)

    try:
        for file_index, path in enumerate(files, start=1):
            logger.info(
                "Reading file %d/%d: %s",
                file_index,
                len(files),
                path,
            )

            for chunk_index, chunk in enumerate(
                pd.read_csv(
                    path,
                    chunksize=CHUNK_SIZE,
                    low_memory=False,
                ),
                start=1,
            ):
                logger.info(
                    "Processing chunk %d from %s (%d rows)",
                    chunk_index,
                    os.path.basename(path),
                    len(chunk),
                )

                for row in chunk.to_dict(orient="records"):
                    record = make_flight_record(row)

                    producer.send(
                        TOPIC,
                        value=record,
                    ).add_callback(
                        on_success
                    ).add_errback(
                        on_error
                    )

                    total_queued += 1

                    if RATE > 0:
                        time.sleep(1.0 / RATE)

                    now_mono = time.monotonic()

                    if (
                        total_queued % PROGRESS_EVERY == 0
                        or now_mono - last_progress >= 15
                    ):
                        producer.flush()

                        logger.info(
                            "Progress: queued=%d confirmed=%d failed=%d",
                            total_queued,
                            total_confirmed,
                            total_failed,
                        )

                        last_progress = now_mono

                producer.flush()

                logger.info(
                    "Chunk complete. Total queued=%d",
                    total_queued,
                )

        producer.flush()

        logger.info(
            "Final delivery status: queued=%d confirmed=%d failed=%d",
            total_queued,
            total_confirmed,
            total_failed,
        )

        if total_failed:
            raise RuntimeError(
                f"{total_failed} Kafka deliveries failed."
            )

        logger.info("All CSV files processed successfully.")

    finally:
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    main()
