# (C) 2024 GoodData Corporation
import pathlib
from datetime import datetime, timedelta
from typing import Optional

import gooddata_flight_server as gf
import orjson
import polars as pl
import pyarrow
import structlog
from gooddata_flexconnect import FlexConnectFunction
from kafka import KafkaConsumer, TopicPartition

_LOGGER = structlog.get_logger("kafka_flexconnect_function")
DATA_DIR = pathlib.Path("/tmp/data/kafka")


class KafkaDirectFlexConnectFunction(FlexConnectFunction):
    Name = "KafkaDirectFlexConnectFunction"
    Schema = pyarrow.schema(
        [
            pyarrow.field("transaction_id", pyarrow.string()),
            pyarrow.field("account_id", pyarrow.string()),
            pyarrow.field("amount", pyarrow.float64()),
            pyarrow.field("currency", pyarrow.string()),
            pyarrow.field("timestamp", pyarrow.timestamp("us")),
            pyarrow.field("transaction_type", pyarrow.string()),
        ]
    )

    def call(
        self,
        parameters: dict,
        columns: Optional[tuple[str, ...]],
        headers: dict[str, list[str]],
    ) -> gf.ArrowData:
        consumer = KafkaConsumer(
            "test",
            bootstrap_servers="kafka:9092",
            value_deserializer=lambda x: orjson.loads(x),
        )

        # Define the time range
        start_time = (datetime.now() - timedelta(minutes=5)).timestamp() * 1000
        end_time = (datetime.now() - timedelta(seconds=1)).timestamp() * 1000

        # Get the partition info
        partitions = consumer.partitions_for_topic("test")
        topic_partitions = [TopicPartition("test", p) for p in partitions]

        # Get the offsets for the start and end times
        start_offsets = consumer.offsets_for_times(
            {tp: start_time for tp in topic_partitions}
        )
        end_offsets = consumer.offsets_for_times(
            {tp: end_time for tp in topic_partitions}
        )

        # Seek to the start offsets
        for tp, offset in start_offsets.items():
            if offset:
                consumer.seek(tp, offset.offset)
            else:
                print(
                    f"No offset found for partition {tp.partition} at start time {start_time}",
                    flush=True,
                )

        results = []

        print(f"{start_offsets=}", flush=True)
        print(f"{end_offsets=}", flush=True)
        # Consume messages until the end offsets
        for msg in consumer:
            if msg.offset >= end_offsets[TopicPartition("test", msg.partition)].offset:
                break
            print(msg.value, flush=True)
            converted_value = {
                "transaction_id": msg.value.get("transaction_id"),
                "account_id": msg.value.get("account_id"),
                "amount": float(msg.value.get("amount")),
                "currency": msg.value.get("currency"),
                "timestamp": datetime.strptime(
                    msg.value.get("timestamp"), "%Y-%m-%d %H:%M:%S.%f"
                ),
                "transaction_type": msg.value.get("transaction_type"),
            }
            results.append(converted_value)
        df = pl.DataFrame(results)
        transformed = (
            df.sort("timestamp")
            .group_by_dynamic(
                "timestamp",
                every="1m",
                by=["currency", "transaction_type", "account_id"],
            )
            .agg(pl.col("amount").sum())
            .select(pl.all().shrink_dtype())
        )

        result_table = transformed.to_arrow()
        return result_table

    @staticmethod
    def on_load(ctx: gf.ServerContext) -> None:
        pass
