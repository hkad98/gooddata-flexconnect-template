# (C) 2024 GoodData Corporation
import pathlib
from datetime import datetime, timedelta
from typing import Optional

import gooddata_flight_server as gf
import polars as pl
import pyarrow
import pyarrow.parquet as pq
import structlog
from gooddata_flexconnect import ExecutionContext, ExecutionType, FlexConnectFunction

_LOGGER = structlog.get_logger("kafka_flexconnect_function")
DATA_DIR = pathlib.Path("/tmp/data/kafka")


class KafkaFlexConnectFunction(FlexConnectFunction):
    Name = "KafkaFlexConnectFunction"
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
        time_minus_10_minutes = datetime.now() - timedelta(minutes=10)
        from_time = pyarrow.scalar(time_minus_10_minutes, type=pyarrow.timestamp("us"))
        dataset = pq.ParquetDataset(
            DATA_DIR,
            schema=self.Schema,
            filters=[
                ("currency", "in", ["USD", "EUR", "GBP", "JPY", "AUD"]),
                ("timestamp", ">=", from_time),
            ],
        )

        table = dataset.read()

        df = pl.from_arrow(table)
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

        _LOGGER.info(
            "function_called",
            parameters=parameters,
            table_size=table.nbytes,
            result_size=result_table.nbytes,
        )

        execution_context = ExecutionContext.from_parameters(parameters)
        if execution_context is None:
            # This can happen for invalid invocations that do not come from GoodData
            raise ValueError("Function did not receive execution context.")

        _LOGGER.info("execution_context", execution_context=execution_context)

        if execution_context.execution_type == ExecutionType.REPORT:
            _LOGGER.info(
                "Received report execution request",
                report_execution_context=execution_context.report_execution_request,
            )
        elif execution_context.execution_type == ExecutionType.LABEL_ELEMENTS:
            _LOGGER.info(
                "Received label elements execution request",
                label_elements_execution_context=execution_context.label_elements_execution_request,
            )
        else:
            _LOGGER.info("Received unknown execution request")
        return result_table

    @staticmethod
    def on_load(ctx: gf.ServerContext) -> None:
        pass
