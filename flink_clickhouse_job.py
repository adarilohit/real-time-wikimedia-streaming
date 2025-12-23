import json
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.time import Time
from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import AggregateFunction, WindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions

# -----------------------------------------------------
# Aggregator
# -----------------------------------------------------
class CountAggregator(AggregateFunction):
    def create_accumulator(self):
        return 0

    def add(self, value, accumulator):
        return accumulator + 1

    def get_result(self, accumulator):
        return accumulator

    def merge(self, a, b):
        return a + b


# -----------------------------------------------------
# Window formatter
# -----------------------------------------------------
class FormatWindowResult(WindowFunction):
    def apply(self, key, window, count, out):
        out.collect(
            Row(
                key,
                window.get_start() // 1000,
                count
            )
        )


# -----------------------------------------------------
# Main Flink Job
# -----------------------------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka source
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("wikimedia.recentchange") \
        .set_group_id("wikimedia-flink") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    raw = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    parsed = raw.map(
        lambda x: json.loads(x),
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Time.seconds(5))
        .with_timestamp_assigner(
            lambda e, ts: int(
                datetime.fromisoformat(
                    e["event_time"].replace("Z", "+00:00")
                ).timestamp() * 1000
            )
        )
    )

    timed = parsed.assign_timestamps_and_watermarks(watermark_strategy)

    aggregated = (
        timed
        .key_by(lambda e: e["wiki"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .aggregate(
            CountAggregator(),
            FormatWindowResult()
        )
    )

    # ClickHouse sink
    clickhouse_sink = JdbcSink.sink(
        """
        INSERT INTO analytics.wiki_edits (wiki, window_start, edit_count)
        VALUES (?, toDateTime(?), ?)
        """,
        Types.ROW([
            Types.STRING(),
            Types.LONG(),
            Types.LONG()
        ]),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url("jdbc:clickhouse://clickhouse:8123/analytics")
            .with_driver_name("com.clickhouse.jdbc.ClickHouseDriver")
            .build()
    )

    aggregated.add_sink(clickhouse_sink)

    env.execute("Wikimedia → Flink → ClickHouse")

