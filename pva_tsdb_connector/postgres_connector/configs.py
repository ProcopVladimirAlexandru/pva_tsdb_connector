import os
from pydantic import Field, StrictStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConnectionSettings(BaseSettings):
    model_config = SettingsConfigDict(
        secrets_dir=os.environ.get("SECRETS_DIR", "/run/secrets")
    )
    host: StrictStr = Field(
        title="TimescaleDB Host",
        description="TimescaleDB host without port.",
        min_length=1,
        alias="TSDB_HOST",
    )
    port: int = Field(title="TimescaleDB Port", gte=1, alias="TSDB_PORT")
    db_name: StrictStr = Field(
        title="TimescaleDB DB Name",
        description="TimescaleDB database to connect to.",
        min_length=1,
        alias="TSDB_DB_NAME",
    )
    username: StrictStr = Field(
        title="TimescaleDB User",
        description="User with access to the DB.",
        min_length=1,
        alias="TSDB_USERNAME",
    )
    password: StrictStr = Field(
        title="TimescaleDB Password",
        description="Password for user with access to the DB.",
        min_length=1,
        alias="TSDB_PASSWORD",
    )
    pool_min_size: int = Field(
        default=3,
        title="Minimum Connection Pool Size",
        gte=1,
        alias="TIMESCALE_POOL_MIN_SIZE",
    )
    pool_max_size: int = Field(
        default=35,
        title="Minimum Connection Pool Size",
        gte=1,
        alias="TIMESCALE_POOL_MAX_SIZE",
    )


class NamingSettings(BaseSettings):
    ts_table: StrictStr = Field(
        default="timeseries", title="Timeseries Table Name", min_length=1
    )
    ts_pkey: StrictStr = Field(
        default="ts_ts_uid_pkey",
        title="Timeseries Table (TS UID, time) PKEY",
        min_length=1,
    )
    ts_value_index: StrictStr = Field(
        default="ts_value_pkey", title="Timeseries Table Value Index", min_length=1
    )

    meta_ts_table: StrictStr = Field(
        default="meta_timeseries", title="Timeseries Metadata Table Name", min_length=1
    )

    tags_table: StrictStr = Field(
        default="tags", title="Timeseries Tags Table Name", min_length=1
    )
    tags_unique_constraint: StrictStr = Field(
        default="tags_unique_from_source_constraint",
        title="Timeseries Tags Unique from Source Constraint",
        min_length=1,
    )

    ts_to_tags_table: StrictStr = Field(
        default="ts_to_tags", title="Timeseries to Tags Table Name", min_length=1
    )
    ts_to_tag_tag_uid_index: StrictStr = Field(
        default="ts_to_tag_tag_uid_index",
        title="Timeseries to Tags Tag UID Index",
        min_length=1,
    )

    metrics_table: StrictStr = Field(
        default="metrics", title="Metrics Table Name", min_length=1
    )
    ts_to_metrics_table: StrictStr = Field(
        default="ts_to_metrics", title="Timeseries to Metrics Table Name", min_length=1
    )
    ts_to_metric_value_index: StrictStr = Field(
        default="ts_to_metrics_value_index",
        title="Timeseries to Metrics Value Index Name",
        min_length=1,
    )
    ts_to_metric_metric_uid_index: StrictStr = Field(
        default="ts_to_metrics_metric_index",
        title="Timeseries to Metrics Metric Index Name",
        min_length=1,
    )

    ts_time_col: StrictStr = Field(
        default="time", title="Timeseries Table Time Column Name", min_length=1
    )
    ts_value_col: StrictStr = Field(
        default="value", title="Timeseries Table Value Column Name", min_length=1
    )
    ts_uid_col: StrictStr = Field(
        default="uid", title="Timeseries Table UID Column Name", min_length=1
    )

    # META TS TABLE
    meta_ts_uid_col: StrictStr = Field(
        default="uid", title="Timeseries Meta Table UID Column Name", min_length=1
    )
    meta_ts_name_col: StrictStr = Field(
        default="name", title="Timeseries Meta Table Name Column Name", min_length=1
    )
    meta_ts_description_col: StrictStr = Field(
        default="description",
        title="Timeseries Meta Table Description Column Name",
        min_length=1,
    )
    meta_ts_last_time_col: StrictStr = Field(
        default="last_time",
        title="Timeseries Meta Table Last Time Column Name",
        min_length=1,
    )
    meta_ts_last_update_time_col: StrictStr = Field(
        default="last_update_time",
        title="Timeseries Meta Table Last Update Time Column Name",
        min_length=1,
    )
    meta_ts_successful_last_update_time_col: StrictStr = Field(
        default="successful_last_update_time",
        title="Timeseries Meta Table Successful Last Update Time Column Name",
    )
    meta_ts_next_update_time_col: StrictStr = Field(
        default="next_update_time",
        title="Timeseries Meta Table Next Update Time Column Name",
    )
    meta_ts_update_frequency_col: StrictStr = Field(
        default="update_frequency",
        title="Timeseries Meta Table Update Frequency Column Name",
    )
    meta_ts_source_uid_col: StrictStr = Field(
        default="source_uid", title="Timeseries Meta Table Source UID Column Name"
    )
    meta_ts_uid_from_source_col: StrictStr = Field(
        default="uid_from_source",
        title="Timeseries Meta Table UID from Source Column Name",
    )
    meta_ts_consecutive_failed_updates_col: StrictStr = Field(
        default="consecutive_failed_updates",
        title="Timeseries Meta Table Consecutive Failed Updates Column Name",
    )
    meta_ts_status_code_col: StrictStr = Field(
        default="status_code", title="Timeseries Meta Table Status Code Column Name"
    )
    meta_ts_unit_col: StrictStr = Field(
        default="unit", title="Timeseries Meta Table TS Unit Column Name"
    )

    tag_uid_col: StrictStr = Field(
        default="uid", title="Tags Table UID Column Name", min_length=1
    )
    tag_name_col: StrictStr = Field(
        default="name", title="Tags Table Column Name", min_length=1
    )
    tag_description_col: StrictStr = Field(
        default="description", title="Tags Table Description Name", min_length=1
    )
    tag_category_col: StrictStr = Field(
        default="category", title="Tags Table Category Column Name", min_length=1
    )
    tag_uid_from_source_col: StrictStr = Field(
        default="uid_from_source",
        title="Tags Table UID from Source Column Name",
        min_length=1,
    )
    tag_source_uid_col: StrictStr = Field(
        default="source_uid", title="Tags Table Source UID Column Name", min_length=1
    )
    tags_pkey: StrictStr = Field(
        default="tags_pkey",
        title="Tags Table Primary Key Constraint Name",
        min_length=1,
    )

    ts_to_tag_ts_uid_col: StrictStr = Field(
        default="ts_uid", title="TS to Tags Table TS UID Column Name", min_length=1
    )
    ts_to_tag_tag_uid_col: StrictStr = Field(
        default="tag_uid", title="TS to Tags Table Tag UID Column Name", min_length=1
    )
    ts_to_tag_pkey: StrictStr = Field(
        default="ts_to_tag_pkey",
        title="TS to Tags Table Primary Key Constraint Name",
        min_length=1,
    )

    metric_uid_col: StrictStr = Field(
        default="uid", title="Metrics Table UID Column Name", min_length=1
    )
    metric_name_col: StrictStr = Field(
        default="name", title="Metrics Table Column Name", min_length=1
    )
    metric_description_col: StrictStr = Field(
        default="description", title="Metrics Table Description Name", min_length=1
    )
    metric_category_col: StrictStr = Field(
        default="category", title="Metrics Table Category Column Name", min_length=1
    )
    metric_unit_col: StrictStr = Field(
        default="unit", title="Metrics Table Unit Column Name", min_length=1
    )
    metric_default_order_col: StrictStr = Field(
        default="default_order",
        title="Metrics Table Default Unit Column Name",
        min_length=1,
    )
    metrics_pkey: StrictStr = Field(
        default="metrics_pkey",
        title="Metrics Table Primary Key Constraint Name",
        min_length=1,
    )

    ts_to_metric_ts_uid_col: StrictStr = Field(
        default="ts_uid", title="TS to Metrics Table TS UID Column Name", min_length=1
    )
    ts_to_metric_metric_uid_col: StrictStr = Field(
        default="metric_uid",
        title="TS to Metrics Table Metric UID Column Name",
        min_length=1,
    )
    ts_to_metric_value_col: StrictStr = Field(
        default="value", title="TS to Metrics Table Value UID Column Name", min_length=1
    )
    ts_to_metric_data_json_col: StrictStr = Field(
        default="data_json",
        title="TS to Metrics Table JSON Data Column Name",
        min_length=1,
    )
    ts_to_metric_pkey: StrictStr = Field(
        default="ts_to_metric_pkey",
        title="TS to Metrics Table Primary Key Constraint Name",
        min_length=1,
    )


class ConnectorSettings(BaseSettings):
    connection: ConnectionSettings = Field(title="TimescaleDB Connection Settings")
    names: NamingSettings = Field(default=NamingSettings(), title="DB Entities Names")
    max_metric_value_abs: float = Field(
        default=1e50,
        title="Maximum Absolute Metric Value",
        description="Metrics with absolute values higher than this will have their absolute values set tot this",
    )
