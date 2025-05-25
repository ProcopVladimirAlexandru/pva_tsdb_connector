import datetime
from pydantic import BaseModel, Field, ConfigDict

from pva_tsdb_connector.enums import TSStatusCodesEnum, MetricDefaultOrders


class NewTSMetadataModel(BaseModel):
    name: str = Field(title="Timeseries Name")
    description: str | None = Field(default=None, title="Timeseries Description")
    last_time: datetime.datetime | None = Field(default=None, title="Timeseries Last Datetime")
    last_update_time: datetime.datetime = Field(title="Timeseries Last Update Datetime")
    successful_last_update_time: datetime.datetime = Field(title="Timeseries Successful Last Update Datetime",
                                                           description="Updated on complete success: meta, datapoints, tags and metrics")
    next_update_time: datetime.datetime | None = Field(default=None, title="Timeseries Next Update Datetime")
    update_frequency: int = Field(default=24*60*60, title="Timeseries Expected Update Frequency (s)", gt=60*60)
    source_uid: str = Field(title="UID String to Identify Timeseries' Source")
    uid_from_source: str = Field(title="Timeseries String UID within the Source")
    consecutive_failed_updates: int = Field(default=0, title="Timeseries Counter for Consecutive Failed Updates", ge=0)
    status_code: TSStatusCodesEnum = Field(default=TSStatusCodesEnum.SUCCESS, title="Internal Update Status Code")
    unit: str | None = Field(default=None, title="TS Unit")

    def get_log_name(self) -> str:
        return f"[new_ts][{self.name}]"


class TSMetadataModel(NewTSMetadataModel):
    uid: int = Field(title="Timeseries Integer UID")
    last_time: datetime.datetime = Field(title="Timeseries Last Datetime")
    next_update_time: datetime.datetime = Field(title="Timeseries Next Update Datetime")

    def get_log_name(self) -> str:
        return f"[ts][{self.name}][{self.uid}]"


class NewTSDataModel(BaseModel):
    time: datetime.datetime = Field(title="Timestamp")
    value: float = Field(title="Value")


class TSDataModel(NewTSDataModel):
    uid: int = Field(title="Timeseries Integer UID")


class NewTagModel(BaseModel):
    name: str = Field(title="Tag Name")
    description: str | None = Field(default=None, title="Tag Description")
    category: str = Field(title="Tag Category")
    source_uid: str = Field(title="UID String to Identify Tag's Source")
    uid_from_source: str = Field(title="Tag Integer UID within the Source")


class TagModel(NewTagModel):
    uid: int = Field(title="Tag UID")


class TSToTagModel(BaseModel):
    ts_uid: int = Field(title="TS UID")
    tag_uid: int = Field(title="Tag UID")


class MetricModel(BaseModel):
    uid: int = Field(title="Metric UID")
    name: str = Field(title="Metric Name")
    description: str = Field(title="Metric Description")
    category: str = Field(title="Metric Name")
    unit: str = Field(title="Metric Unit")
    default_order: str = Field(title="Metric Default Order", default=MetricDefaultOrders.DESC)


class TSToMetricJSONModel(BaseModel):
    model_config = ConfigDict(extra='allow')


class TSToMetricModel(BaseModel):
    ts_uid: int = Field(title="TS UID")
    metric_uid: int = Field(title="Metric UID")
    value: float = Field(title="Metric Value")
    data_json: str | None = Field(default=None, title="Metric JSON Data")
