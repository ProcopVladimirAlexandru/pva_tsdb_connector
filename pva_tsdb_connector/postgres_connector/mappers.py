from typing import Mapping

from pva_tsdb_connector.models import (
    NewTSMetadataModel,
    TSMetadataModel,
    TSDataModel,
    NewTagModel,
    TagModel,
    TSToTagModel,
    MetricModel,
    TSToMetricModel,
)
from pva_tsdb_connector.enums import TSStatusCodesEnum
from pva_tsdb_connector.postgres_connector.configs import NamingSettings


class Mapper(object):
    @staticmethod
    def ts_data_model_to_db_dict(model: TSDataModel, names: NamingSettings) -> dict:
        return {
            names.ts_uid_col: model.uid,
            names.ts_time_col: model.ts_time_col,
            names.ts_value_col: model.ts_value_col,
        }

    @staticmethod
    def db_row_to_ts_data_model(d: Mapping, names: NamingSettings) -> TSDataModel:
        return TSDataModel(
            uid=d[names.ts_uid_col],
            time=d[names.ts_time_col],
            value=d[names.ts_value_col],
        )

    @staticmethod
    def ts_metadata_model_to_db_dict(
        model: NewTSMetadataModel, names: NamingSettings
    ) -> dict:
        ret = {
            names.meta_ts_name_col: model.name,
            names.meta_ts_description_col: model.description,
            names.meta_ts_last_time_col: model.last_time,
            names.meta_ts_last_update_time_col: model.last_update_time,
            names.meta_ts_successful_last_update_time_col: model.successful_last_update_time,
            names.meta_ts_next_update_time_col: model.next_update_time,
            names.meta_ts_update_frequency_col: model.update_frequency,
            names.meta_ts_source_uid_col: model.source_uid,
            names.meta_ts_uid_from_source_col: model.uid_from_source,
            names.meta_ts_consecutive_failed_updates_col: model.consecutive_failed_updates,
            names.meta_ts_status_code_col: model.status_code,
            names.meta_ts_unit_col: model.unit,
        }
        if isinstance(model, TSMetadataModel):
            ret[names.meta_ts_uid_col] = model.uid
        return ret

    @staticmethod
    def db_row_to_metadata_model(d: Mapping, names: NamingSettings) -> TSMetadataModel:
        return TSMetadataModel(
            uid=d[names.meta_ts_uid_col],
            name=d[names.meta_ts_name_col],
            description=d[names.meta_ts_description_col],
            last_time=d[names.meta_ts_last_time_col],
            last_update_time=d[names.meta_ts_last_update_time_col],
            successful_last_update_time=d[
                names.meta_ts_successful_last_update_time_col
            ],
            next_update_time=d[names.meta_ts_next_update_time_col],
            update_frequency=d[names.meta_ts_update_frequency_col],
            source_uid=d[names.meta_ts_source_uid_col],
            uid_from_source=d[names.meta_ts_uid_from_source_col],
            consecutive_failed_updates=d[names.meta_ts_consecutive_failed_updates_col],
            status_code=TSStatusCodesEnum(d[names.meta_ts_status_code_col]),
            unit=d[names.meta_ts_unit_col],
        )

    @staticmethod
    def ts_to_tag_model_to_db_dict(model: TSToTagModel, names: NamingSettings) -> dict:
        return {
            names.ts_to_tag_ts_uid_col: model.ts_uid,
            names.ts_to_tag_tag_uid_col: model.tag_uid,
        }

    @staticmethod
    def db_row_to_ts_to_tag_model(d: Mapping, names: NamingSettings) -> TSToTagModel:
        return TSToTagModel(
            ts_uid=d[names.ts_to_tag_ts_uid_col], tag_uid=d[names.ts_to_tag_tag_uid_col]
        )

    @staticmethod
    def tag_model_to_db_dict(model: NewTagModel, names: NamingSettings) -> dict:
        ret = {
            names.tag_name_col: model.name,
            names.tag_description_col: model.description,
            names.tag_category_col: model.category,
            names.tag_source_uid_col: model.source_uid,
            names.tag_uid_from_source_col: model.uid_from_source,
        }
        if isinstance(model, TagModel):
            ret[names.tag_uid_col] = model.uid
        return ret

    @staticmethod
    def db_row_to_tag_model(d: Mapping, names: NamingSettings) -> TagModel:
        return TagModel(
            uid=d[names.tag_uid_col],
            name=d[names.tag_name_col],
            description=d[names.tag_description_col],
            category=d[names.tag_category_col],
            source_uid=d[names.tag_source_uid_col],
            uid_from_source=d[names.tag_uid_from_source_col],
        )

    @staticmethod
    def db_row_to_metric_model(d: Mapping, names: NamingSettings) -> MetricModel:
        return MetricModel(
            uid=d[names.metric_uid_col],
            name=d[names.metric_name_col],
            description=d[names.metric_description_col],
            category=d[names.metric_category_col],
            default_order=d[names.metric_default_order_col],
            unit=d[names.metric_unit_col],
        )

    @staticmethod
    def metric_model_to_db_dict(model: MetricModel, names: NamingSettings) -> dict:
        return {
            names.metric_uid_col: model.uid,
            names.metric_name_col: model.name,
            names.metric_description_col: model.description,
            names.metric_category_col: model.category,
            names.metric_unit_col: model.unit,
            names.metric_default_order_col: model.default_order,
        }

    @staticmethod
    def db_row_to_ts_to_metric_model(
        d: Mapping, names: NamingSettings, max_abs: float | None
    ) -> TSToMetricModel:
        if max_abs <= 0:
            raise ValueError("max_abs must be a positive number")
        value: float = d[names.ts_to_metric_value_col]
        if max_abs is not None:
            value = min(value, max_abs) if value > 0 else max(value, -max_abs)

        return TSToMetricModel(
            ts_uid=d[names.ts_to_metric_ts_uid_col],
            metric_uid=d[names.ts_to_metric_metric_uid_col],
            value=value,
            data_json=d[names.ts_to_metric_data_json_col],
        )

    @staticmethod
    def ts_to_metric_model_to_db_dict(
        model: TSToMetricModel, names: NamingSettings
    ) -> dict:
        return {
            names.ts_to_metric_ts_uid_col: model.ts_uid,
            names.ts_to_metric_metric_uid_col: model.metric_uid,
            names.ts_to_metric_value_col: model.value,
            names.ts_to_metric_data_json_col: model.data_json,
        }
