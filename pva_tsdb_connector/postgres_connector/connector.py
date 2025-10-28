import structlog
import datetime
from typing import Any
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqlalchemy import (
    URL,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Text,
    DateTime,
    DOUBLE_PRECISION,
    CheckConstraint,
    UniqueConstraint,
    ForeignKey,
    PrimaryKeyConstraint,
    Index,
    select,
    func,
    text,
)
from sqlalchemy import delete, update, distinct, and_
from sqlalchemy.dialects.postgresql import insert, JSONB, aggregate_order_by
from pgvector.sqlalchemy import Vector  # type: ignore
from sqlalchemy.ext.asyncio.engine import AsyncEngine

from pva_tsdb_connector.exceptions import TSDBException
from pva_tsdb_connector.postgres_connector.configs import ConnectorSettings
from pva_tsdb_connector.models import (
    NewTSMetadataModel,
    TSMetadataModel,
    TSDataModel,
    NewTagModel,
    TagModel,
    TSToTagModel,
    MetricModel,
    TSToMetricModel,
    TSWithTagsAndDataModel,
    NewMetricValueModel,
    TSWithValues,
    MetricValueWithOperands,
    TSWithVisualizationVectorModel,
)
from pva_tsdb_connector.postgres_connector.mappers import Mapper
from pva_tsdb_connector.enums import AllOrAnyTags


class AsyncPostgresSQLAlchemyCoreConnector:
    def __init__(self, config: ConnectorSettings):
        self._config = config
        self._logger = structlog.get_logger(component=self.__class__.__name__)
        self._dialect = "postgresql"
        self._driver = "asyncpg"
        self._url = URL.create(
            f"{self._dialect}+{self._driver}",
            username=self._config.connection.username,
            password=self._config.connection.password,
            host=self._config.connection.host,
            port=self._config.connection.port,
            database=self._config.connection.db_name,
        )
        self._metadata = MetaData()

    async def connect(self) -> None:
        try:
            self._engine: AsyncEngine = create_async_engine(
                self._url,
                echo=True,
                echo_pool=False,
                pool_size=self._config.connection.pool_min_size,
                max_overflow=self._config.connection.pool_max_size
                - self._config.connection.pool_min_size,
                query_cache_size=100,
            )
        except Exception as ex:
            err_msg = f"Failed to create SQLA engine to PostgreSQL. Error: {ex}."
            self._logger.exception(err_msg)
            raise TSDBException(err_msg) from Exception
        self._logger.info("Initialized Connector")

        try:
            async with self._engine.begin() as conn:
                await conn.run_sync(self._metadata.reflect)
        except Exception as ex:
            err_msg = f"Failed to reflect DB. Error: {ex}"
            self._logger.exception(err_msg)
            raise TSDBException(err_msg) from Exception
        self._logger.info("Reflected DB")

    async def init_db(self, metric_models: list[MetricModel]):
        async with self._engine.begin() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            await conn.commit()
        await self.get_metadata_table()
        await self.get_ts_table()
        await self.get_tags_table()
        await self.get_ts_to_tags_table()
        await self.get_metrics_table()
        await self.get_metric_values_table()
        await self.get_metric_value_operands_table()
        await self.get_ts_visualization_vectors_table()

        async with self._engine.begin() as conn:
            await conn.run_sync(self._metadata.create_all)
        await self.upsert_metrics(metric_models)

    async def get_connection(self):
        return await self._engine.connect()

    async def get_metadata_table(self) -> Table:
        table = self._metadata.tables.get(self._config.names.meta_ts_table, None)
        if table is not None:
            return table
        table = Table(
            self._config.names.meta_ts_table,
            self._metadata,
            Column(self._config.names.meta_ts_uid_col, Integer, primary_key=True),
            Column(self._config.names.meta_ts_name_col, String(2048), nullable=False),
            Column(self._config.names.meta_ts_description_col, Text, nullable=True),
            Column(
                self._config.names.meta_ts_last_time_col,
                DateTime(timezone=True),
                nullable=False,
            ),
            Column(
                self._config.names.meta_ts_last_update_time_col,
                DateTime(timezone=True),
                nullable=False,
            ),
            Column(
                self._config.names.meta_ts_successful_last_update_time_col,
                DateTime(timezone=True),
                nullable=False,
            ),
            Column(
                self._config.names.meta_ts_next_update_time_col,
                DateTime(timezone=True),
                nullable=True,
            ),
            Column(
                self._config.names.meta_ts_update_frequency_col,
                Integer,
                CheckConstraint(
                    f"{self._config.names.meta_ts_update_frequency_col} > 0"
                ),
                nullable=True,
            ),
            Column(
                self._config.names.meta_ts_source_uid_col, String(2048), nullable=False
            ),
            Column(
                self._config.names.meta_ts_uid_from_source_col,
                String(2048),
                nullable=False,
            ),
            Column(
                self._config.names.meta_ts_consecutive_failed_updates_col,
                Integer,
                CheckConstraint(
                    f"{self._config.names.meta_ts_consecutive_failed_updates_col} >= 0"
                ),
                nullable=False,
                default=0,
            ),
            Column(self._config.names.meta_ts_status_code_col, Integer, nullable=False),
            Column(self._config.names.meta_ts_unit_col, String(128), nullable=True),
            UniqueConstraint(
                self._config.names.meta_ts_source_uid_col,
                self._config.names.meta_ts_uid_from_source_col,
            ),
        )
        return table

    async def get_ts_table(self) -> Table:
        table = self._metadata.tables.get(self._config.names.ts_table, None)
        if table is not None:
            return table
        table = Table(
            self._config.names.ts_table,
            self._metadata,
            Column(
                self._config.names.ts_time_col, DateTime(timezone=True), nullable=False
            ),
            Column(self._config.names.ts_value_col, DOUBLE_PRECISION(), nullable=True),
            Column(
                self._config.names.ts_uid_col,
                Integer,
                ForeignKey(
                    f"{self._config.names.meta_ts_table}.{self._config.names.meta_ts_uid_col}"
                ),
                nullable=False,
            ),
            PrimaryKeyConstraint(
                self._config.names.ts_uid_col,
                self._config.names.ts_time_col,
                name=self._config.names.ts_pkey,
            ),
        )
        Index(
            self._config.names.ts_value_index, table.c[self._config.names.ts_value_col]
        )
        return table

    async def get_tags_table(self):
        table = self._metadata.tables.get(self._config.names.tags_table, None)
        if table is not None:
            return table
        table = Table(
            self._config.names.tags_table,
            self._metadata,
            Column(self._config.names.tag_uid_col, Integer),
            Column(self._config.names.tag_name_col, String(2048), nullable=False),
            Column(self._config.names.tag_description_col, Text, nullable=True),
            Column(
                self._config.names.tag_category_col,
                String(2048),
                nullable=False,
                default="Others",
            ),
            Column(
                self._config.names.tag_uid_from_source_col, String(2048), nullable=False
            ),
            Column(self._config.names.tag_source_uid_col, String(2048), nullable=False),
            PrimaryKeyConstraint(
                self._config.names.tag_uid_col, name=self._config.names.tags_pkey
            ),
            UniqueConstraint(
                self._config.names.tag_uid_from_source_col,
                self._config.names.tag_source_uid_col,
                name=self._config.names.tags_unique_constraint,
            ),
        )
        return table

    async def get_ts_to_tags_table(self):
        table = self._metadata.tables.get(self._config.names.ts_to_tags_table, None)
        if table is not None:
            return table
        table = Table(
            self._config.names.ts_to_tags_table,
            self._metadata,
            Column(
                self._config.names.ts_to_tag_ts_uid_col,
                Integer,
                ForeignKey(
                    f"{self._config.names.meta_ts_table}.{self._config.names.meta_ts_uid_col}",
                    name=f"{self._config.names.ts_to_tag_ts_uid_col}_fkey",
                ),
            ),
            Column(
                self._config.names.ts_to_tag_tag_uid_col,
                Integer,
                ForeignKey(
                    f"{self._config.names.tags_table}.{self._config.names.tag_uid_col}",
                    name=f"{self._config.names.ts_to_tag_tag_uid_col}_fkey",
                ),
            ),
            PrimaryKeyConstraint(
                self._config.names.ts_to_tag_ts_uid_col,
                self._config.names.ts_to_tag_tag_uid_col,
                name=self._config.names.ts_to_tag_pkey,
            ),
        )
        Index(
            self._config.names.ts_to_tag_tag_uid_index,
            table.c[self._config.names.ts_to_tag_tag_uid_col],
        )
        return table

    async def get_metrics_table(self):
        table = self._metadata.tables.get(self._config.names.metrics_table, None)
        if table is not None:
            return table
        table = Table(
            self._config.names.metrics_table,
            self._metadata,
            Column(
                self._config.names.metric_uid_col,
                Integer,
                CheckConstraint(f"{self._config.names.metric_uid_col} > 0"),
                autoincrement=False,
            ),
            Column(self._config.names.metric_name_col, String(2048), nullable=False),
            Column(self._config.names.metric_description_col, Text, nullable=True),
            Column(
                self._config.names.metric_category_col,
                String(2048),
                nullable=False,
                default="Others",
            ),
            Column(self._config.names.metric_unit_col, String(32), nullable=True),
            Column(
                self._config.names.metric_default_order_col, String(8), nullable=True
            ),
            PrimaryKeyConstraint(
                self._config.names.metric_uid_col, name=self._config.names.metrics_pkey
            ),
        )
        return table

    async def get_metric_values_table(self):
        table = self._metadata.tables.get(self._config.names.metric_values_table, None)
        if table is not None:
            return table
        table = Table(
            self._config.names.metric_values_table,
            self._metadata,
            Column(self._config.names.metric_values_uid_col, Integer),
            Column(
                self._config.names.metric_values_metric_uid_col,
                Integer,
                ForeignKey(
                    f"{self._config.names.metrics_table}.{self._config.names.metric_uid_col}",
                    name=f"{self._config.names.metric_values_metric_uid_col}_fkey",
                ),
            ),
            Column(
                self._config.names.metric_values_value_col,
                DOUBLE_PRECISION(),
                nullable=True,
            ),
            Column(
                self._config.names.metric_values_data_json_col, JSONB, nullable=True
            ),
            PrimaryKeyConstraint(
                self._config.names.metric_values_uid_col,
                name=self._config.names.metric_values_pkey,
            ),
        )
        Index(
            self._config.names.metric_values_value_index,
            table.c[self._config.names.metric_values_value_col],
        )
        Index(
            self._config.names.metric_values_metric_uid_index,
            table.c[self._config.names.metric_values_metric_uid_col],
        )
        return table

    async def get_metric_value_operands_table(self):
        table = self._metadata.tables.get(
            self._config.names.metric_value_operands_table, None
        )
        if table is not None:
            return table
        table = Table(
            self._config.names.metric_value_operands_table,
            self._metadata,
            Column(
                self._config.names.metric_value_operands_metric_value_uid_col,
                Integer,
                ForeignKey(
                    f"{self._config.names.metric_values_table}.{self._config.names.metric_values_uid_col}",
                    name=f"{self._config.names.metric_value_operands_metric_value_uid_col}_fkey",
                    ondelete="CASCADE",
                ),
            ),
            Column(
                self._config.names.metric_value_operands_ts_uid_col,
                Integer,
                ForeignKey(
                    f"{self._config.names.meta_ts_table}.{self._config.names.meta_ts_uid_col}",
                    name=f"{self._config.names.metric_value_operands_ts_uid_col}_fkey",
                    ondelete="CASCADE",
                ),
            ),
        )
        Index(
            self._config.names.metric_value_operands_metric_value_uid_index,
            table.c[self._config.names.metric_value_operands_metric_value_uid_col],
        )
        Index(
            self._config.names.metric_value_operands_ts_uid_index,
            table.c[self._config.names.metric_value_operands_ts_uid_col],
        )
        return table

    async def get_ts_visualization_vectors_table(self):
        table = self._metadata.tables.get(
            self._config.names.ts_visualization_vectors_table, None
        )
        if table is not None:
            return table
        table = (
            Table(
                self._config.names.ts_visualization_vectors_table,
                self._metadata,
                Column(
                    self._config.names.ts_visualization_vectors_ts_uid_col,
                    Integer,
                    ForeignKey(
                        f"{self._config.names.meta_ts_table}.{self._config.names.meta_ts_uid_col}",
                        name=f"{self._config.names.ts_visualization_vectors_ts_uid_col}_fkey",
                    ),
                ),
                Column(
                    self._config.names.ts_visualization_vectors_vector_col, Vector(2)
                ),
                PrimaryKeyConstraint(
                    self._config.names.ts_visualization_vectors_ts_uid_col,
                    name=self._config.names.ts_visualization_vectors_pkey,
                ),
            ),
        )
        return table

    async def upsert_metrics(self, metric_models: list[MetricModel]):
        table = await self.get_metrics_table()
        stmt = (
            insert(table)
            .values(
                [
                    Mapper.metric_model_to_db_dict(m, self._config.names)
                    for m in metric_models
                ]
            )
            .on_conflict_do_nothing(self._config.names.metrics_pkey)
        )
        async with self._engine.connect() as conn:
            await conn.execute(stmt)
            await conn.commit()

    async def get_metrics(
        self, conn: AsyncConnection, uids: list[int] | None = None
    ) -> list[MetricModel]:
        table = await self.get_metrics_table()
        stmt = select(table)
        if uids is not None:
            if len(uids) == 0:
                return []
            stmt = stmt.where(table.c[self._config.names.metric_uid_col].in_(uids))
        results = (await conn.execute(stmt)).mappings().fetchall()
        return [
            Mapper.db_row_to_metric_model(d, names=self._config.names) for d in results
        ]

    async def upsert_tags(
        self, conn: AsyncConnection, tag_models: list[NewTagModel]
    ) -> list[TagModel]:
        db_dicts = [
            Mapper.tag_model_to_db_dict(model, names=self._config.names)
            for model in tag_models
        ]
        table = await self.get_tags_table()
        res = await conn.execute(
            insert(table)
            .values(db_dicts)
            .on_conflict_do_nothing(self._config.names.tags_unique_constraint)
            .returning(table.c[self._config.names.tag_uid_col])
        )
        results: list[TagModel] = list()
        i = 0
        for r in res:
            results.append(TagModel(uid=r[0], **(tag_models[i].model_dump())))
            i += 1
        return results

    async def get_tags(
        self,
        conn: AsyncConnection,
        uids: list[int] | None = None,
        uids_from_source: list[str] | None = None,
    ) -> list[TagModel]:
        table = await self.get_tags_table()
        stmt = select(table)
        if uids is not None:
            if len(uids) == 0:
                return []
            stmt = stmt.where(table.c[self._config.names.tag_uid_col].in_(uids))

        if uids_from_source is not None:
            if len(uids_from_source) == 0:
                return []
            stmt = stmt.where(
                table.c[self._config.names.tag_uid_from_source_col].in_(
                    uids_from_source
                )
            )
        results = (await conn.execute(stmt)).mappings().fetchall()
        return [
            Mapper.db_row_to_tag_model(d, names=self._config.names) for d in results
        ]

    async def get_ts_to_tags(
        self, conn: AsyncConnection, ts_uids: list[int] | None = None
    ) -> list[TSToTagModel]:
        table = await self.get_ts_to_tags_table()
        stmt = select(table)
        if ts_uids is not None:
            if len(ts_uids) == 0:
                return []
            stmt = stmt.where(
                table.c[self._config.names.ts_to_tag_ts_uid_col].in_(ts_uids)
            )
        results = (await conn.execute(stmt)).mappings().fetchall()
        return [
            Mapper.db_row_to_ts_to_tag_model(d, names=self._config.names)
            for d in results
        ]

    async def insert_ts_to_tags(
        self, conn: AsyncConnection, ts_to_tag_models: list[TSToTagModel]
    ):
        table = await self.get_ts_to_tags_table()
        db_dicts = [
            Mapper.ts_to_tag_model_to_db_dict(model, names=self._config.names)
            for model in ts_to_tag_models
        ]
        return await conn.execute(
            insert(table)
            .values(db_dicts)
            .on_conflict_do_nothing(self._config.names.ts_to_tag_pkey)
        )

    async def get_metadata(
        self,
        conn: AsyncConnection,
        uids: list[int] | None = None,
        data_source_uid: str | None = None,
        order_by: str | None = None,
        order_asc: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[TSMetadataModel]:
        table = await self.get_metadata_table()
        stmt = select(table)
        if uids is not None:
            if len(uids) == 0:
                return []
            stmt = stmt.where(table.c[self._config.names.metric_uid_col].in_(uids))
        if data_source_uid is not None:
            stmt = stmt.where(
                table.c[self._config.names.meta_ts_source_uid_col] == data_source_uid
            )
        if order_by is not None:
            order_by_column = table.c[order_by]
            if order_asc:
                order_by_column_ordered = order_by_column.asc()
            else:
                order_by_column_ordered = order_by_column.desc()
            stmt = stmt.order_by(order_by_column_ordered)
        if limit is not None:
            stmt = stmt.limit(limit)
        if offset is not None:
            stmt = stmt.offset(offset)

        results = (await conn.execute(stmt)).mappings().fetchall()
        return [
            Mapper.db_row_to_metadata_model(row, names=self._config.names)
            for row in results
        ]

    async def get_ts_to_metrics(
        self,
        conn: AsyncConnection,
        ts_uids: list[int] | None = None,
        metric_uids: list[int] | None = None,
    ) -> list[TSToMetricModel]:
        metric_values_table = await self.get_metric_values_table()
        operands_table = await self.get_metric_value_operands_table()

        stmt = (
            select(
                metric_values_table.c[self._config.names.metric_values_uid_col],
                metric_values_table.c[self._config.names.metric_values_metric_uid_col],
                metric_values_table.c[self._config.names.metric_values_value_col],
                metric_values_table.c[self._config.names.metric_values_data_json_col],
                func.array_agg(
                    operands_table.columns[
                        self._config.names.metric_value_operands_ts_uid_col
                    ]
                ).label("ts_uids"),
            )
            .join(
                operands_table,
                metric_values_table.c[self._config.names.metric_values_uid_col]
                == operands_table.c[
                    self._config.names.metric_value_operands_metric_value_uid_col
                ],
            )
            .group_by(metric_values_table.c[self._config.names.metric_values_uid_col])
        )

        if metric_uids is not None:
            if len(metric_uids) == 0:
                return list()
            stmt = stmt.where(
                metric_values_table.c[
                    self._config.names.metric_values_metric_uid_col
                ].in_(metric_uids)
            )

        if ts_uids is not None:
            if len(ts_uids) == 0:
                return list()
            stmt = stmt.where(
                metric_values_table.c[self._config.names.metric_values_uid_col].in_(
                    select(
                        operands_table.c[
                            self._config.names.metric_value_operands_metric_value_uid_col
                        ]
                    ).where(
                        operands_table.c[
                            self._config.names.metric_value_operands_ts_uid_col
                        ].in_(ts_uids)
                    )
                )
            )
        results = (await conn.execute(stmt)).mappings().fetchall()
        return [
            Mapper.db_row_to_ts_to_metric_model(
                d, names=self._config.names, max_abs=self._config.max_metric_value_abs
            )
            for d in results
        ]

    async def insert_ts_to_metrics(
        self, conn: AsyncConnection, ts_to_metrics_models: list[TSToMetricModel]
    ):
        if len(ts_to_metrics_models) == 0:
            return
        metric_values_table = await self.get_metric_values_table()
        operands_table = await self.get_metric_value_operands_table()

        page_size: int = 1024
        inserted: int = 0
        while inserted < len(ts_to_metrics_models):
            ts_to_metrics_models_page: list[TSToMetricModel] = ts_to_metrics_models[
                inserted : inserted + page_size
            ]
            models_page: list[NewMetricValueModel] = [
                NewMetricValueModel.from_ts_to_metric_model(m)
                for m in ts_to_metrics_models_page
            ]

            result = (
                (
                    await conn.execute(
                        insert(metric_values_table)
                        .values(
                            [
                                Mapper.new_metric_value_model_to_db_dict(
                                    m, self._config.names
                                )
                                for m in models_page
                            ]
                        )
                        .returning(
                            metric_values_table.c[
                                self._config.names.metric_values_uid_col
                            ]
                        )
                    )
                )
                .mappings()
                .fetchall()
            )

            if len(result) != len(ts_to_metrics_models_page):
                err_msg = "Result length mismatch between inserted rows result and ts_to_metrics_models_page."
                self._logger.error(err_msg, exc_info=True)
                raise TSDBException(err_msg) from Exception

            operands_rows: list[dict] = list()
            for i, row in enumerate(result):
                metric_value_uid = row["uid"]
                ts_to_metric_model = ts_to_metrics_models_page[i]
                for operand_uid in ts_to_metric_model.ts_uids:
                    operands_rows.append(
                        {
                            self._config.names.metric_value_operands_metric_value_uid_col: metric_value_uid,
                            self._config.names.metric_value_operands_ts_uid_col: operand_uid,
                        }
                    )

            await conn.execute(insert(operands_table).values(operands_rows))

            inserted += len(result)

    async def delete_metric_values(
        self,
        conn: AsyncConnection,
        ts_uids: list[int],
        metric_ids: list[int],
        operands_count: int,
    ) -> None:
        if len(metric_ids) == 0:
            return

        metric_values_table = await self.get_metric_values_table()
        operands_table = await self.get_metric_value_operands_table()
        min_aggr = func.min(
            operands_table.c[self._config.names.metric_value_operands_ts_uid_col]
        )
        count_aggr = func.count(
            operands_table.c[self._config.names.metric_value_operands_ts_uid_col]
        )

        stmt = delete(metric_values_table).where(
            and_(
                metric_values_table.c[
                    self._config.names.metric_values_metric_uid_col
                ].in_(metric_ids),
                metric_values_table.c[self._config.names.metric_values_uid_col].in_(
                    select(
                        operands_table.c[
                            self._config.names.metric_value_operands_metric_value_uid_col
                        ]
                    )
                    .group_by(
                        operands_table.c[
                            self._config.names.metric_value_operands_metric_value_uid_col
                        ]
                    )
                    .having(and_(min_aggr.in_(ts_uids), count_aggr == operands_count))
                ),
            )
        )
        await conn.execute(stmt)

    async def create_ts_metadata(
        self, conn: AsyncConnection, ts_metadata_model: NewTSMetadataModel
    ) -> TSMetadataModel:
        db_dict = Mapper.ts_metadata_model_to_db_dict(
            ts_metadata_model, names=self._config.names
        )
        table = await self.get_metadata_table()
        res = (
            await conn.execute(insert(table).values([db_dict]).returning(table.c.uid))
        ).fetchall()
        return TSMetadataModel(uid=res[0][0], **(ts_metadata_model.model_dump()))

    async def update_ts_metadata(
        self, conn: AsyncConnection, ts_metadata_model: TSMetadataModel
    ) -> None:
        db_dict = Mapper.ts_metadata_model_to_db_dict(
            ts_metadata_model, names=self._config.names
        )
        table = await self.get_metadata_table()
        await conn.execute(
            update(table)
            .where(table.c[self._config.names.meta_ts_uid_col] == ts_metadata_model.uid)
            .values(
                {
                    k: v
                    for k, v in db_dict.items()
                    if k != self._config.names.meta_ts_uid_col
                }
            )
        )

    async def update_ts_metadata_col(
        self, conn: AsyncConnection, col_name: str, uids: list[int], value: Any
    ) -> None:
        table = await self.get_metadata_table()
        await conn.execute(
            update(table)
            .where(table.c[self._config.names.meta_ts_uid_col].in_(uids))
            .values({table.c[col_name]: value})
        )

    async def get_timeseries(
        self,
        conn: AsyncConnection,
        ts_uids: list[int] | None = None,
        order_asc: bool = True,
        start_date: datetime.datetime | None = None,
    ) -> list[TSDataModel]:
        table = await self.get_ts_table()
        stmt = select(table)
        if ts_uids is not None:
            if len(ts_uids) == 0:
                return []
            stmt = stmt.where(table.c[self._config.names.ts_uid_col].in_(ts_uids))
        if start_date is not None:
            stmt = stmt.where(table.c[self._config.names.ts_time_col] >= start_date)
        order_by_column = table.columns[self._config.names.ts_time_col]
        if order_asc:
            order_by_column_ordered = order_by_column.asc()
        else:
            order_by_column_ordered = order_by_column.desc()
        stmt = stmt.order_by(order_by_column_ordered)
        results = (await conn.execute(stmt)).mappings().fetchall()
        return [
            Mapper.db_row_to_ts_data_model(row, names=self._config.names)
            for row in results
        ]

    async def append_to_ts(
        self, conn: AsyncConnection, models: list[TSDataModel]
    ) -> None:
        if len(models) == 0:
            return
        table = await self.get_ts_table()
        page_size: int = 1024
        inserted: int = 0
        while inserted < len(models):
            models_page: list[TSDataModel] = models[inserted : inserted + page_size]
            result = await conn.execute(
                insert(table)
                .values(
                    [
                        Mapper.ts_data_model_to_db_dict(m, self._config.names)
                        for m in models_page
                    ]
                )
                .on_conflict_do_nothing(self._config.names.ts_pkey)
            )
            inserted += result.rowcount

    async def get_last_ts_times(
        self, conn: AsyncConnection, ts_uids: list[int]
    ) -> dict[str, datetime.datetime]:
        if len(ts_uids) == 0:
            return dict()
        table = await self.get_ts_table()
        ts_uid_col = table.c[self._config.names.ts_uid_col]
        ts_time_col = self._config.names.ts_time_col
        stmt = (
            select(ts_uid_col, func.max(ts_time_col))
            .where(ts_uid_col.in_(ts_uids))
            .group_by(ts_uid_col)
        )
        result = (await conn.execute(stmt)).mappings().fetchall()
        return {r[self._config.names.ts_uid_col]: r["max"] for r in result}

    async def _get_tags_subquery(
        self,
        tag_uids: list[int] | None,
        all_or_any_tags: AllOrAnyTags,
    ):
        if tag_uids is None or len(tag_uids) == 0:
            return None

        ts_to_tags_table = await self.get_ts_to_tags_table()
        if all_or_any_tags == AllOrAnyTags.ANY:
            return select(
                distinct(ts_to_tags_table.c[self._config.names.ts_to_tag_ts_uid_col])
            ).where(
                ts_to_tags_table.c[self._config.names.ts_to_tag_tag_uid_col].in_(
                    tag_uids
                )
            )
        else:
            return (
                select(
                    distinct(
                        ts_to_tags_table.c[self._config.names.ts_to_tag_ts_uid_col]
                    )
                )
                .where(
                    ts_to_tags_table.c[self._config.names.ts_to_tag_tag_uid_col].in_(
                        tag_uids
                    )
                )
                .group_by(ts_to_tags_table.c[self._config.names.ts_to_tag_ts_uid_col])
                .having(
                    func.count(
                        distinct(
                            ts_to_tags_table.c[self._config.names.ts_to_tag_tag_uid_col]
                        )
                    )
                    == len(tag_uids)
                )
            )

    async def get_ordered_values_and_operands(
        self,
        conn: AsyncConnection,
        order_by_metric_uid: int,
        order_asc: bool,
        all_or_any_tags: AllOrAnyTags,
        limit: int,
        offset: int,
        tag_uids: list[int] | None,
    ) -> list[MetricValueWithOperands]:
        if order_by_metric_uid in [16, 17, 18]:
            limit *= 2
            offset *= 2
        if limit == 0:
            return list()

        meta_ts_table = await self.get_metadata_table()
        metric_values_table = await self.get_metric_values_table()
        operands_table = await self.get_metric_value_operands_table()
        order_by_col = metric_values_table.c[self._config.names.metric_values_value_col]
        if order_asc:
            order_by_col = order_by_col.asc()
        else:
            order_by_col = order_by_col.desc()

        tags_subquery = await self._get_tags_subquery(
            tag_uids=tag_uids, all_or_any_tags=all_or_any_tags
        )
        metric_value_where_clauses = [
            metric_values_table.c[self._config.names.metric_values_metric_uid_col]
            == order_by_metric_uid
        ]
        if tags_subquery is not None:
            metric_value_where_clauses.append(
                operands_table.c[
                    self._config.names.metric_value_operands_ts_uid_col
                ].in_(tags_subquery)
            )

        stmt = (
            select(
                meta_ts_table,
                metric_values_table.c[
                    self._config.names.metric_values_metric_uid_col
                ].label("order_by_metric_uid"),
                metric_values_table.c[self._config.names.metric_values_value_col].label(
                    "order_by_metric_value"
                ),
            )
            .join(
                operands_table,
                meta_ts_table.c[self._config.names.meta_ts_uid_col]
                == operands_table.c[
                    self._config.names.metric_value_operands_ts_uid_col
                ],
            )
            .join(
                metric_values_table,
                metric_values_table.c[self._config.names.metric_values_uid_col]
                == operands_table.c[
                    self._config.names.metric_value_operands_metric_value_uid_col
                ],
            )
            .where(
                metric_values_table.c[self._config.names.metric_values_uid_col].in_(
                    select(
                        metric_values_table.c[self._config.names.metric_values_uid_col]
                    )
                    .join(
                        operands_table,
                        metric_values_table.c[self._config.names.metric_values_uid_col]
                        == operands_table.c[
                            self._config.names.metric_value_operands_metric_value_uid_col
                        ],
                    )
                    .where(and_(*metric_value_where_clauses))
                )
            )
            .order_by(order_by_col)
            .order_by(
                metric_values_table.c[self._config.names.metric_values_uid_col].asc()
            )
            .order_by(
                operands_table.c[
                    self._config.names.metric_value_operands_ts_uid_col
                ].asc()
            )
            .offset(offset)
            .limit(limit)
        )
        res = (await conn.execute(stmt)).mappings().fetchall()

        chartop: list[MetricValueWithOperands] = list()
        increment: int = 1
        if order_by_metric_uid in [16, 17, 18]:
            increment = 2
        i: int = 0
        while i < len(res):
            operand_rows = res[i : i + increment]
            chartop.append(
                MetricValueWithOperands(
                    operands=[
                        Mapper.db_row_to_metadata_model(row, names=self._config.names)
                        for row in operand_rows
                    ],
                    metric_uid=operand_rows[0]["order_by_metric_uid"],
                    metric_value=operand_rows[0]["order_by_metric_value"],
                )
            )
            i += increment
        return chartop

    async def get_ts_with_tags_and_data(
        self, t0: datetime.datetime, offset: int, limit: int
    ) -> list[TSWithTagsAndDataModel]:
        meta_ts_table = await self.get_metadata_table()
        ts_to_tags_table = await self.get_ts_to_tags_table()
        tags_subquery = (
            select(
                meta_ts_table.columns[self._config.names.meta_ts_uid_col].label("uid"),
                func.array_agg(
                    ts_to_tags_table.columns[self._config.names.ts_to_tag_tag_uid_col]
                ).label("tag_uids"),
            )
            .join(
                ts_to_tags_table,
                meta_ts_table.columns[self._config.names.meta_ts_uid_col]
                == ts_to_tags_table.columns[self._config.names.ts_to_tag_ts_uid_col],
            )
            .group_by(meta_ts_table.columns[self._config.names.meta_ts_uid_col])
            .alias("tags_subquery")
        )

        ts_table = await self.get_ts_table()
        values_subquery = (
            select(
                meta_ts_table.columns[self._config.names.meta_ts_uid_col].label("uid"),
                func.array_agg(
                    aggregate_order_by(
                        ts_table.columns[self._config.names.ts_time_col],
                        ts_table.columns[self._config.names.ts_time_col],
                    )
                ).label("times"),
                func.array_agg(
                    aggregate_order_by(
                        ts_table.columns[self._config.names.ts_value_col],
                        ts_table.columns[self._config.names.ts_time_col],
                    )
                ).label("values"),
            )
            .join(
                ts_table,
                meta_ts_table.columns[self._config.names.meta_ts_uid_col]
                == ts_table.columns[self._config.names.ts_uid_col],
            )
            .where(ts_table.columns[self._config.names.ts_time_col] > t0)
            .group_by(meta_ts_table.columns[self._config.names.meta_ts_uid_col])
            .alias("values_subquery")
        )

        stmt = (
            select(
                tags_subquery.c["uid"],
                tags_subquery.c["tag_uids"],
                values_subquery.c["times"],
                values_subquery.c["values"],
            )
            .join(values_subquery, tags_subquery.c.uid == values_subquery.c.uid)
            .order_by(tags_subquery.c["uid"].asc())
            .offset(offset)
            .limit(limit)
        )

        async with self._engine.connect() as conn:
            results = (await conn.execute(stmt)).mappings().fetchall()
        return [Mapper.db_row_to_ts_with_tags_and_data_model(d) for d in results]

    async def get_ts_with_values_metric_values(
        self, metric_ids: list[int]
    ) -> list[TSWithValues]:
        meta_ts_table = await self.get_metadata_table()
        metric_values_table = await self.get_metric_values_table()
        operands_table = await self.get_metric_value_operands_table()
        values_array_agg = func.array_agg(
            aggregate_order_by(
                metric_values_table.c[self._config.names.metric_values_value_col],
                metric_values_table.c[
                    self._config.names.metric_values_metric_uid_col
                ].asc(),
            )
        )
        stmt = (
            select(
                meta_ts_table.c[self._config.names.meta_ts_uid_col].label("ts_uid"),
                values_array_agg.label("values"),
            )
            .join(
                operands_table,
                meta_ts_table.c[self._config.names.meta_ts_uid_col]
                == operands_table.c[
                    self._config.names.metric_value_operands_ts_uid_col
                ],
            )
            .join(
                metric_values_table,
                operands_table.c[
                    self._config.names.metric_value_operands_metric_value_uid_col
                ]
                == metric_values_table.c[self._config.names.metric_values_uid_col],
            )
            .where(
                metric_values_table.c[
                    self._config.names.metric_values_metric_uid_col
                ].in_(metric_ids)
            )
            .group_by(meta_ts_table.columns[self._config.names.meta_ts_uid_col])
            .having(func.array_length(values_array_agg, 1) == len(metric_ids))
        )
        async with self._engine.connect() as conn:
            results = (await conn.execute(stmt)).mappings().fetchall()
        return [Mapper.db_row_to_ts_with_values(d) for d in results]

    async def delete_ts_visualization_vectors(self, conn: AsyncConnection):
        ts_visualization_vectors_table = await self.get_ts_visualization_vectors_table()
        stmt = delete(ts_visualization_vectors_table)
        await conn.execute(stmt)

    async def insert_ts_visualization_vectors(
        self, conn: AsyncConnection, models: list[TSWithValues]
    ):
        ts_visualization_vectors_table = await self.get_ts_visualization_vectors_table()
        page_size: int = 1024
        inserted: int = 0
        while inserted < len(models):
            models_page = models[inserted : inserted + page_size]
            stmt = insert(ts_visualization_vectors_table).values(
                [
                    Mapper.ts_with_values_model_to_db_dict(m, self._config.names)
                    for m in models_page
                ]
            )
            result = await conn.execute(stmt)
            inserted += result.rowcount

    async def get_ts_with_visualization_vector(
        self,
        conn: AsyncConnection,
        origin_vector: list[float] | None,
        origin_ts_uid: int | None,
        radius: float,
        limit: int,
        exclude_ts_uids: list[int] | None = None,
    ) -> list[TSWithVisualizationVectorModel]:
        meta_ts_table = await self.get_metadata_table()
        ts_visualization_vectors_table = await self.get_ts_visualization_vectors_table()
        join_stmt = select(
            meta_ts_table,
            ts_visualization_vectors_table.c[
                self._config.names.ts_visualization_vectors_vector_col
            ],
        ).join(
            ts_visualization_vectors_table,
            meta_ts_table.c[self._config.names.meta_ts_uid_col]
            == ts_visualization_vectors_table.c[
                self._config.names.ts_visualization_vectors_ts_uid_col
            ],
        )
        stmt = (
            join_stmt.where(
                ts_visualization_vectors_table.c[
                    self._config.names.ts_visualization_vectors_vector_col
                ].l2_distance(
                    origin_vector
                    if origin_vector is not None
                    else (
                        select(
                            ts_visualization_vectors_table.c[
                                self._config.names.ts_visualization_vectors_vector_col
                            ]
                        ).where(
                            ts_visualization_vectors_table.c[
                                self._config.names.ts_visualization_vectors_ts_uid_col
                            ]
                            == origin_ts_uid
                        )
                    )
                )
                <= radius
            )
            .order_by(meta_ts_table.c[self._config.names.meta_ts_uid_col].asc())
            .limit(limit)
        )
        if origin_ts_uid is not None:
            stmt = stmt.union(  # type: ignore
                join_stmt.where(
                    meta_ts_table.c[self._config.names.meta_ts_uid_col] == origin_ts_uid
                )
            )
        if exclude_ts_uids:
            vv_subquery = stmt.alias("vv_subquery")
            stmt = select(vv_subquery).where(
                vv_subquery.c[self._config.names.meta_ts_uid_col].not_in(
                    exclude_ts_uids
                )
            )

        results = (await conn.execute(stmt)).mappings().fetchall()
        return [
            Mapper.db_row_to_ts_with_visualization_vector(d, self._config.names)
            for d in results
        ]

    async def get_ts_uids_with_vv(
        self, conn: AsyncConnection, ts_uids: list[int]
    ) -> list[int]:
        ts_uids = list(set(ts_uids))
        ts_visualization_vectors_table = await self.get_ts_visualization_vectors_table()
        stmt = select(
            ts_visualization_vectors_table.c[
                self._config.names.ts_visualization_vectors_ts_uid_col
            ]
        ).where(
            ts_visualization_vectors_table.c[
                self._config.names.ts_visualization_vectors_ts_uid_col
            ].in_(ts_uids)
        )
        results = (await conn.execute(stmt)).mappings().fetchall()
        return [
            d[self._config.names.ts_visualization_vectors_ts_uid_col] for d in results
        ]

    async def close(self):
        self._logger.info("Will dispose of async engine")
        await self._engine.dispose()
        self._logger.info("Disposed of async engine")

    @property
    def meta_ts_uid_col_name(self):
        return self._config.names.meta_ts_uid_col

    @property
    def last_time_col_name(self):
        return self._config.names.meta_ts_last_time_col

    @property
    def last_update_time_col_name(self):
        return self._config.names.meta_ts_last_update_time_col

    @property
    def successful_last_update_time_col_name(self):
        return self._config.names.meta_ts_successful_last_update_time_col

    @property
    def ts_time_col_name(self):
        return self._config.names.ts_time_col

    @property
    def ts_value_col_name(self):
        return self._config.names.ts_value_col

    @property
    def ts_to_tag_ts_uid_col(self):
        return self._config.names.ts_to_tag_ts_uid_col

    @property
    def ts_to_metric_ts_uid_col(self):
        return self._config.names.ts_to_metric_ts_uid_col
