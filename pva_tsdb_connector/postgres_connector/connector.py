import structlog
import datetime
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
)
from sqlalchemy import delete, update, distinct, and_
from sqlalchemy.dialects.postgresql import insert, JSONB

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
        self._engine = None
        self._metadata = MetaData()

    async def connect(self):
        try:
            self._engine = create_async_engine(
                self._url,
                echo=False,
                echo_pool=False,
                pool_size=self._config.connection.pool_min_size,
                max_overflow=self._config.connection.pool_max_size
                - self._config.connection.pool_min_size,
                query_cache_size=100,
            )
        except Exception as ex:
            err_msg = f"Failed to create SQLA engine to PostgreSQL. Error: {ex}."
            self._logger.error(err_msg, exc_info=True)
            raise TSDBException(err_msg) from Exception
        self._logger.info("Initialized Connector")

        try:
            async with self._engine.begin() as conn:
                await conn.run_sync(self._metadata.reflect)
        except Exception as ex:
            err_msg = f"Failed to reflect DB. Error: {ex}"
            self._logger.error(err_msg, exc_info=True)
            raise TSDBException(err_msg) from Exception
        self._logger.info("Reflected DB")

    async def init_db(self, metric_models: list[MetricModel]):
        await self.get_metadata_table()
        await self.get_ts_table()
        await self.get_tags_table()
        await self.get_ts_to_tags_table()
        await self.get_metrics_table()
        await self.get_ts_to_metrics_table()

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
                DateTime(timezone=False),
                nullable=False,
            ),
            Column(
                self._config.names.meta_ts_last_update_time_col,
                DateTime(timezone=False),
                nullable=False,
            ),
            Column(
                self._config.names.meta_ts_successful_last_update_time_col,
                DateTime(timezone=False),
                nullable=False,
            ),
            Column(
                self._config.names.meta_ts_next_update_time_col,
                DateTime(timezone=False),
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
                self._config.names.ts_time_col, DateTime(timezone=False), nullable=False
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

    async def get_ts_to_metrics_table(self):
        table = self._metadata.tables.get(self._config.names.ts_to_metrics_table, None)
        if table is not None:
            return table
        table = Table(
            self._config.names.ts_to_metrics_table,
            self._metadata,
            Column(
                self._config.names.ts_to_metric_ts_uid_col,
                Integer,
                ForeignKey(
                    f"{self._config.names.meta_ts_table}.{self._config.names.meta_ts_uid_col}",
                    name=f"{self._config.names.ts_to_metric_ts_uid_col}_fkey",
                ),
            ),
            Column(
                self._config.names.ts_to_metric_metric_uid_col,
                Integer,
                ForeignKey(
                    f"{self._config.names.metrics_table}.{self._config.names.metric_uid_col}",
                    name=f"{self._config.names.ts_to_metric_metric_uid_col}_fkey",
                ),
            ),
            Column(
                self._config.names.ts_to_metric_value_col,
                DOUBLE_PRECISION(),
                nullable=True,
            ),
            Column(self._config.names.ts_to_metric_data_json_col, JSONB, nullable=True),
            PrimaryKeyConstraint(
                self._config.names.ts_to_metric_ts_uid_col,
                self._config.names.ts_to_metric_metric_uid_col,
                name=self._config.names.ts_to_metric_pkey,
            ),
        )
        Index(
            self._config.names.ts_to_metric_value_index,
            table.c[self._config.names.ts_to_metric_value_col],
        )
        Index(
            self._config.names.ts_to_metric_metric_uid_index,
            table.c[self._config.names.ts_to_metric_metric_uid_col],
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
        self, conn: AsyncConnection, uids: list[int] = None
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
        uids: list[int] = None,
        uids_from_source: list[str] = None,
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
        self, conn: AsyncConnection, ts_uids: list[int] = None
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
            .on_conflict_do_nothing(self._config.names.ts_to_tags_unique_constraint)
        )

    async def get_metadata(
        self,
        conn: AsyncConnection,
        uids: list[int] = None,
        order_by: str = None,
        order_asc: bool = True,
        limit: int = None,
        offset: int = None,
    ) -> list[TSMetadataModel]:
        table = await self.get_metadata_table()
        stmt = select(table)
        if uids is not None:
            if len(uids) == 0:
                return []
            stmt = stmt.where(table.c[self._config.names.metric_uid_col].in_(uids))
        if order_by is not None:
            order_by_column = table.c[order_by]
            if order_asc:
                order_by_column = order_by_column.asc()
            else:
                order_by_column = order_by_column.desc()
            stmt = stmt.order_by(order_by_column)
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
        self, conn: AsyncConnection, ts_uids: list[int] = None
    ) -> list[TSToMetricModel]:
        table = await self.get_ts_to_metrics_table()
        stmt = select(table)
        if ts_uids is not None:
            if len(ts_uids) == 0:
                return []
            stmt = stmt.where(
                table.c[self._config.names.ts_to_metric_ts_uid_col].in_(ts_uids)
            )
        results = (await conn.execute(stmt)).mappings().fetchall()
        return [
            Mapper.db_row_to_ts_to_metric_model(
                d, names=self._config.names, max_abs=self._config.max_metric_value_abs
            )
            for d in results
        ]

    async def upsert_ts_to_metrics(
        self, conn: AsyncConnection, ts_to_metrics_models: list[TSToMetricModel]
    ):
        db_dicts = [
            Mapper.ts_to_metric_model_to_db_dict(model, names=self._config.names)
            for model in ts_to_metrics_models
        ]
        table = await self.get_ts_to_metrics_table()
        for db_dict in db_dicts:
            await conn.execute(
                insert(table)
                .values(db_dict)
                .on_conflict_do_update(
                    constraint=self._config.names.ts_to_metric_pkey,
                    set_={
                        self._config.names.ts_to_metric_value_col: db_dict[
                            self._config.names.ts_to_metric_value_col
                        ],
                        self._config.names.ts_to_metric_json_col: db_dict[
                            self._config.names.ts_to_metric_json_co
                        ],
                    },
                )
            )

    async def delete_ts_to_metrics(
        self, conn: AsyncConnection, ts_uids: list[int]
    ) -> None:
        if len(ts_uids) == 0:
            return
        table = await self.get_ts_to_metrics_table()
        await conn.execute(
            delete(table).where(
                table.c[self._config.names.ts_to_metric_ts_uid_col].in_(ts_uids)
            )
        )

    async def create_ts_metadata(
        self, ts_metadata_model: NewTSMetadataModel
    ) -> TSMetadataModel:
        db_dict = Mapper.ts_metadata_model_to_db_dict(
            ts_metadata_model, names=self._config.names
        )
        table = await self.get_metadata_table()
        with self._engine.connect() as conn:
            res = await conn.execute(
                insert(table).values([db_dict]).returning(table.c.uid)
            )
        return TSMetadataModel(uid=res[0], **(ts_metadata_model.model_dump()))

    async def update_ts_metadata(self, ts_metadata_model: TSMetadataModel) -> None:
        db_dict = Mapper.ts_metadata_model_to_db_dict(
            ts_metadata_model, names=self._config.names
        )
        table = await self.get_metadata_table()
        async with self._engine.connect() as conn:
            await conn.execute(
                update(table)
                .where(
                    table.c[self._config.names.meta_ts_uid_col] == ts_metadata_model.uid
                )
                .values(
                    {
                        k: v
                        for k, v in db_dict.items()
                        if k != self._config.names.meta_ts_uid_col
                    }
                )
            )

    async def update_ts_metadata_col(
        self, conn: AsyncConnection, col_name: str, uids: list[int], value: any
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
    ) -> list[TSDataModel]:
        table = await self.get_ts_table()
        stmt = select(table)
        if ts_uids is not None:
            if len(ts_uids) == 0:
                return []
            stmt = stmt.where(table.c[self._config.names.ts_uid_col].in_(ts_uids))
        order_by_column = table.columns[self._config.names.ts_time_col]
        if order_asc:
            order_by_column = order_by_column.asc()
        else:
            order_by_column = order_by_column.desc()
        stmt = stmt.order_by(order_by_column)
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
        await conn.execute(
            insert(table)
            .values(
                [Mapper.ts_data_model_to_db_dict(m, self._config.names) for m in models]
            )
            .on_conflict_do_nothing(self._config.names.ts_pkey)
        )

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

    async def get_filtered_metadata(
        self,
        conn: AsyncConnection,
        order_by_metric_uid: int,
        order_asc: bool,
        all_or_any_tags: AllOrAnyTags,
        limit: int,
        offset: int,
        tag_uids: list[int] | None,
    ) -> list[TSMetadataModel]:
        meta_ts_table = await self.get_metadata_table()
        ts_to_metrics_table = await self.get_ts_to_metrics_table()
        ts_to_tags_table = await self.get_ts_to_tags_table()
        order_by_col = ts_to_metrics_table.c[self._config.names.ts_to_metric_value_col]
        if order_asc:
            order_by_col = order_by_col.asc()
        else:
            order_by_col = order_by_col.desc()

        where_clauses = [
            ts_to_metrics_table.c[self._config.names.ts_to_metric_metric_uid_col]
            == order_by_metric_uid
        ]
        tags_subquery = None
        if tag_uids is not None and len(tag_uids) > 0:
            if all_or_any_tags == AllOrAnyTags.ANY:
                tags_subquery = select(
                    distinct(
                        ts_to_tags_table.c[self._config.names.ts_to_tag_ts_uid_col]
                    )
                ).where(
                    ts_to_tags_table.c[self._config.names.ts_to_tag_tag_uid_col].in_(
                        tag_uids
                    )
                )
            elif all_or_any_tags == AllOrAnyTags.ALL:
                tags_subquery = (
                    select(
                        distinct(
                            ts_to_tags_table.c[self._config.names.ts_to_tag_ts_uid_col]
                        )
                    )
                    .where(
                        ts_to_tags_table.c[
                            self._config.names.ts_to_tag_tag_uid_col
                        ].in_(tag_uids)
                    )
                    .group_by(
                        ts_to_tags_table.c[self._config.names.ts_to_tag_ts_uid_col]
                    )
                    .having(
                        func.count(
                            distinct(
                                ts_to_tags_table.c[
                                    self._config.names.ts_to_tag_tag_uid_col
                                ]
                            )
                        )
                        == len(tag_uids)
                    )
                )

        if tags_subquery is None:
            where_clause = where_clauses[0]
        else:
            where_clause = and_(
                where_clauses[0], meta_ts_table.c["uid"].in_(tags_subquery)
            )
        stmt = (
            select(meta_ts_table)
            .join(
                ts_to_metrics_table,
                meta_ts_table.c[self._config.names.meta_ts_uid_col]
                == ts_to_metrics_table.c[self._config.names.ts_to_metric_ts_uid_col],
            )
            .where(where_clause)
            .order_by(order_by_col)
            .offset(offset)
            .limit(limit)
        )
        res = (await conn.execute(stmt)).mappings().fetchall()
        return [
            Mapper.db_row_to_metadata_model(r, names=self._config.names) for r in res
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
