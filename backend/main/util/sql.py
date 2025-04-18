import csv
import io
from typing import Any, Collection, Iterable, Mapping, Optional, cast

import pandas as pd
from django.db.backends.utils import CursorWrapper
from psycopg import sql


def create_temp_table(
    cursor: CursorWrapper, table: str, columns: list[tuple[str, str, str]]
) -> None:
    stmt = sql.SQL("create temporary table {table} ({columns}) on commit drop").format(
        table=sql.Identifier(table),
        columns=sql.SQL(",").join(
            [
                sql.SQL("{col} {col_type} {constraints}").format(
                    col=sql.Identifier(col),
                    col_type=sql.SQL(col_type),
                    constraints=sql.SQL(constraints),
                )
                for col, col_type, constraints in columns
            ]
        ),
    )
    cursor.execute(stmt)


def create_temp_table_like(cursor: CursorWrapper, table: str, like_table: str) -> None:
    stmt = sql.SQL(
        "create temporary table {table} (like {like_table}) on commit drop"
    ).format(
        table=sql.Identifier(table),
        like_table=sql.Identifier(like_table),
    )
    cursor.execute(stmt)


def drop_table(cursor: CursorWrapper, table: str) -> None:
    stmt = sql.SQL("drop table {table};").format(
        table=sql.Identifier(table),
    )
    cursor.execute(stmt)


def add_column(
    cursor: CursorWrapper,
    table: str,
    column: str,
    column_type: str,
    constraints: list[str] = [],
) -> None:
    stmt = sql.SQL(
        "alter table {table} add column {column} {column_type} {constraints}"
    ).format(
        table=sql.Identifier(table),
        column=sql.Identifier(column),
        column_type=sql.SQL(column_type),
        constraints=sql.SQL(" ").join(
            [sql.SQL(constraint) for constraint in constraints]
        ),
    )
    cursor.execute(stmt)


def drop_column(cursor: CursorWrapper, table: str, column: str) -> None:
    stmt = sql.SQL("alter table {table} drop column {column}").format(
        table=sql.Identifier(table),
        column=sql.Identifier(column),
    )
    cursor.execute(stmt)


def create_index(
    cursor: CursorWrapper, table: str, column: str, index_name: str
) -> None:
    stmt = sql.SQL("create index {index_name} on {table} ({column})").format(
        index_name=sql.Identifier(index_name),
        table=sql.Identifier(table),
        column=sql.Identifier(column),
    )
    cursor.execute(stmt)


def load_data(
    cursor: CursorWrapper,
    table_name: str,
    data: Iterable[Mapping[str, object]],
    col_names: Collection[str],
) -> None:
    buffer = io.BytesIO()
    text_io = io.TextIOWrapper(buffer, encoding="utf-8", newline="", write_through=True)
    writer = csv.DictWriter(text_io, fieldnames=col_names, extrasaction="ignore")

    writer.writerows(data)
    text_io.detach()
    buffer.seek(0)

    # TODO: Log rows copied
    stmt = sql.SQL(
        "copy {table} ({columns}) from stdin with (format csv, delimiter ',')"
    ).format(
        table=sql.Identifier(table_name),
        columns=sql.SQL(",").join([sql.Identifier(col) for col in col_names]),
    )
    with cursor.copy(stmt) as copy:
        while chunk := buffer.read(1024):
            copy.write(chunk)


def load_df(
    cursor: CursorWrapper,
    table_name: str,
    df: pd.DataFrame,
    col_names: list[str],
) -> int:
    buffer = io.BytesIO()

    df.to_csv(buffer, columns=col_names, index=False)
    buffer.seek(0)

    stmt = sql.SQL(
        "copy {table} ({columns}) from stdin with (format csv, header, delimiter ',')"
    ).format(
        table=sql.Identifier(table_name),
        columns=sql.SQL(",").join([sql.Identifier(col) for col in col_names]),
    )

    with cursor.copy(stmt) as copy:
        copy.write(buffer.read())

    row_count = cursor.rowcount

    if row_count != len(df):
        raise Exception(
            f"Copied fewer rows than expected. Expected: {len(df)} Actual: {row_count}"
        )

    # pyright seems to pick this up correctly, but not mypy
    # error: Returning Any from function declared to return "int"  [no-any-return]
    return cast(int, row_count)


def extract_df(
    cursor: CursorWrapper,
    query: sql.Composed | sql.SQL,
    dtype: Mapping[str, str],
    query_params: Optional[Mapping[str, Any]] = {},
    na_filter: bool = True,
    parse_dates: list[str] = [],
) -> pd.DataFrame:
    stmt = sql.SQL(
        "copy ({query}) to stdin with (format csv, header, force_quote *, delimiter ',')"
    ).format(
        query=query,
    )

    with cursor.copy(stmt, query_params) as copy:
        buffer = io.BytesIO()

        for data in copy:
            buffer.write(data)

        buffer.seek(0)

        return pd.read_csv(
            buffer, dtype=dtype, na_filter=na_filter, parse_dates=parse_dates
        )
