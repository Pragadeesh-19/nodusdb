import json
import sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    sys.exit(77)


def column_info(parquet_file):
    arrow_schema = parquet_file.schema_arrow
    columns = []
    for index in range(len(arrow_schema)):
        field = arrow_schema.field(index)
        metadata = field.metadata or {}
        field_id = metadata.get(b"PARQUET:field_id")
        columns.append({
            "name": field.name,
            "arrow_type": str(field.type),
            "field_id": int(field_id) if field_id is not None else None,
            "physical": parquet_file.schema.column(index).physical_type,
            "nullable": field.nullable,
        })
    return columns


def statistics(parquet_file):
    result = []
    for group_index in range(parquet_file.metadata.num_row_groups):
        group = parquet_file.metadata.row_group(group_index)
        entries = []
        for column_index in range(group.num_columns):
            stats = group.column(column_index).statistics
            if stats is None or not stats.has_min_max:
                entries.append(None)
                continue
            entries.append({"min": render(stats.min), "max": render(stats.max), "nulls": stats.null_count})
        result.append(entries)
    return result


def render(value):
    if hasattr(value, "timestamp"):
        return int(value.timestamp() * 1_000_000)
    return value


def values(table):
    columns = []
    for index in range(table.num_columns):
        column = table.column(index)
        if pa.types.is_timestamp(column.type):
            column = column.cast(pa.int64())
        columns.append(column.to_pylist())
    return columns


def main(path):
    parquet_file = pq.ParquetFile(path)
    table = parquet_file.read()
    print(json.dumps({
        "rows": table.num_rows,
        "row_groups": parquet_file.metadata.num_row_groups,
        "created_by": parquet_file.metadata.created_by,
        "columns": column_info(parquet_file),
        "statistics": statistics(parquet_file),
        "values": values(table),
    }))


if __name__ == "__main__":
    main(sys.argv[1])
