from dataclasses import dataclass
import logging
import re
from collections import defaultdict

from graphlib import TopologicalSorter, CycleError
from pathlib import Path
from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner

DIALECT = "snowflake"


@dataclass(frozen=True)
class SnowflakeName:
    name: str
    schema: str

    @property
    def schema_qualified_name(self) -> str:
        return f'{self.schema}.{self.name}'


def extract_dependency_graph(root_dir: Path) -> tuple[dict[str, Path], dict[str, set[str]]]:
    """
    Scan all .sql files and return:
      - object_to_file_map: normalized target object name -> Path to file defining it
      - dependencies_by_target: target object name -> set of normalized source object names
    """
    # Use a simple recursive glob that actually finds all SQL files.
    sql_files = list(root_dir.rglob("**/*.sql"))
    if not sql_files:
        return {}, {}

    path_by_obj: dict[str, Path] = {}
    dependencies_by_obj: dict[str, set[str]] = {}

    expected_names = set((p.parent.parent.name + "." + p.stem).upper()
                         for p in sql_files)

    for file_path in sql_files:
        runner: LineageRunner | None = None
        target_objects: list[SnowflakeName] = []
        source_objects: list[SnowflakeName] = []
        try:
            file_sql = file_path.read_text(encoding="utf-8")
            runner = LineageRunner(file_path=str(file_path), dialect=DIALECT, sql=file_sql, silent_mode=True)
            source_objects = [
                SnowflakeName(name=t.raw_name.upper(), schema=t.schema.raw_name.upper())
                for t in runner.source_tables
            ]
            target_objects = [
                SnowflakeName(name=t.raw_name.upper(), schema=t.schema.raw_name.upper())
                for t in runner.target_tables
            ]
        except SQLLineageException as e:
            logging.debug("LineageRunner failed for %s: %s", file_path, e)

        assumed_schema = file_path.parent.parent.name
        assumed_obj_name = file_path.stem

        if not runner or not target_objects:
            print(
                f"Using basic parsing for: {assumed_schema}.{assumed_obj_name}")
            sql = file_path.read_text(encoding="utf-8")
            target_name = SnowflakeName(name=assumed_obj_name.upper(), schema=assumed_schema.upper())
            target_objects = [target_name]
            possible_names = _find_possible_names_in_sql(sql, assumed_schema)
            source_objects = [
                n for n in possible_names
                if n.schema_qualified_name in expected_names
            ]

        target_names = [t.schema_qualified_name for t in target_objects]
        source_names = [
            s.schema_qualified_name for s in source_objects if s not in target_objects
        ]

        for qualified_target in target_names:
            path_by_obj[qualified_target] = file_path
            dependencies_by_obj.setdefault(qualified_target, set()).update(source_names)

    return path_by_obj, dependencies_by_obj


def order_objects_topologically(
    objs: list[str],
    dependencies_by_obj: dict[str, set[str]],
) -> list[str]:
    """
    Returns a list of object names ordered topologically by their dependencies.
    If a cycle is detected, returns objects in arbitrary order.
    """
    graph: dict[str, set[str]] = {obj: set() for obj in objs}
    for obj, dependencies in dependencies_by_obj.items():
        graph[obj] |= {d for d in dependencies if d != obj}
    try:
        return list(TopologicalSorter(graph).static_order())
    except CycleError:
        return list(graph.keys())


def get_dependency_ordered_objects(root_dir: Path) -> list[tuple[str, Path, list[str]]]:
    """
    Reads all .sql files in a directory
    Returns a list in dependency order of tuples.
    The tuple contains the qualified object name, the file path, and the ordered list of dependencies
    """
    path_by_obj, dependencies_by_obj = extract_dependency_graph(root_dir)
    if not path_by_obj:
        return []
    ordered_objects = order_objects_topologically(
        list(path_by_obj.keys()), dependencies_by_obj)
    return [
        (obj, path_by_obj[obj], sorted(
            dependencies_by_obj[obj], key=ordered_objects.index))
        for obj in ordered_objects if obj in path_by_obj
    ]


def extract_column_lineage_edges(root_dir: Path) -> dict[str, set[str]]:
    """
    Scan all .sql files and return a mapping of:
      target_column -> set of source_columns

    Column names are normalized as SCHEMA.TABLE.COLUMN (upper-case).
    """
    sql_files = list(root_dir.rglob("**/*.sql"))
    if not sql_files:
        return {}

    edges_by_target: dict[str, set[str]] = defaultdict(set)

    for file_path in sql_files:
        try:
            file_sql = file_path.read_text(encoding="utf-8")
            runner = LineageRunner(
                file_path=str(file_path),
                dialect=DIALECT,
                sql=file_sql,
                silent_mode=True,
            )
            col_lineage = runner.get_column_lineage()
        except SQLLineageException as e:
            logging.debug("LineageRunner failed for %s: %s", file_path, e)
            continue
        except Exception as e:
            logging.debug("Column lineage failed for %s: %s", file_path, e)
            continue

        assumed_schema = file_path.parent.parent.name

        for item in col_lineage:
            if isinstance(item, tuple) and len(item) == 2:
                source_columns = [item[0]]
                target_columns = [item[1]]
            elif hasattr(item, "source_columns") and hasattr(item, "target_columns"):
                source_columns = list(item.source_columns)
                target_columns = list(item.target_columns)
            else:
                continue

            for source_col in source_columns:
                for target_col in target_columns:
                    source_key = _normalize_column_key(source_col, assumed_schema)
                    target_key = _normalize_column_key(target_col, assumed_schema)
                    if not source_key or not target_key:
                        continue
                    edges_by_target[target_key].add(source_key)

    return dict(edges_by_target)


def build_column_lineage_paths(
    target_column_key: str,
    edges_by_target: dict[str, set[str]],
    max_depth: int = 10,
) -> list[list[str]]:
    """
    Build all column lineage paths from sources to the target column.
    Paths are returned as lists of column keys in source -> target order.
    """

    def _dfs(current: str, depth: int, visiting: set[str]) -> list[list[str]]:
        if current in visiting:
            return []
        if depth <= 0:
            return [[current]]

        sources = edges_by_target.get(current)
        if not sources:
            return [[current]]

        visiting.add(current)
        paths: list[list[str]] = []
        for src in sorted(sources):
            for sub_path in _dfs(src, depth - 1, visiting):
                paths.append(sub_path + [current])
        visiting.remove(current)
        return paths

    return _dfs(target_column_key, max_depth, set())


def build_debug_trace_plan(
    root_dir: Path,
    target_table: str,
    target_column: str,
    filter_column: str,
    filter_value: str,
    max_depth: int = 10,
) -> list[str]:
    """
    Build a debug trace plan for a target column and filter column.
    Returns a list of output lines (Snowflake SQL comments + queries).
    """
    edges_by_target = extract_column_lineage_edges(root_dir)
    normalized_table = _normalize_table_name(target_table)
    normalized_target_col = _normalize_column_name(target_column)
    normalized_filter_col = _normalize_column_name(filter_column)

    target_col_key = f"{normalized_table}.{normalized_target_col}"
    filter_col_key = f"{normalized_table}.{normalized_filter_col}"

    target_paths = build_column_lineage_paths(target_col_key, edges_by_target, max_depth=max_depth)
    filter_paths = build_column_lineage_paths(filter_col_key, edges_by_target, max_depth=max_depth)

    table_filter_columns: dict[str, set[str]] = defaultdict(set)
    for path in filter_paths:
        for col_key in path:
            table_filter_columns[_table_key(col_key)].add(_column_name(col_key))

    resolved_filter_columns = {
        table: next(iter(cols))
        for table, cols in table_filter_columns.items()
        if len(cols) == 1
    }

    lines: list[str] = []
    lines.append("-- Start query")
    lines.append(
        f"select {normalized_target_col} from {normalized_table} where {normalized_filter_col} = {filter_value};"
    )

    if filter_paths:
        lines.append("")
        lines.append(f"-- Filter column lineage for {normalized_filter_col}")
        for idx, path in enumerate(filter_paths, start=1):
            lines.append(f"-- Filter path {idx}")
            for step in range(len(path) - 1, -1, -1):
                col_key = path[step]
                table_key = _table_key(col_key)
                column_name = _column_name(col_key)
                lines.append(f"-- {table_key}")
                lines.append(
                    f"select * from {table_key} where {column_name} = {filter_value};"
                )

    if not target_paths:
        lines.append(
            f"-- No column lineage found for {normalized_table}.{normalized_target_col}"
        )
        return lines

    for idx, path in enumerate(target_paths, start=1):
        lines.append("")
        lines.append(f"-- Path {idx}")

        # Walk from target -> sources for debugging order
        for step in range(len(path) - 1, 0, -1):
            downstream_col = path[step]
            upstream_col = path[step - 1]
            downstream_table = _table_key(downstream_col)
            upstream_table = _table_key(upstream_col)

            lines.append(f"-- {downstream_table} -> {upstream_table}")

            filter_col_for_upstream = resolved_filter_columns.get(upstream_table)
            if filter_col_for_upstream:
                lines.append(
                    f"select * from {upstream_table} where {filter_col_for_upstream} = {filter_value};"
                )
            elif upstream_table in table_filter_columns:
                lines.append(
                    f"-- Multiple filter column candidates found for {upstream_table}; inspect lineage."
                )
                lines.append(f"select * from {upstream_table};")
            else:
                lines.append(
                    f"-- No filter column lineage found for {upstream_table}; inspect join keys manually."
                )
                lines.append(f"select * from {upstream_table};")

    return lines


def _find_possible_names_in_sql(sql_text: str, assumed_schema_name: str) -> set[SnowflakeName]:
    """Lightweight regex-based scan to find qualified object names in SQL."""

    # match 1-3 dot-separated identifiers, allowing double-quoted identifiers
    ident = r'(?:[A-Za-z_][\w$]*|"[^"]+")'
    pattern = re.compile(rf'\b{ident}(?:\s*\.\s*{ident}){{0,2}}\b')
    names: set[SnowflakeName] = set()
    for match in pattern.findall(sql_text):
        # pattern.findall with this regex returns the whole match when there's
        # one capture group; to be safe, re-run split
        token = match if isinstance(match, str) else match[0]
        parts = [p.strip().strip('"') for p in re.split(r'\s*\.\s*', token)]
        if not parts:
            continue
        if len(parts) == 1:
            names.add(SnowflakeName(parts[0].upper(), assumed_schema_name.upper()))
        else:
            # use last two parts as schema.table
            names.add(SnowflakeName(parts[-1].upper(), parts[-2].upper()))
    return names


def _normalize_table_name(raw_table_name: str) -> str:
    parts = [p.strip().strip('"') for p in re.split(r"\s*\.\s*", raw_table_name) if p.strip()]
    if len(parts) >= 2:
        return f"{parts[-2].upper()}.{parts[-1].upper()}"
    return raw_table_name.strip().strip('"').upper()


def _normalize_column_name(raw_column_name: str) -> str:
    return raw_column_name.strip().strip('"').upper()


def _normalize_column_key(column_obj, assumed_schema: str) -> str:
    try:
        table = column_obj.parent
        table_name = table.raw_name if table else None
        schema_obj = table.schema if table else None
        schema_name = schema_obj.raw_name if schema_obj else None
        if not schema_name or str(schema_name) == "<default>":
            schema_name = assumed_schema
        if not table_name or not schema_name:
            return ""
        return f"{schema_name.upper()}.{table_name.upper()}.{_normalize_column_name(column_obj.raw_name)}"
    except Exception:
        return ""


def _table_key(column_key: str) -> str:
    parts = column_key.split(".")
    return f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else column_key


def _column_name(column_key: str) -> str:
    parts = column_key.split(".")
    return parts[2] if len(parts) >= 3 else column_key
