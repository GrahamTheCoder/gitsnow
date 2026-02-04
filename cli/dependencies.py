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


def extract_join_edges_by_target(root_dir: Path) -> dict[str, list[tuple[str, str, str, str]]]:
    """
    Scan all .sql files and return join edges per target table.
    Each edge is (left_table, left_column, right_table, right_column).
    """
    sql_files = list(root_dir.rglob("**/*.sql"))
    if not sql_files:
        return {}

    path_by_obj, _ = extract_dependency_graph(root_dir)
    targets_by_path: dict[Path, set[str]] = defaultdict(set)
    for obj_name, path in path_by_obj.items():
        targets_by_path[path].add(obj_name)

    edges_by_target: dict[str, list[tuple[str, str, str, str]]] = defaultdict(list)

    for file_path in sql_files:
        try:
            file_sql = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        join_edges = _extract_join_edges(file_sql)
        if not join_edges:
            continue

        for target in targets_by_path.get(file_path, set()):
            edges_by_target[target].extend(join_edges)

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
    join_edges_by_target = extract_join_edges_by_target(root_dir)
    normalized_table = _normalize_table_name(target_table)
    normalized_target_col = _normalize_column_name(target_column)
    normalized_filter_col = _normalize_column_name(filter_column)

    target_col_key = f"{normalized_table}.{normalized_target_col}"
    filter_col_key = f"{normalized_table}.{normalized_filter_col}"

    target_paths = build_column_lineage_paths(target_col_key, edges_by_target, max_depth=max_depth)

    lines: list[str] = []
    if not target_paths:
        lines.append(
            f"-- No column lineage found for {normalized_table}.{normalized_target_col}"
        )
        return lines

    for idx, path in enumerate(target_paths, start=1):
        lines.append("")
        lines.append(f"-- Path {idx}")
        lines.extend(
            _build_cte_chain_for_path(
                path=path,
                filter_column=normalized_filter_col,
                filter_value=filter_value,
                join_edges_by_target=join_edges_by_target,
            )
        )

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


def _extract_join_edges(sql_text: str) -> list[tuple[str, str, str, str]]:
    ident = r'(?:[A-Za-z_][\w$]*|"[^"]+")'
    table_pattern = rf'{ident}(?:\s*\.\s*{ident}){{1,2}}'
    alias_pattern = ident

    alias_map: dict[str, str] = {}
    for match in re.finditer(rf'\b(from|join)\s+({table_pattern})(?:\s+({alias_pattern}))?', sql_text, re.IGNORECASE):
        raw_table = match.group(2)
        raw_alias = match.group(3)
        table_name = _normalize_table_name(raw_table)
        alias_map[table_name] = table_name
        if raw_alias:
            alias_map[_strip_quotes(raw_alias).upper()] = table_name

    edges: list[tuple[str, str, str, str]] = []
    for match in re.finditer(rf'({alias_pattern})\s*\.\s*({alias_pattern})\s*=\s*({alias_pattern})\s*\.\s*({alias_pattern})', sql_text, re.IGNORECASE):
        left_alias = _strip_quotes(match.group(1)).upper()
        left_col = _strip_quotes(match.group(2)).upper()
        right_alias = _strip_quotes(match.group(3)).upper()
        right_col = _strip_quotes(match.group(4)).upper()

        left_table = alias_map.get(left_alias)
        right_table = alias_map.get(right_alias)
        if not left_table or not right_table:
            continue

        edges.append((left_table, left_col, right_table, right_col))

    return edges


def _build_join_filter_queries(
    upstream_table: str,
    join_edges: list[tuple[str, str, str, str]],
    resolved_filter_columns: dict[str, str],
    filter_value: str,
) -> list[str]:
    queries: list[str] = []
    for left_table, left_col, right_table, right_col in join_edges:
        if left_table == upstream_table:
            other_table = right_table
            upstream_join_col = left_col
            other_join_col = right_col
        elif right_table == upstream_table:
            other_table = left_table
            upstream_join_col = right_col
            other_join_col = left_col
        else:
            continue

        filter_col = resolved_filter_columns.get(other_table)
        if not filter_col:
            continue

        queries.append(
            f"select * from {upstream_table} where {upstream_join_col} in (select {other_join_col} from {other_table} where {filter_col} = {filter_value});"
        )

    return queries


def _strip_quotes(value: str) -> str:
    return value.strip().strip('"')


def _build_cte_chain_for_path(
    path: list[str],
    filter_column: str,
    filter_value: str,
    join_edges_by_target: dict[str, list[tuple[str, str, str, str]]],
) -> list[str]:
    """
    Build a WITH CTE chain for a single lineage path.
    Path is a list of column keys in source -> target order.
    """
    table_sequence = [_table_key(col) for col in path]
    table_sequence = [table_sequence[-1]] + list(reversed(table_sequence[:-1]))

    cte_lines: list[str] = []
    cte_lines.append("with")

    if not table_sequence:
        return cte_lines

    first_table = table_sequence[0]
    cte_lines.append(
        f"  cte_0 as (select * from {first_table} where {filter_column} = {filter_value})"
    )

    for idx in range(1, len(table_sequence)):
        downstream_table = table_sequence[idx - 1]
        upstream_table = table_sequence[idx]
        join_edges = join_edges_by_target.get(downstream_table, [])
        join_edge = _find_join_edge(downstream_table, upstream_table, join_edges)
        if join_edge:
            upstream_join_col, downstream_join_col, is_indirect = join_edge
            if is_indirect:
                cte_lines.append(
                    f", cte_{idx} as (select * from {upstream_table} where {upstream_join_col} in (select {downstream_join_col} from cte_{idx - 1}))"
                )
            else:
                cte_lines.append(
                    f", cte_{idx} as (select * from {upstream_table} where {upstream_join_col} in (select {downstream_join_col} from cte_{idx - 1}))"
                )
        else:
            cte_lines.append(
                f", cte_{idx} as (select * from {upstream_table})"
            )

    cte_lines.append(f"select * from cte_{len(table_sequence) - 1};")
    return cte_lines


def _find_join_edge(
    downstream_table: str,
    upstream_table: str,
    join_edges: list[tuple[str, str, str, str]],
) -> tuple[str, str, bool] | None:
    for left_table, left_col, right_table, right_col in join_edges:
        if left_table == downstream_table and right_table == upstream_table:
            return right_col, left_col, False
        if right_table == downstream_table and left_table == upstream_table:
            return left_col, right_col, False

    for left_table, left_col, right_table, right_col in join_edges:
        if left_table == upstream_table:
            return left_col, right_col, True
        if right_table == upstream_table:
            return right_col, left_col, True
    return None
