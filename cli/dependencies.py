from dataclasses import dataclass
import logging
import re
import warnings
from collections import defaultdict

from graphlib import TopologicalSorter, CycleError
from pathlib import Path
from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner
from sqlfluff.core import Linter, FluffConfig

DIALECT = "snowflake"
warnings.filterwarnings(
    "ignore",
    message=r"SQLLineage doesn't support analyzing statement type.*",
)


@dataclass(frozen=True)
class SnowflakeName:
    name: str
    schema: str

    @property
    def schema_qualified_name(self) -> str:
        return f'{self.schema}.{self.name}'


def extract_dependency_graph(root_dir: Path, quiet: bool = False) -> tuple[dict[str, Path], dict[str, set[str]]]:
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
            normalized_sql = _normalize_lineage_sql(file_sql)
            runner = LineageRunner(file_path=str(file_path), dialect=DIALECT, sql=normalized_sql, silent_mode=True)
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
            if quiet:
                logging.debug("Using basic parsing for: %s.%s", assumed_schema, assumed_obj_name)
            else:
                print(f"Using basic parsing for: {assumed_schema}.{assumed_obj_name}")
            sql = _normalize_lineage_sql(file_path.read_text(encoding="utf-8"))
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
    path_by_obj, dependencies_by_obj = extract_dependency_graph(root_dir, quiet=True)
    if not path_by_obj:
        return []
    ordered_objects = order_objects_topologically(
        list(path_by_obj.keys()), dependencies_by_obj)
    return [
        (obj, path_by_obj[obj], sorted(
            dependencies_by_obj[obj], key=ordered_objects.index))
        for obj in ordered_objects if obj in path_by_obj
    ]


def collect_dependency_paths(root_dir: Path, target_table: str) -> set[Path]:
    """
    Collect file paths for the target table and its transitive dependencies.
    """
    path_by_obj, dependencies_by_obj = extract_dependency_graph(root_dir, quiet=True)
    normalized_target = _normalize_table_name(target_table)

    visited: set[str] = set()
    to_visit = [normalized_target]

    while to_visit:
        current = to_visit.pop()
        if current in visited:
            continue
        visited.add(current)
        for dep in dependencies_by_obj.get(current, set()):
            if dep not in visited:
                to_visit.append(dep)

    return {path_by_obj[obj] for obj in visited if obj in path_by_obj}


def extract_column_lineage_edges(root_dir: Path, include_paths: set[Path] | None = None) -> dict[str, set[str]]:
    """
    Scan all .sql files and return a mapping of:
      target_column -> set of source_columns

    Column names are normalized as SCHEMA.TABLE.COLUMN (upper-case).
    """
    sql_files = list(root_dir.rglob("**/*.sql"))
    if not sql_files:
        return {}
    if include_paths:
        sql_files = [p for p in sql_files if p in include_paths]

    edges_by_target: dict[str, set[str]] = defaultdict(set)

    for file_path in sql_files:
        try:
            warnings.filterwarnings(
                "ignore",
                message=r"SQLLineage doesn't support analyzing statement type.*",
            )
            file_sql = _normalize_lineage_sql(file_path.read_text(encoding="utf-8"))
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


def extract_join_edges_by_target(root_dir: Path, include_paths: set[Path] | None = None) -> dict[str, list[tuple[str, str, str, str]]]:
    """
    Scan all .sql files and return join edges per target table.
    Each edge is (left_table, left_column, right_table, right_column).
    """
    sql_files = list(root_dir.rglob("**/*.sql"))
    if not sql_files:
        return {}
    if include_paths:
        sql_files = [p for p in sql_files if p in include_paths]

    path_by_obj, _ = extract_dependency_graph(root_dir, quiet=True)
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


def extract_table_columns_by_object(root_dir: Path, include_paths: set[Path] | None = None) -> dict[str, set[str]]:
    """
    Extract a best-effort set of column names for each object.
    """
    path_by_obj, _ = extract_dependency_graph(root_dir, quiet=True)
    columns_by_obj: dict[str, set[str]] = {}

    for obj_name, path in path_by_obj.items():
        if include_paths and path not in include_paths:
            continue
        try:
            raw_sql = path.read_text(encoding="utf-8")
        except OSError:
            continue

        sql_text = _normalize_lineage_sql(raw_sql)
        columns = _extract_defined_columns(sql_text)
        if columns:
            columns_by_obj[obj_name] = columns

    return columns_by_obj


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
    filter_predicates: list[tuple[str, str]] | None = None,
    table_dependency_depth: int = 5,
) -> list[str]:
    """
    Build a debug trace plan for a target column and filter column.
    Returns a list of output lines (Snowflake SQL comments + queries).
    """
    normalized_table = _normalize_table_name(target_table)
    _, dependencies_by_obj = extract_dependency_graph(root_dir, quiet=True)
    include_paths = collect_dependency_paths(root_dir, normalized_table)
    edges_by_target = extract_column_lineage_edges(root_dir, include_paths=include_paths)
    join_edges_by_target = extract_join_edges_by_target(root_dir, include_paths=include_paths)
    table_columns_by_obj = extract_table_columns_by_object(root_dir, include_paths=include_paths)
    normalized_target_col = _normalize_column_name(target_column)
    normalized_filter_col = _normalize_column_name(filter_column)

    target_col_key = f"{normalized_table}.{normalized_target_col}"

    if filter_predicates is None:
        filter_predicates = [(normalized_filter_col, filter_value)]
    else:
        filter_predicates = [
            (_normalize_column_name(col), value) for col, value in filter_predicates
        ]

    target_paths = build_column_lineage_paths(target_col_key, edges_by_target, max_depth=max_depth)
    target_paths = _extend_paths_with_table_dependencies(
        target_paths,
        dependencies_by_obj,
        join_edges_by_target,
        table_columns_by_obj,
        filter_columns={col for col, _ in filter_predicates},
        max_depth=table_dependency_depth,
    )

    lines: list[str] = []
    if not target_paths:
        lines.append(
            f"-- No column lineage found for {normalized_table}.{normalized_target_col}"
        )
        return lines

    lines.extend(
        _build_cte_chain_for_paths(
            paths=target_paths,
            filter_predicates=filter_predicates,
            join_edges_by_target=join_edges_by_target,
            table_columns_by_obj=table_columns_by_obj,
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


def _fmt_identifier(value: str) -> str:
    return value.lower()


def _extract_defined_columns(sql_text: str) -> set[str]:
    """
    Best-effort extraction of defined columns from CREATE TABLE or SELECT.
    """
    columns = _extract_columns_from_create_table(sql_text)
    if columns:
        return columns
    return _extract_columns_from_select(sql_text)


def _extract_columns_from_create_table(sql_text: str) -> set[str]:
    match = re.search(r'\bcreate\b[\s\S]*?\btable\b[\s\S]*?\(', sql_text, re.IGNORECASE)
    if not match:
        return set()

    start = match.end() - 1
    depth = 0
    column_chars: list[str] = []
    for idx in range(start, len(sql_text)):
        ch = sql_text[idx]
        if ch == '(':
            depth += 1
            if depth == 1:
                continue
        elif ch == ')':
            depth -= 1
            if depth == 0:
                break
        if depth >= 1:
            column_chars.append(ch)

    if not column_chars:
        return set()

    column_text = ''.join(column_chars)
    columns: set[str] = set()
    segment = []
    depth = 0
    for ch in column_text:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if ch == ',' and depth == 0:
            col = ''.join(segment).strip()
            segment = []
            if col:
                columns.add(_strip_quotes(col.split()[0]).upper())
        else:
            segment.append(ch)
    last = ''.join(segment).strip()
    if last:
        columns.add(_strip_quotes(last.split()[0]).upper())
    return columns


def _extract_columns_from_select(sql_text: str) -> set[str]:
    try:
        linter = Linter(config=FluffConfig(overrides={"dialect": DIALECT}))
        parsed = linter.parse_string(sql_text)
        if not parsed.tree:
            return set()
        statement = next(parsed.tree.recursive_crawl("select_statement"), None)
        if not statement:
            return set()
        select_clause = next(statement.recursive_crawl("select_clause"), None)
        if not select_clause:
            return set()
        columns: set[str] = set()
        for element in select_clause.recursive_crawl("select_clause_element"):
            alias_expr = next(element.recursive_crawl("alias_expression"), None)
            if alias_expr:
                alias_name = next(alias_expr.recursive_crawl("naked_identifier"), None)
                if alias_name:
                    columns.add(_strip_quotes(alias_name.raw).upper())
                    continue
            column_ref = next(element.recursive_crawl("column_reference"), None)
            if column_ref:
                parts = [p.strip() for p in column_ref.raw.split(".") if p.strip()]
                if parts:
                    columns.add(_strip_quotes(parts[-1]).upper())
        return columns
    except Exception:
        return set()


def _normalize_lineage_sql(sql_text: str) -> str:
    """
    Normalize Snowflake dynamic table DDL to improve lineage parsing.
    """
    text = re.sub(r'\bdynamic\s+table\b', 'table', sql_text, flags=re.IGNORECASE)
    text = re.sub(
        r'\)\s*(?:target_lag|refresh_mode|initialize|warehouse)\b[\s\S]*?\bas\b',
        ') as',
        text,
        flags=re.IGNORECASE,
    )
    return text


def parse_debug_query(sql_text: str) -> tuple[str, str | None, list[tuple[str, str]]]:
    """
    Parse a simple SELECT query and return:
      (target_table, target_column, filter_predicates)

    filter_predicates are (column_name, value) with values left as raw SQL.
    """
    linter = Linter(config=FluffConfig(overrides={"dialect": DIALECT}))
    parsed = linter.parse_string(sql_text)
    if not parsed.tree:
        raise ValueError("Could not parse query.")

    statement = next(parsed.tree.recursive_crawl("select_statement"), None)
    if not statement:
        raise ValueError("Only SELECT queries are supported for parsing.")

    from_clause = next(statement.recursive_crawl("from_clause"), None)
    if not from_clause:
        raise ValueError("Could not find a FROM clause in the query.")

    table_ref = next(from_clause.recursive_crawl("table_reference"), None)
    if not table_ref:
        raise ValueError("Could not find a table reference in the FROM clause.")

    target_table = _normalize_table_name(table_ref.raw)

    alias_expr = next(from_clause.recursive_crawl("alias_expression"), None)
    alias_token = None
    if alias_expr:
        alias_name = next(alias_expr.recursive_crawl("naked_identifier"), None)
        if alias_name:
            alias_token = _strip_quotes(alias_name.raw).upper()

    target_column: str | None = None
    select_clause = next(statement.recursive_crawl("select_clause"), None)
    if select_clause:
        column_ref = next(select_clause.recursive_crawl("column_reference"), None)
        if column_ref:
            column_parts = [p.strip() for p in column_ref.raw.split(".") if p.strip()]
            if len(column_parts) >= 2:
                alias = _strip_quotes(column_parts[-2]).upper()
                if not alias_token or alias == alias_token:
                    target_column = _strip_quotes(column_parts[-1]).upper()
            elif column_parts:
                target_column = _strip_quotes(column_parts[-1]).upper()

    predicates: list[tuple[str, str]] = []
    where_clause = next(statement.recursive_crawl("where_clause"), None)
    if where_clause:
        tokens = [
            seg for seg in where_clause.recursive_crawl_all()
            if seg.is_type("column_reference")
            or seg.is_type("comparison_operator")
            or seg.is_type("quoted_literal")
            or seg.is_type("numeric_literal")
        ]
        for idx, seg in enumerate(tokens[:-2]):
            if not seg.is_type("column_reference"):
                continue
            op = tokens[idx + 1]
            value = tokens[idx + 2]
            if not op.is_type("comparison_operator") or "=" not in op.raw:
                continue
            if not (value.is_type("quoted_literal") or value.is_type("numeric_literal")):
                continue

            column_parts = [p.strip() for p in seg.raw.split(".") if p.strip()]
            alias = None
            column = None
            if len(column_parts) >= 2:
                alias = _strip_quotes(column_parts[-2]).upper()
                column = _strip_quotes(column_parts[-1]).upper()
            elif column_parts:
                column = _strip_quotes(column_parts[-1]).upper()

            if column:
                if not alias_token or not alias or alias == alias_token:
                    predicates.append((column, value.raw))

    return target_table, target_column, predicates


def _build_cte_chain_for_paths(
    paths: list[list[str]],
    filter_predicates: list[tuple[str, str]],
    join_edges_by_target: dict[str, list[tuple[str, str, str, str]]],
    table_columns_by_obj: dict[str, set[str]],
) -> list[str]:
    """
    Build a single WITH CTE chain for all lineage paths with branching CTE names.
    """
    root_table = None
    path_tables: list[list[str]] = []
    for path in paths:
        table_sequence = [_table_key(col) for col in path]
        table_sequence = [table_sequence[-1]] + list(reversed(table_sequence[:-1]))
        if not table_sequence:
            continue
        if root_table is None:
            root_table = table_sequence[0]
        path_tables.append(table_sequence)

    if not root_table:
        return ["-- No tables found for lineage paths."]

    tree = _build_table_path_tree(root_table, path_tables)
    cte_lines: list[str] = ["with"]

    root_name = "cte_0"
    if filter_predicates:
        filters = " and ".join(
            f"{_fmt_identifier(column)} = {value}" for column, value in filter_predicates
        )
        cte_lines.append(
            f"  {root_name} as (select * from {_fmt_identifier(root_table)} where {filters})"
        )
    else:
        cte_lines.append(
            f"  {root_name} as (select * from {_fmt_identifier(root_table)})"
        )

    leaf_ctes: list[str] = []
    leaf_depths: dict[str, int] = {}

    def _emit_children(node, parent_cte: str, prefix: str, depth: int) -> None:
        children = node["children"]
        if not children:
            leaf_ctes.append(parent_cte)
            leaf_depths[parent_cte] = depth
            return

        emitted = False
        for idx, child in enumerate(children):
            child_name = f"{prefix}_{idx}"
            parent_table = node["table"]
            child_table = child["table"]
            join_edges = join_edges_by_target.get(parent_table, [])
            join_edge = _find_join_edge(parent_table, child_table, join_edges)
            if join_edge:
                upstream_join_col, downstream_join_col, _ = join_edge
                cte_lines.append(
                    f", {child_name} as (select * from {_fmt_identifier(child_table)} where {_fmt_identifier(upstream_join_col)} in (select {_fmt_identifier(downstream_join_col)} from {parent_cte}))"
                )
                _emit_children(child, child_name, child_name, depth + 1)
                emitted = True
                continue

            if filter_predicates:
                child_columns = table_columns_by_obj.get(child_table, set())
                applicable_columns = [
                    column for column, _ in filter_predicates if column in child_columns
                ]
                if applicable_columns:
                    filter_terms = " and ".join(
                        f"{_fmt_identifier(column)} in (select {_fmt_identifier(column)} from {parent_cte})"
                        for column in applicable_columns
                    )
                    cte_lines.append(
                        f", {child_name} as (select * from {_fmt_identifier(child_table)} where {filter_terms})"
                    )
                    _emit_children(child, child_name, child_name, depth + 1)
                    emitted = True
                continue

            continue

        if not emitted:
            leaf_ctes.append(parent_cte)
            leaf_depths[parent_cte] = depth

    _emit_children(tree, root_name, root_name, 0)

    if leaf_ctes:
        deepest_leaf = max(leaf_ctes, key=lambda name: (leaf_depths.get(name, 0), name))
        cte_lines.append(f"select * from {deepest_leaf};")
        if len(leaf_ctes) > 1:
            cte_lines.append(f"-- Leaf CTEs: {', '.join(leaf_ctes)}")
    else:
        cte_lines.append(f"select * from {root_name};")

    return cte_lines


def _build_table_path_tree(root_table: str, paths: list[list[str]]):
    root = {"table": root_table, "children": []}
    for path in paths:
        if not path or path[0] != root_table:
            continue
        current = root
        for table in path[1:]:
            existing = next((c for c in current["children"] if c["table"] == table), None)
            if not existing:
                existing = {"table": table, "children": []}
                current["children"].append(existing)
            current = existing
    return root


def build_table_dependency_paths(
    target_table: str,
    dependencies_by_obj: dict[str, set[str]],
    max_depth: int = 10,
) -> list[list[str]]:
    """
    Build table dependency paths in source -> target order.
    """

    def _dfs(current: str, depth: int, visiting: set[str]) -> list[list[str]]:
        if current in visiting:
            return []
        if depth <= 0:
            return [[current]]

        deps = dependencies_by_obj.get(current)
        if not deps:
            return [[current]]

        visiting.add(current)
        paths: list[list[str]] = []
        for dep in sorted(deps):
            for sub_path in _dfs(dep, depth - 1, visiting):
                paths.append(sub_path + [current])
        visiting.remove(current)
        return paths

    return _dfs(target_table, max_depth, set())


def _extend_paths_with_table_dependencies(
    paths: list[list[str]],
    dependencies_by_obj: dict[str, set[str]],
    join_edges_by_target: dict[str, list[tuple[str, str, str, str]]],
    table_columns_by_obj: dict[str, set[str]],
    filter_columns: set[str],
    max_depth: int = 10,
) -> list[list[str]]:
    """
    Extend column lineage paths using table-level dependencies when sources are leaves.
    """
    expanded: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()

    def _deps_with_filters(table: str) -> list[str]:
        deps = dependencies_by_obj.get(table, set())
        if not deps:
            return []
        schema_prefix = table.split(".")[0] + "."
        same_schema = [d for d in deps if d.startswith(schema_prefix)]
        cross_schema = [d for d in deps if not d.startswith(schema_prefix)]

        join_edges = join_edges_by_target.get(table, [])
        join_tables = {left for left, _, _, _ in join_edges} | {right for _, _, right, _ in join_edges}

        candidates = same_schema[:] if same_schema else []
        for dep in cross_schema:
            if dep in join_tables:
                candidates.append(dep)

        if not candidates:
            candidates = list(deps)

        if not filter_columns:
            return candidates

        filtered = [
            dep for dep in candidates
            if dep in join_tables or filter_columns & table_columns_by_obj.get(dep, set())
        ]
        return filtered if filtered else candidates

    def _build_dep_paths(table: str, depth: int, visiting: set[str]) -> list[list[str]]:
        if table in visiting:
            return []
        if depth <= 0:
            return [[table]]

        deps = _deps_with_filters(table)
        if not deps:
            return [[table]]

        visiting.add(table)
        paths_out: list[list[str]] = []
        for dep in deps:
            for sub in _build_dep_paths(dep, depth - 1, visiting):
                paths_out.append(sub + [table])
        visiting.remove(table)
        return paths_out

    for path in paths:
        if not path:
            continue
        source_table = _table_key(path[0])
        dep_paths = _build_dep_paths(source_table, max_depth, set())

        for dep_path in dep_paths:
            merged = dep_path + path[1:]
            key = tuple(merged)
            if key in seen:
                continue
            seen.add(key)
            expanded.append(merged)

    return expanded


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
    return None
