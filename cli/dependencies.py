import sys

from graphlib import TopologicalSorter, CycleError
from pathlib import Path
from sqllineage.runner import LineageRunner
from sqllineage.core.models import Table

DIALECT = "snowflake"


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

    def normalize_name(table: Table, assumed_schema_name: str = '<default>') -> str:
        # Keep same normalization strategy as before
        schema_name = table.schema.raw_name if table.schema else assumed_schema_name
        return str(schema_name + "." + table.raw_name).lower()

    path_by_obj: dict[str, Path] = {}
    dependencies_by_obj: dict[str, set[str]] = {}

    expected_names = set((p.parent.parent.name + "." + p.stem).upper() for p in sql_files)

    for file_path in sql_files:
        runner: LineageRunner | None = None
        target_tables: list[Table] = []
        source_tables: list[Table] = []
        try:
            runner = LineageRunner(file_path=str(file_path), dialect=DIALECT, sql=file_path.read_text(
                encoding="utf-8"), silent_mode=True)
            source_tables = runner.source_tables
            target_tables = runner.target_tables
        except Exception as e:
            pass

        assumed_schema = file_path.parent.parent.name
        assumed_obj_name = file_path.stem

        if not runner or not target_tables:
            print(f"Using basic parsing for: {assumed_schema}.{assumed_obj_name}")
            sql = file_path.read_text(encoding="utf-8")
            source_tables = [Table(name=n)
                             for n in _find_qualified_names_in_sql(sql)
                             if n.upper() in expected_names]
            target_tables = [Table(name=assumed_obj_name)]

        source_names = [normalize_name(s, assumed_schema)
                        for s in source_tables]
        target_names = [normalize_name(t, assumed_schema)
                        for t in target_tables]

        for qualified_target in target_names:
            path_by_obj[qualified_target] = file_path
            dependencies_by_obj.setdefault(
                qualified_target, set()).update(source_names)

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


def _find_qualified_names_in_sql(sql_text: str) -> set[str]:
    """Lightweight regex-based scan to find qualified object names in SQL.

    Returns normalized names in the form 'schema.object' or 'object'.
    """
    import re

    # match 1-3 dot-separated identifiers, allowing double-quoted identifiers
    ident = r'(?:[A-Za-z_][\w$]*|"[^"]+")'
    pattern = re.compile(rf'\b{ident}(?:\s*\.\s*{ident}){{0,2}}\b')
    names: set[str] = set()
    for match in pattern.findall(sql_text):
        # pattern.findall with this regex returns the whole match when there's
        # one capture group; to be safe, re-run split
        token = match if isinstance(match, str) else match[0]
        parts = [p.strip().strip('"') for p in re.split(r'\s*\.\s*', token)]
        if not parts:
            continue
        if len(parts) == 1:
            names.add(f"{parts[0]}")
        else:
            # use last two parts as schema.table
            names.add(f"{parts[-2]}.{parts[-1]}")
    return names
