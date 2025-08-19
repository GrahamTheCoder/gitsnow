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

    for file_path in sql_files:
        try:
            runner = LineageRunner(file_path=file_path, dialect=DIALECT, sql=file_path.read_text(encoding="utf-8"))
            targets: list[Table] = runner.target_tables
        except Exception as e:
            print(f"Skipping {file_path}: {e}", file=sys.stderr)
            continue # Ignore files that cannot be parsed at all

        if not targets:
            continue

        sources: set[str] = {normalize_name(s) for s in runner.source_tables}
        for target_table in targets:
            qualified_target = normalize_name(target_table, file_path.parent.parent.name)
            path_by_obj[qualified_target] = file_path
            dependencies_by_obj.setdefault(qualified_target, set()).update(sources)

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
    ordered_objects = order_objects_topologically(list(path_by_obj.keys()), dependencies_by_obj)
    return [
        (obj, path_by_obj[obj], sorted(dependencies_by_obj[obj], key=ordered_objects.index))
                for obj in ordered_objects if obj in path_by_obj
        ]
