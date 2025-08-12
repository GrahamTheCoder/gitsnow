from pathlib import Path
import sys


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

    def normalize_name(table: Table) -> str:
        # Keep same normalization strategy as before
        return str(table.schema.raw_name + "." + table.raw_name).lower()

    object_to_file_map: dict[str, Path] = {}
    dependencies_by_target: dict[str, set[str]] = {}

    for file_path in sql_files:
        try:
            runner = LineageRunner(file_path=file_path, dialect=DIALECT, sql=file_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Skipping {file_path}: {e}", file=sys.stderr)
            continue # Ignore files that cannot be parsed at all

        targets: list[Table] = runner.target_tables
        if not targets:
            continue

        sources: set[str] = {normalize_name(s) for s in runner.source_tables}
        for target_table in targets:
            qualified_target = normalize_name(target_table)
            object_to_file_map[qualified_target] = file_path
            dependencies_by_target.setdefault(qualified_target, set()).update(sources)

    return object_to_file_map, dependencies_by_target

def order_files_by_deepest_dependency_first(
    object_to_file_map: dict[str, Path],
    dependencies_by_target: dict[str, set[str]],
) -> list[Path]:
    from graphlib import TopologicalSorter, CycleError
    files = set(object_to_file_map.values())
    graph: dict[Path, set[Path]] = {fp: set() for fp in files}
    for t, srcs in dependencies_by_target.items():
        if t in object_to_file_map:
            tfp = object_to_file_map[t]
            graph[tfp] |= {object_to_file_map[s] for s in srcs if s in object_to_file_map and object_to_file_map[s] != tfp}
    try:
        return list(TopologicalSorter(graph).static_order())
    except CycleError:
        return list(graph.keys())

def get_dependency_ordered_files(root_dir: Path) -> list[Path]:
    """
    Reads all .sql files in a directory, parses them for dependencies,
    and returns a list of file paths ordered by deepest dependency first.
    """
    object_to_file_map, dependencies_by_target = extract_dependency_graph(root_dir)
    if not object_to_file_map:
        return []
    return order_files_by_deepest_dependency_first(object_to_file_map, dependencies_by_target)

