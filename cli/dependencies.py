from dataclasses import dataclass
import logging
import re

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
