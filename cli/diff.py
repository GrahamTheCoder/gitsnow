import click
from pathlib import Path
from sqlfluff.core import Linter, FluffConfig

from cli.db import SnowflakeObject

from .format import get_formatter


def get_semantic_structure(parsed_tree):
    """Extract semantic elements, ignoring comments and whitespace."""
    semantic_elements = []

    def traverse(segment):
        # Skip comment segments and whitespace-only segments
        if segment.is_type("comment") or (hasattr(segment, 'raw') and segment.raw.isspace()):
            return

        # For meaningful segments, collect their type and content
        if hasattr(segment, 'segments') and segment.segments:
            for child in segment.segments:
                traverse(child)
        else:
            # Leaf node - collect normalized content
            if hasattr(segment, 'raw') and segment.raw.strip():
                semantic_elements.append({
                    'type': segment.get_type(),
                    # Keep original case, formatter will handle normalization
                    'content': segment.raw.strip()
                })

    traverse(parsed_tree)
    return semantic_elements


def are_semantically_equal(sql1: str, sql2: str, dialect="snowflake"):
    """Compare two SQL statements semantically, ignoring comments and whitespace."""
    # Use formatter to standardize both SQL statements (handles casing, formatting, etc.)
    formatter = get_formatter()

    try:
        formatted_sql1 = formatter.format_sql(sql1)
        formatted_sql2 = formatter.format_sql(sql2)

        # Parse formatted SQL to extract semantic structure
        config = FluffConfig(overrides={"dialect": dialect})
        linter = Linter(config=config)

        parsed1 = linter.parse_string(formatted_sql1)
        parsed2 = linter.parse_string(formatted_sql2)

        if not parsed1.tree or not parsed2.tree:
            return formatted_sql1.strip() == formatted_sql2.strip()

        semantic1 = get_semantic_structure(parsed1.tree)
        semantic2 = get_semantic_structure(parsed2.tree)

        return semantic1 == semantic2
    except (AttributeError, TypeError, ValueError):
        # If parsing fails, fall back to formatted string comparison
        try:
            formatted_sql1 = formatter.format_sql(sql1)
            formatted_sql2 = formatter.format_sql(sql2)
            click.echo(
                "Warning: SQL parsing failed, falling back to formatted string comparison.")
            return formatted_sql1.strip() == formatted_sql2.strip()
        except (AttributeError, TypeError, ValueError):
            # If formatting also fails, use simple comparison
            click.echo(
                "Warning: SQL formatting failed, falling back to simple string comparison.")
            return sql1.strip() == sql2.strip()


def get_objects_from_files(db_name: str, file_paths: list[Path]) -> list[tuple[str, str]]:
    """
    Parses a list of SQL files to get the object type and fully qualified name for each.
    """
    object_identifiers = []
    for file_path in file_paths:
        try:
            file_sql = file_path.read_text()
            obj_type, obj_name = get_db_object_details(file_sql)
            if len(obj_name.split('.')) < 2:
                click.echo(
                    f"Warning: Cannot determine full object name for {file_path}. Skipping.")
                continue
            if len(obj_name.split('.')) == 2:
                obj_name = f'"{db_name}".{obj_name}'
            object_identifiers.append((obj_type, obj_name))
        except (ValueError, IOError) as e:
            click.echo(
                f"Warning: Could not parse object from {file_path}: {e}")
    return object_identifiers


def get_db_object_details(sql_text: str, dialect="snowflake"):
    """Parses SQL text to find the name and type of the created object."""
    config = FluffConfig(overrides={"dialect": dialect})
    linter = Linter(config=config)
    parsed = linter.parse_string(sql_text)

    if parsed.tree:
        for statement in parsed.tree.recursive_crawl('statement'):
            # Check for table creation
            create_table = next(statement.recursive_crawl(
                'create_table_statement'), None)
            if create_table:
                obj_ref = next(create_table.recursive_crawl(
                    'table_reference'), None)
                if obj_ref:
                    return 'TABLE', obj_ref.raw

            # Check for view creation
            create_view = next(statement.recursive_crawl(
                'create_view_statement'), None)
            if create_view:
                obj_ref = next(create_view.recursive_crawl(
                    'table_reference'), None)
                if obj_ref:
                    return 'VIEW', obj_ref.raw

            # Check for procedure creation
            create_proc = next(statement.recursive_crawl(
                'create_procedure_statement'), None)
            if create_proc:
                obj_ref = next(create_proc.recursive_crawl(
                    'function_name'), None)
                if obj_ref:
                    return 'PROCEDURE', obj_ref.raw

            # Check for function creation
            create_func = next(statement.recursive_crawl(
                'create_function_statement'), None)
            if create_func:
                obj_ref = next(create_func.recursive_crawl(
                    'function_name'), None)
                if obj_ref:
                    return 'FUNCTION', obj_ref.raw

    raise ValueError(
        "Could not find a supported CREATE statement in the file.")


def get_semantic_changed_files(ordered_files: list[tuple[str, Path]], db_objects: list[SnowflakeObject], scripts_path: Path) -> list[Path]:
    changed_files: list[Path] = []
    db_ddls = {obj.schema_qualified_name.upper(): obj.ddl for obj in db_objects}
    for (obj_name, file_path) in ordered_files:
        try:
            file_sql = file_path.read_text()
            db_sql = db_ddls.get(obj_name.upper())
            is_different, reason = semantic_diff(file_sql, db_sql)

            if is_different:
                changed_files.append(file_path)
                click.echo(
                    f"  - CHANGE DETECTED ({reason}): {file_path.relative_to(scripts_path)}")
        except (ValueError, IOError) as e:
            click.echo(f"Warning: Could not process {file_path}: {e}")
    return changed_files


def semantic_diff(file_sql: str, db_sql: str | None):
    """
    Compares a local SQL file definition with the corresponding object in Snowflake.
    Returns a tuple of (bool, str) indicating (is_different, reason).
    If db_sql is provided, it is used as the database DDL instead of fetching it.
    """
    try:
        obj_type, obj_name = get_db_object_details(file_sql)

        # If db_sql is not provided, fetch it from the database
        if db_sql is None:
            return True, f"{obj_type} '{obj_name}' does not exist in DB"

        # Semantic comparison ignoring comments and whitespace
        if not are_semantically_equal(db_sql, file_sql):
            return True, "SQL mismatch"

        return False, "In sync"

    except ValueError:
        # File doesn't contain a valid CREATE statement.
        return False, "Not a creatable object"
