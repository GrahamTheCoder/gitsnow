import sqlfluff
import snowflake.connector
from pathlib import Path
from sqlfluff.core import Linter, FluffConfig
import re

from .format import format_sql
from .db import get_ddl

def get_db_object_details(sql_text: str, dialect="snowflake"):
    """Parses SQL text to find the name and type of the created object."""
    config = FluffConfig(overrides={"dialect": dialect})
    linter = Linter(config=config)
    parsed = linter.parse_string(sql_text)

    if parsed.tree:
        for statement in parsed.tree.recursive_crawl('statement'):
            # Check for table creation
            create_table = next(statement.recursive_crawl('create_table_statement'), None)
            if create_table:
                obj_ref = next(create_table.recursive_crawl('table_reference'), None)
                if obj_ref:
                    return 'TABLE', obj_ref.raw

            # Check for view creation
            create_view = next(statement.recursive_crawl('create_view_statement'), None)
            if create_view:
                obj_ref = next(create_view.recursive_crawl('table_reference'), None)
                if obj_ref:
                    return 'VIEW', obj_ref.raw

            # Check for procedure creation
            create_proc = next(statement.recursive_crawl('create_procedure_statement'), None)
            if create_proc:
                obj_ref = next(create_proc.recursive_crawl('function_name'), None)
                if obj_ref:
                    return 'PROCEDURE', obj_ref.raw

            # Check for function creation
            create_func = next(statement.recursive_crawl('create_function_statement'), None)
            if create_func:
                obj_ref = next(create_func.recursive_crawl('function_name'), None)
                if obj_ref:
                    return 'FUNCTION', obj_ref.raw

    raise ValueError("Could not find a supported CREATE statement in the file.")


def compare_file_to_db(file_path: Path, conn: snowflake.connector.SnowflakeConnection):
    """
    Compares a local SQL file definition with the corresponding object in Snowflake.
    Returns a tuple of (bool, str) indicating (is_different, reason).
    """
    try:
        file_sql = file_path.read_text()
        obj_type, obj_name = get_db_object_details(file_sql)

        with conn.cursor() as cursor:
            db_sql = get_ddl(cursor, obj_type, obj_name)

        if db_sql.startswith("-- DDL for") or db_sql.startswith("-- Failed to get DDL"):
            return True, "Object does not exist in DB"

        if obj_type == 'TABLE':
            db_sql = re.sub('create or replace table', 'create or alter table', db_sql, flags=re.IGNORECASE)

        # Unquote identifiers before formatting
        unquoted_db_sql = re.sub(r'\"([A-Z_][A-Z0-9_$]*)\"', r'\1', db_sql)

        # Semantic comparison by formatting both strings
        formatted_file_sql = format_sql(file_sql)
        formatted_db_sql = format_sql(unquoted_db_sql)

        if formatted_file_sql != formatted_db_sql:
            return True, "Schema mismatch"

        return False, "In sync"

    except ValueError:
        # File doesn't contain a valid CREATE statement.
        return False, "Not a creatable object"
    except snowflake.connector.errors.ProgrammingError:
        # This can happen if the object doesn't exist, which we treat as a difference.
        return True, "Object does not exist in DB"
    except Exception as e:
        # For other errors, assume a difference to be safe.
        return True, f"Error during comparison: {e}"
