import os
from snowflake.connector.cursor import SnowflakeCursor
import toml
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dataclasses import dataclass
import traceback
import re

@dataclass
class SnowflakeObject:
    name: str
    schema: str
    type: str
    ddl: str

    @property
    def schema_qualified_name(self) -> str:
        return f'{self.schema}.{self.name}'

def get_connection(db_name: str) -> snowflake.connector.SnowflakeConnection:
    """Establishes a connection to Snowflake using user profile TOML file with JWT authentication."""
    try:
        toml_path = os.path.expanduser("~/.snowflake/connections.toml")
        config = toml.load(toml_path)
        # Find the connection config where the key ends with 'gitsnow'
        connection_name = f"{db_name}__GITSNOW".upper()
        conn_info = config.get(connection_name)
        if conn_info is None:
            raise KeyError(f"Could not find a connection profile in ~/.snowflake/connections.toml with the name '{connection_name}'")

        # Load private key
        with open(conn_info["private_key_path"], "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=conn_info["private_key_file_pwd"].encode(),
            )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return snowflake.connector.connect(
            user=conn_info["user"],
            account=conn_info["account"],
            warehouse=conn_info["warehouse"],
            authenticator=conn_info["authenticator"],
            role=conn_info["role"],
            private_key=pkb,
        )
    except KeyError as e:
        tb = traceback.format_exc()
        raise ConnectionError(f"Missing TOML config value: {e}\nStack trace:\n{tb}")
    except FileNotFoundError:
        tb = traceback.format_exc()
        raise ConnectionError(f"TOML file not found at {toml_path}\nStack trace:\n{tb}")
    except Exception as e:
        tb = traceback.format_exc()
        raise ConnectionError(f"Failed to connect: {e}\nStack trace:\n{tb}")

def get_all_schemas(conn: snowflake.connector.SnowflakeConnection, db_name: str) -> list[str]:
    """Fetches all non-system schemas in a given database."""
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW SCHEMAS IN DATABASE \"{db_name}\"")
        # Filter out system schemas
        return [row[1] for row in cursor if row[1] not in ('INFORMATION_SCHEMA', 'PUBLIC')]



def get_objects_in_schema(conn: snowflake.connector.SnowflakeConnection, db_name: str, schema_name: str, cursor=None) -> list[SnowflakeObject]:
    """Fetches all supported objects in a schema including functions, procedures, streams, and tasks.

    This implementation first collects fully-qualified object names and their types, then
    requests all DDLs in a single batched query via get_all_ddls, and finally constructs
    SnowflakeObject instances from the batch result. This reduces round-trips for many objects.
    """

    candidates: list[tuple[str, str, str]] = []  # list of (object_type, fully_qualified_name, simple_name)
    results: list[SnowflakeObject] = []

    def _collect_from_show_command(cur, show_command: str, object_type: str, name_column_index: int = 1, args_column_index: int | None = None):
        try:
            cur.execute(show_command)
            rows = cur.fetchall()
            for row in rows:
                simple_name = row[name_column_index]
                full_name = f'"{db_name}"."{schema_name}"."{simple_name}"'

                # Procedures sometimes need argument list appended
                if object_type == "PROCEDURE" and args_column_index is not None and len(row) > args_column_index:
                    arg_types = row[args_column_index]
                    if arg_types:
                        ddl_name = f'{full_name}({arg_types})'
                    else:
                        ddl_name = full_name
                else:
                    ddl_name = full_name

                candidates.append((object_type, ddl_name, simple_name))
        except Exception as e:
            print(f"[Warning] Failed to execute {show_command}: {e}")

    def _gather_objects(cur: SnowflakeCursor):
        upper_db = db_name.upper()
        upper_schema = schema_name.upper()
        if upper_db in ("SNOWFLAKE",) or upper_schema in ("INFORMATION_SCHEMA",):
            return

        # SHOW OBJECTS to get common objects (tables, views, etc.)
        try:
            cur.execute(f'SHOW OBJECTS IN SCHEMA "{db_name}"."{schema_name}"')
            rows = cur.fetchall()
            for row in rows:
                simple_name = row[1]
                kind = (row[4] or "").upper()
                if kind == "PROCEDURE":
                    # fetch procedure variants (to get arg signatures)
                    _collect_from_show_command(
                        cur,
                        f'SHOW PROCEDURES LIKE \'{simple_name}\' IN SCHEMA "{db_name}"."{schema_name}"',
                        "PROCEDURE",
                        name_column_index=1,
                        args_column_index=7,
                    )
                else:
                    full_name = f'"{db_name}"."{schema_name}"."{simple_name}"'
                    candidates.append((kind, full_name, simple_name))
        except Exception as e:
            print(f"[Warning] Failed to get objects from SHOW OBJECTS: {e}")

        # Other object types
        _collect_from_show_command(cur, f'SHOW USER FUNCTIONS IN SCHEMA "{db_name}"."{schema_name}"', "FUNCTION")
        _collect_from_show_command(cur, f'SHOW USER PROCEDURES IN SCHEMA "{db_name}"."{schema_name}"', "PROCEDURE", args_column_index=7)
        _collect_from_show_command(cur, f'SHOW STREAMS IN SCHEMA "{db_name}"."{schema_name}"', "STREAM")
        _collect_from_show_command(cur, f'SHOW TASKS IN SCHEMA "{db_name}"."{schema_name}"', "TASK")

    # Use provided cursor or open one
    if cursor:
        _gather_objects(cursor)
    else:
        with conn.cursor() as cur:
            _gather_objects(cur)

    # If nothing to fetch, return empty list
    if not candidates:
        return []

    # Build the list of (type, obj_name) for batch DDL fetch
    to_fetch = [(obj_type, obj_name) for (obj_type, obj_name, _) in candidates]
    ddl_map = get_all_ddls(conn, to_fetch)

    # Construct SnowflakeObject instances from batch results
    for obj_type, obj_name, simple_name in candidates:
        # key format used by get_all_ddls is '{schema}.{simple_name}' (without quotes)
        # extract schema and simple_name from obj_name
        cleaned = obj_name.replace('"', '')
        parts = cleaned.split('.')
        # parts -> [db, schema, simple] or for procedures [db, schema, simple(args)]
        if len(parts) < 3:
            continue
        schema_part = parts[1]
        simple_part = parts[2]
        key = f'{schema_part}.{simple_part}'

        ddl = ddl_map.get(key)
        if not ddl:
            # skip objects with no accessible DDL
            continue

        results.append(SnowflakeObject(name=simple_name, schema=schema_part, type=obj_type, ddl=ddl))

    return results

def get_all_ddls(conn: snowflake.connector.SnowflakeConnection, objects: list[tuple[str, str]]) -> dict[str, str]:
    """
    Fetches DDL for a list of objects in a single query.
    """
    if not objects:
        return {}

    # Build a UNION ALL query to fetch all DDLs at once
    union_queries = []
    for obj_type, obj_name in objects:
        union_queries.append(f"SELECT '{obj_name}' as obj_name, GET_DDL('{obj_type}', '{obj_name}', TRUE) as ddl")

    full_query = "\nUNION ALL\n".join(union_queries)

    with conn.cursor() as cursor:
        try:
            cursor.execute(full_query)
            rows = cursor.fetchall()

            ddl_map = {}
            for row in rows:
                obj_name, ddl = row
                if ddl and not ddl.startswith("-- Failed to get DDL"):
                    [db_name, schema_name, simple_name] = obj_name.replace('"', '').split('.')
                    ddl = _fixup_ddl_and_type(cursor, db_name, schema_name, "UNKNOWN", ddl, simple_name)
                    ddl_map[f'{schema_name}.{simple_name}'] = ddl
            return ddl_map
        except snowflake.connector.errors.ProgrammingError as e:
            tb = traceback.format_exc()
            print(f"-- Failed to execute batch DDL query: {e}\nStack trace:\n{tb}")
            return {}

def _fixup_ddl_and_type(cursor: SnowflakeCursor, db_name: str, schema_name: str, kind_label: str, ddl: str, simple_name: str) -> str:
    """
    Fixes up DDL for Snowflake objects, and for dynamic tables, replaces column list with full definitions from DESCRIBE TABLE.
    """
    # Replace db_name.schema_name (case-insensitive) with schema_name before first '('
    ddl = re.sub(
        rf'(CREATE\s[^(]*){db_name}\.({schema_name}\.)',
        r'\1\2',
        ddl,
        flags=re.IGNORECASE
    )

    # If dynamic table, replace column list with full definitions (including types)
    if kind_label.upper() == "TABLE":
        # Find the column list in the DDL
        match = re.search(r'(CREATE\s.*?\()(.*?)(\)\s*TARGET_LAG)', ddl, re.DOTALL | re.IGNORECASE)
        if match:
            # Get full column definitions from DESCRIBE TABLE
            cursor.execute(f'DESCRIBE TABLE "{db_name}"."{schema_name}"."{simple_name}"')
            desc_rows = cursor.fetchall()
            col_defs = []
            for row in desc_rows:
                (col_name, col_type, row_type, is_nullable, _, _, _, _, _, comment) = row[0:10]
                if row_type == "COLUMN":
                    col_type = re.sub(r'NUMBER\(38,\s*0\)', 'INTEGER', col_type, flags=re.IGNORECASE)
                    null_str = " NOT NULL" if is_nullable == "N" else ""
                    comment_str = f" COMMENT '{comment}'" if comment else ""
                    col_defs.append(f'{col_name} {col_type}{null_str}{comment_str}')
            full_col_def = ',\n    '.join(col_defs)
            # Replace the column list in the DDL
            ddl = ddl[:match.start(2)] + full_col_def + ddl[match.end(2):]
    return ddl
