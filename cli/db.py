import os
import toml
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dataclasses import dataclass
import traceback
import re

@dataclass
class SnowflakeObject:
    name: str
    type: str
    ddl: str

def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Establishes a connection to Snowflake using user profile TOML file with JWT authentication."""
    try:
        toml_path = os.path.expanduser("~/.snowflake/connections.toml")
        config = toml.load(toml_path)
        _, conn_info = list(config.items())[0]

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

def get_all_schemas(conn, db_name: str) -> list[str]:
    """Fetches all non-system schemas in a given database."""
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW SCHEMAS IN DATABASE \"{db_name}\"")
        # Filter out system schemas
        return [row[1] for row in cursor if row[1] not in ('INFORMATION_SCHEMA', 'PUBLIC')]



def get_objects_in_schema(conn: snowflake.connector.SnowflakeConnection, db_name: str, schema_name: str, cursor=None) -> list[SnowflakeObject]:
    """Fetches all supported objects (tables, views, procedures, dynamic tables) in a schema using a single SHOW OBJECTS call."""
    objects = []

    def _make_snowflake_object(cursor, kind_label: str, ddl_name: str, simple_name: str):
        ddl = get_ddl(cursor, kind_label, ddl_name)
        if not ddl:
            return None
        return SnowflakeObject(name=simple_name, type=kind_label, ddl=ddl)

    def _get_objects(cursor):
        upper_db = db_name.upper()
        upper_schema = schema_name.upper()
        if upper_db in ("SNOWFLAKE",) or upper_schema in ("INFORMATION_SCHEMA",):
            return

        cursor.execute(f'SHOW OBJECTS IN SCHEMA "{db_name}"."{schema_name}"')
        rows = cursor.fetchall()

        for row in rows:
            simple_name = row[1]
            kind = (row[4] or "").upper()
            full_name = f'"{db_name}"."{schema_name}"."{simple_name}"'
            obj = None

            if kind == "PROCEDURE":
                cursor.execute(f'SHOW PROCEDURES LIKE \'{simple_name}\' IN SCHEMA "{db_name}"."{schema_name}"')
                proc_rows = cursor.fetchall()
                for prow in proc_rows:
                    arg_types = prow[7]
                    ddl_name = f'{full_name}({arg_types})'
                    obj = _make_snowflake_object(cursor, "PROCEDURE", ddl_name, simple_name)
            else:
                obj = _make_snowflake_object(cursor, kind, full_name, simple_name)
            
            if obj:
                objects.append(obj)

    if cursor:
        _get_objects(cursor)
    else:
        with conn.cursor() as cursor:
            _get_objects(cursor)

    return objects

def get_ddl(cursor, obj_type: str, fully_qualified_name: str) -> str | None:
    [db_name, schema_name, simple_name] = fully_qualified_name.replace('"', '').split('.')
    ddl = get_ddl_raw(cursor, obj_type, fully_qualified_name)
    if ddl.startswith("-- Failed to get DDL"):
        print(f"[DDL Permission] Cannot get DDL for {obj_type.lower()}: {fully_qualified_name}\n{ddl}")
        return None
    ddl = _fixup_ddl_and_type(cursor, db_name, schema_name, obj_type, ddl, simple_name)
    return ddl

def get_ddl_raw(cursor, obj_type: str, obj_name: str) -> str:
    """Generic function to get DDL for any object."""
    try:
        cursor.execute(f"SELECT GET_DDL('{obj_type}', '{obj_name}', TRUE)")
        result = cursor.fetchone()
        return result[0] if result else f"-- DDL for {obj_name} could not be retrieved."
    except snowflake.connector.errors.ProgrammingError as e:
        tb = traceback.format_exc()
        return f"-- Failed to get DDL for {obj_name}: {e}\nStack trace:\n{tb}"


def _fixup_ddl_and_type(cursor, db_name: str, schema_name: str, kind_label: str, ddl: str, simple_name: str) -> str:
    """
    Fixes up DDL for Snowflake objects, and for dynamic tables, replaces column list with full definitions from DESCRIBE TABLE.
    """
    # Replace db_name.schema_name (case-insensitive) with schema_name before first '('
    ddl = re.sub(
        rf'(CREATE\s[^(]*){db_name}\.({schema_name}\s*[^(]*\()',
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
                if row[2] == "COLUMN":
                    # row[0]=name, row[1]=type
                    col_type = row[1]
                    # Replace NUMBER(38, 0) with INTEGER
                    col_type = re.sub(r'NUMBER\(38,\s*0\)', 'INTEGER', col_type, flags=re.IGNORECASE)
                    col_defs.append(f'{row[0]} {col_type}')
            full_col_def = ',\n    '.join(col_defs)
            # Replace the column list in the DDL
            ddl = ddl[:match.start(2)] + full_col_def + ddl[match.end(2):]
    return ddl
