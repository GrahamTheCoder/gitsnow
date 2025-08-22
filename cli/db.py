import os
import toml
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dataclasses import dataclass
import traceback

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
    """Fetches all supported objects (tables, views, procedures) in a schema using a single SHOW OBJECTS call."""
    objects = []

    def _make_snowflake_object(cursor, kind_label: str, ddl_name: str, simple_name: str):
        ddl = get_ddl(cursor, kind_label, ddl_name)
        if ddl.startswith("-- Failed to get DDL"):
            print(f"[DDL Permission] Cannot get DDL for {kind_label.lower()}: {ddl_name}\n{ddl}")
            return None
        type_map = {
            "TABLE": "tables",
            "VIEW": "views",
            "PROCEDURE": "procedures",
        }
        return SnowflakeObject(name=simple_name, type=type_map.get(kind_label, kind_label.lower()), ddl=ddl)

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

            if kind in ("TABLE", "VIEW"):
                obj = _make_snowflake_object(cursor, kind, full_name, simple_name)
            elif kind == "PROCEDURE":
                cursor.execute(f'SHOW PROCEDURES LIKE \'{simple_name}\' IN SCHEMA "{db_name}"."{schema_name}"')
                proc_rows = cursor.fetchall()
                for prow in proc_rows:
                    arg_types = prow[7]
                    ddl_name = f'{full_name}({arg_types})'
                    obj = _make_snowflake_object(cursor, "PROCEDURE", ddl_name, simple_name)
            
            if obj:
                objects.append(obj)

    if cursor:
        _get_objects(cursor)
    else:
        with conn.cursor() as cursor:
            _get_objects(cursor)

    return objects

def get_ddl(cursor, obj_type: str, obj_name: str) -> str:
    """Generic function to get DDL for any object."""
    try:
        cursor.execute(f"SELECT GET_DDL('{obj_type}', '{obj_name}')")
        result = cursor.fetchone()
        return result[0] if result else f"-- DDL for {obj_name} could not be retrieved."
    except snowflake.connector.errors.ProgrammingError as e:
        tb = traceback.format_exc()
        return f"-- Failed to get DDL for {obj_name}: {e}\nStack trace:\n{tb}"
