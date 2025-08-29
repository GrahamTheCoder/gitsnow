from sqlfluff.core import Linter, FluffConfig
import re


def format_sql(sql: str) -> str:
    """
    Formats a SQL string using sqlfluff.
    """
    try:
        # Instantiate the rule for fix_string
        sql = force_create_or_alter_table(sql)
        config = FluffConfig.from_root()
        linter = Linter(config=config)
        result = linter.lint_string(sql, fix=True)
        fixed_str, _ = result.fix_string()
        fixed_str = fix_dynamic_table_options(fixed_str)
        return fixed_str
    except Exception as e:
        # In case of any formatting errors, return the original sql
        print(f"Warning: Could not format SQL. Error: {e}")
        return sql

# They get a newline before each equals for some reason
def fix_dynamic_table_options(fixed_str):
    return re.sub(r'[\r\n]+=', ' =', fixed_str)

# Tables shouldn't be replaced since it'll nuke their data
def force_create_or_alter_table(script_text: str) -> str:
    script_text = re.sub(
                    r'create\s+(or\s+replace\s+)?((local|global|temp|temporary|volatile|transient)\s+)*table',
                    'create or alter \\2table',
                    script_text,
                    flags=re.IGNORECASE
                )
    
    return script_text