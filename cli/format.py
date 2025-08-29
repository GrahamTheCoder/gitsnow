from sqlfluff.core import Linter, FluffConfig
import re
from pathlib import Path
from typing import Optional


class SqlFormatter:
    """A SQL formatter that can be configured with a custom config path."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the SQL formatter.
        
        Args:
            config_path: Optional path to a custom sqlfluff config file/directory.
                        If None, will use the default from_root() behavior.
        """
        self.config_path = config_path
        self._config = None
        self._linter = None
    
    def _get_config(self) -> FluffConfig:
        """Get the sqlfluff config, creating it if necessary."""
        if self._config is None:
            if self.config_path:
                # If a custom config path is provided, use it
                self._config = FluffConfig.from_root(extra_config_path=str(self.config_path))
            else:
                # Use default behavior (search from current directory)
                self._config = FluffConfig.from_root()
        return self._config
    
    def _get_linter(self) -> Linter:
        """Get the linter, creating it if necessary."""
        if self._linter is None:
            self._linter = Linter(config=self._get_config())
        return self._linter
    
    def format_sql(self, sql: str) -> str:
        """
        Formats a SQL string using sqlfluff.
        
        Args:
            sql: The SQL string to format
            
        Returns:
            The formatted SQL string
        """
        try:
            # Instantiate the rule for fix_string
            sql = self._force_create_or_alter_table(sql)
            linter = self._get_linter()
            result = linter.lint_string(sql, fix=True)
            fixed_str, _ = result.fix_string()
            fixed_str = self._fix_dynamic_table_options(fixed_str)
            return fixed_str
        except Exception as e:
            # In case of any formatting errors, return the original sql
            print(f"Warning: Could not format SQL. Error: {e}")
            return sql
    
    def _fix_dynamic_table_options(self, fixed_str: str) -> str:
        """Fix dynamic table options formatting - they get a newline before each equals for some reason."""
        return re.sub(r'[\r\n]+=', ' =', fixed_str)
    
    def _force_create_or_alter_table(self, script_text: str) -> str:
        """Replace CREATE OR REPLACE TABLE with CREATE OR ALTER TABLE since tables shouldn't be replaced as it'll nuke their data."""
        script_text = re.sub(
            r'create\s+(or\s+replace\s+)?((local|global|temp|temporary|volatile|transient)\s+)*table',
            r'create or alter \2table',
            script_text,
            flags=re.IGNORECASE
        )
        return script_text


# Global formatter instance - will be configured by the DI container
_formatter: Optional[SqlFormatter] = None


def get_formatter() -> SqlFormatter:
    """Get the configured formatter instance."""
    global _formatter
    if _formatter is None:
        # Fallback to default formatter if not configured
        _formatter = SqlFormatter()
    return _formatter


def configure_formatter(config_path: Optional[Path] = None) -> None:
    """Configure the global formatter instance."""
    global _formatter
    _formatter = SqlFormatter(config_path)


# Backward compatibility function
def format_sql(sql: str) -> str:
    """
    Formats a SQL string using the configured formatter.
    This function maintains backward compatibility with existing code.
    """
    return get_formatter().format_sql(sql)