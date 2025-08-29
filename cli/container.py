"""Dependency injection container for the gitsnow CLI."""

from pathlib import Path
from typing import Optional

from .format import configure_formatter


class Container:
    """Simple dependency injection container for configuring services."""
    
    def __init__(self):
        self._configured = False
    
    def configure(self, config_path: Optional[Path] = None) -> None:
        """
        Configure all services with the given configuration.
        
        Args:
            config_path: Optional path to configuration directory or file.
                        If None, services will use their default configuration discovery.
        """
        if self._configured:
            return  # Already configured
        
        # Configure the SQL formatter
        configure_formatter(config_path)
        
        self._configured = True
    
    def is_configured(self) -> bool:
        """Check if the container has been configured."""
        return self._configured


# Global container instance
_container: Optional[Container] = None


def get_container() -> Container:
    """Get the global container instance."""
    global _container
    if _container is None:
        _container = Container()
    return _container


def configure_services(config_path: Optional[Path] = None) -> None:
    """Configure all services. This should be called at application startup."""
    get_container().configure(config_path)