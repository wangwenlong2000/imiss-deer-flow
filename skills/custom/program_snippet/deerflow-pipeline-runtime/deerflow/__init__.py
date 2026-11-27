"""Allow the standalone pipeline runtime to coexist with the backend package."""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
