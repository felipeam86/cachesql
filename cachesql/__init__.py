# ruff: noqa: F401
__version__ = "0.2.2"
import logging

from .sql import Database

logging.getLogger(__name__).addHandler(logging.NullHandler())
