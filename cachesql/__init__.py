__version__ = "0.2.0"
import logging

from .sql import Database

logging.getLogger(__name__).addHandler(logging.NullHandler())
