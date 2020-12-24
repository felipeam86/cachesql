import logging
from functools import lru_cache, wraps

import sqlparse

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1024)
def normalize_query(query: str, max_length=40000) -> str:
    """Normalize query format with sqlparse

    This function normalize the format of a given query by:

    * Striping spaces from both ends
    * Reindenting the query
    * Replacing tabs with spaces
    * Making all keywords uppercase (e.g. SELECT, FROM, WHERE)
    * Striping comments
    """
    if len(query) > max_length:
        logger.debug(
            "The query is too big and would take too much time to normalize. Sending the query as is"
        )
        return query
    else:
        return sqlparse.format(
            query.strip(),
            reindent=True,
            indent_tabs=False,
            indent_width=4,
            keyword_case="upper",
            strip_comments=True,
        )
