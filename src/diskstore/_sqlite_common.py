"""Driver-independent SQL helpers shared by the SQLite backends."""

from typing import Any


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: Any) -> str:
    """Format *value* as a SQL literal.

    Used for DDL defaults and PRAGMA values, which cannot be bound as
    parameters.  Equivalent to ``apsw.format_sql_value`` for the types
    diskstore supports.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, bytes):
        return "X'" + value.hex() + "'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(f"cannot format SQL literal: {value!r}")


format_sql_value = quote_literal
"APSW-compatible alias for :func:`quote_literal`."
