from __future__ import annotations

import re

from demo.reset import TABLES
from stack import tools


def schema_tables() -> set[str]:
    sql = (tools.REPO / "db" / "schema.sql").read_text()
    return set(re.findall(r"CREATE TABLE IF NOT EXISTS\s+(\w+)", sql, re.IGNORECASE))


def test_reset_truncates_every_table_in_the_schema():
    """A new table must be added to the reset list, or demos leak state."""
    assert schema_tables() == set(TABLES)


def test_reset_does_not_list_tables_that_do_not_exist():
    assert set(TABLES) <= schema_tables()
