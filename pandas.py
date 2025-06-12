import sqlite3

class DataFrame:
    def __init__(self, rows, columns=None):
        self.rows = list(rows)
        self.columns = columns or []

    @property
    def empty(self):
        return len(self.rows) == 0


def read_sql(query: str, conn: sqlite3.Connection) -> DataFrame:
    cursor = conn.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return DataFrame(rows, columns)
