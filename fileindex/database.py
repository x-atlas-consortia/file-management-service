import logging
import os
import sqlite3
from collections import namedtuple

from fileindex.src.timed_step import TimedStep

DBFile = namedtuple("DBFile", ["path", "rel_path", "size", "last_modified_at"])


def _prefix_upper_bound(path_prefix: str) -> str:
    """Return the smallest string strictly greater than every string beginning with
    path_prefix, so that `path >= path_prefix AND path < upper_bound` selects exactly
    the rows that `path LIKE path_prefix || '%'` would - but lets the index on path
    perform a range SEARCH instead of a full SCAN. The upper bound is path_prefix with
    its final character incremented by one code point."""
    return path_prefix[:-1] + chr(ord(path_prefix[-1]) + 1)


class Database:
    def __init__(self, db_path: str, logger: logging.Logger):
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file {db_path} does not exist.")

        self.logger = logger
        self.conn = sqlite3.connect(
            f"file:{db_path}?mode=ro",
            uri=True,
            timeout=5.0,
            check_same_thread=True,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query_files(self, path_prefix: str) -> list[DBFile]:
        # Avoid a "WHERE path LIKE ?" clauses which forces a full scan of the
        # path index on each call.
        # "WHERE path >= ? AND path < ?" selects the same rows directly seeking
        # the dataset's slice of the index instead of scanning every row.
        #
        # ROW_NUMBER() indicates file revisions, with 1 indicating the "latest" file,
        # filtered by the outer SELECT.
        #
        # "Latest" file identify by version ordering alone. version is assigned by
        # insert_files() as MAX(version) + 1 on every insert, so it strictly reflects
        # insert order regardless of what a file's mtime claims.
        # last_modified_at is the file's mtime, which is outside our control. A file
        # whose mtime moves backward (e.g. restored from an earlier backup) would make
        # last_modified_at DESC pick a stale row as "latest."
        # version does not have the vulnerability, and ties in row selection are not
        # possible because (path, version) is the table's primary key.
        upper_bound = _prefix_upper_bound(path_prefix)
        query = """
            SELECT path, size, last_modified_at FROM (
                SELECT path, size, last_modified_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY path
                           ORDER BY version DESC
                       ) AS rn
                FROM files
                WHERE path >= ? AND path < ? AND deleted_at IS NULL
            )
            WHERE rn = 1
        """
        with TimedStep(self.logger, f"SQLite query_files for {path_prefix}"):
            rows = self.conn.execute(query, (path_prefix, upper_bound)).fetchall()

        return [
            DBFile(row[0], os.path.relpath(row[0], path_prefix), row[1], row[2])
            for row in rows
        ]

    def close(self):
        self.conn.close()
