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
        # Changed: WHERE path LIKE ? forced a full scan of the path index on every
        # call - confirmed via EXPLAIN QUERY PLAN against the original HuBMAP
        # database, and the single largest cost in the whole nightly pipeline there
        # (~29,000s total across ~11k datasets, a ~1.4s floor per call regardless of
        # dataset size). path >= ? AND path < ? (a half-open range) selects exactly
        # the same rows but lets SQLite seek directly to this dataset's slice of the
        # index instead of scanning every row.
        #
        # ROW_NUMBER() replaces the (path, last_modified_at) IN (SELECT path,
        # MAX(last_modified_at) ... GROUP BY path) form, which returned BOTH rows for
        # a path on a last_modified_at tie and let a downstream dict key collision
        # pick between them nondeterministically. Ties here are broken by version -
        # PRIMARY KEY (path, version) is monotonic, so the higher version is always
        # the genuinely newer row.
        upper_bound = _prefix_upper_bound(path_prefix)
        query = """
            SELECT path, size, last_modified_at FROM (
                SELECT path, size, last_modified_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY path
                           ORDER BY last_modified_at DESC, version DESC
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
