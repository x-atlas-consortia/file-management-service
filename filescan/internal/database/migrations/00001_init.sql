CREATE TABLE IF NOT EXISTS datasets (
    uuid TEXT NOT NULL PRIMARY KEY CHECK(length(uuid) = 32 AND uuid GLOB '[0-9a-f]*')
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS files (
    path TEXT NOT NULL CHECK(length(path) > 0),  -- No empty paths
    version INTEGER NOT NULL CHECK(version > 0),  -- Version must be positive
    dataset_uuid TEXT NOT NULL CHECK(length(dataset_uuid) = 32 AND dataset_uuid GLOB '[0-9a-f]*'),  -- dataset UUIDs
    sha256 TEXT NOT NULL CHECK(length(sha256) = 64 AND sha256 GLOB '[0-9a-f]*'),  -- SHA-256 checksum validation
    md5 TEXT NOT NULL CHECK(length(md5) = 32 AND md5 GLOB '[0-9a-f]*'),  -- MD5 checksum validation
    size INTEGER NOT NULL CHECK(size >= 0),  -- No negative file sizes
    last_modified_at INTEGER NOT NULL CHECK(last_modified_at > 0),  -- Valid Unix timestamp
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),  -- Track when record was created
    deleted_at INTEGER CHECK(deleted_at > 0),  -- Track when record was deleted
    PRIMARY KEY (path, version),
    FOREIGN KEY (dataset_uuid) REFERENCES datasets(uuid) ON DELETE RESTRICT
) WITHOUT ROWID;
