# FileScan

FileScan walks one or more directories, records new and changed files in a local SQLite database, and marks previously recorded files as deleted when they are no longer found during a complete scan.

## Requirements

- Go 1.26.6 or newer to build or run from source.
- Docker to use the provided `build.sh` script.

## Build

From this directory, build and run directly with Go:

```sh
go build -o bin/filescan .
./bin/filescan --help
```

Or run without producing a binary:

```sh
go run . --help
```

The Docker build script produces a Linux amd64 binary at `filescan/bin/filescan` (when run from the repository root):

```sh
./filescan/build.sh
```

## Scan directories

Start a scan by providing at least one directory. You can specify multiple directories with repeated `-D` flags or a comma-separated value:

```sh
./bin/filescan scan start -D /data/archive
./bin/filescan scan start -D /data/archive -D /data/another-archive
./bin/filescan scan start --directories /data/archive,/data/another-archive
```

To exclude entries, use `-i`/`--ignore`. Patterns are matched against both an entry's name and its full path using Go's `filepath.Match` glob syntax. A trailing slash limits a pattern to directories:

```sh
./bin/filescan scan start -D /data/archive \
  -i '*.tmp' \
  -i 'cache/'
```

You can combine multiple directories and ignore patterns in one scan:

```sh
./bin/filescan scan start \
  -D /data/archive \
  -D /data/another-archive \
  -i '*.tmp' \
  -i 'cache/'
```

The scanner skips symbolic links. It uses four directory walkers and four file processors by default. Walkers enumerate directories; processors inspect files and hash new or changed content. Adjust either pool to suit the machine and the storage being scanned:

```sh
./bin/filescan scan start -D /data/archive --walkers 8 --processors 6
```

For a starting point, leave the defaults in place, then increase one setting at a time and compare scan duration and system load. More walkers can help when traversing many directories, while more processors can help when processing many files. Hashing reads file contents, so processor concurrency is often limited by storage throughput, especially on network or slower disks; too many workers can add contention without speeding up a scan. Keep both values at least 1, and tune separately for each host and storage environment.

Run `./bin/filescan scan start --help` for command-specific options. Global options include `--profile` (`-p`) and `--log-level` (`-l`, one of `debug`, `info`, `warn`, or `error`). For example:

```sh
./bin/filescan --profile staging --log-level debug scan start -D /data/archive
```

## File records and storage

For each new or changed file, FileScan stores its path, size, modification time, MD5 and SHA-256 checksums, dataset UUID, and version. A directory name in the file's path must contain a 32-character lowercase hexadecimal sequence for the file to be recorded; the filename itself is not used to find the dataset UUID. For example:

```text
/data/archive/0123456789abcdef0123456789abcdef/image.tif
```

Unchanged files are skipped based on their size and modification time. Keep directory paths consistent between runs, since the path itself is used to identify file records.

The database is created automatically at:

```text
~/.local/share/filescan/filescan-<profile>.db
```

The default profile is `default`. Log files are written to `~/.local/share/filescan/logs/` as well as printed to the console.

At the end of a completed scan, previously recorded files under each scanned directory that were not encountered are marked deleted. Ignored paths are not encountered, so adding an ignore pattern for a previously indexed file can cause it to be marked deleted. A cancelled scan does not perform deletion detection. Scanning only the directories that should be reconciled helps avoid unintended deletion marks.
