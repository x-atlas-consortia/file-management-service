import hashlib
import json
import logging
import os
import re
import signal
import sys
import threading
import time
from argparse import ArgumentParser
from collections import namedtuple
from configparser import ConfigParser
from typing import Optional, Union
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from fileindex.database import Database, DBFile
from fileindex.filemanager import FileManager
# New: isolates the HuBMAP-vs-SenNet differences (Cypher, record parsing, UBKG
# context, ES document identity fields). See file_indexing_utils.py's module
# docstring for the full rationale.
from fileindex.src.file_indexing_utils import FileIndexingUtils
from fileindex.src.timed_step import TimedStep
from neo4j import Driver, GraphDatabase
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

tz_utc = ZoneInfo("UTC")
process_utc_start = datetime.now(tz_utc)

Config = namedtuple(
    "Config",
    [
        "app",
        "config_file_path",
        "database",
        "log_id",
        "log_level",
        "elastic_search_url",
        "elastic_search_public_index",
        "elastic_search_private_index",
        "globus_groups_token",
        "globus_public_endpoint_filepath",
        "globus_consortium_endpoint_filepath",
        "globus_protected_endpoint_filepath",
        "neo4j_uri",
        "neo4j_username",
        "neo4j_password",
        "slack_notifications",
        "ingest_api_url",
        "ubkg_url",
        "uuid_api_url",
        "dataset_uuids",
        "force_update",
    ],
)

UUIDFileInfo = namedtuple(
    "UUIDFileInfo", ["path", "md5_checksum", "sha256_checksum", "base_dir", "size"]
)


def parse_config() -> Config:
    parser = ArgumentParser(description="Index file info to Elastic Search.")
    # The SenNet-specific or HuBMAP-specific configuration for this fileindex Python process.
    parser.add_argument(
        "--config", default="config.ini", help="Path to the configuration file (ini format)"
    )
    # The configuration file read by both filescan (Go) and this Python file.
    parser.add_argument(
        "--shared-config",
        default="src/config.ini",
        help="Path to the config shared with filescan (ini format)",
    )
    parser.add_argument(
        "--force-update",
        dest="force_update",
        action="store_true",
        default=False,
        help="Force all datasets to be upserted, ignoring staleness checks",
    )
    parser.add_argument(
        "-u",
        "--uuid-filepath",
        dest="dataset_uuid_filepath",
        help="Path to a file containing dataset UUIDs, one UUID per line",
    )
    args = parser.parse_args()
    dataset_uuids: list[str] = []
    if args.dataset_uuid_filepath:
        if not os.path.isfile(args.dataset_uuid_filepath):
            parser.error(f"UUID file not found: {args.dataset_uuid_filepath}")
        with open(args.dataset_uuid_filepath, "r", encoding="utf-8") as f:
            dataset_uuids = [line.strip() for line in f if line.strip()]

    if not os.path.isfile(args.shared_config):
        parser.error(f"Shared config file not found: {args.shared_config}")

    # Separate ConfigParser for each of the configuration files.
    # N.B. The section named "Default" (capital D only) in the shared configuration
    #      used by Go is NOT Python's special-cased [DEFAULT] section spelled in full caps.
    #      Treated as an ordinary section Python, matched by exact, case-sensitive name.
    shared_c = ConfigParser()
    shared_c.read(args.shared_config)

    c = ConfigParser()
    c.read(args.config)

    # Return a merged configuration from the shared files and arguments.
    # N.B. The FileIndexingUtils class of file_indexing_utils.py loads further configuration from
    #      the app-specific file, to be least disruptive to SenNet's use of this module.
    return Config(
        config_file_path=args.config,
        force_update=args.force_update,
        database=shared_c.get("Default", "DATABASE_FILEPATH"),
        app=c.get("DEFAULT", "APP", fallback="unconfigured"),
        log_id=c.get("Local", "LOG_ID", fallback="default"),
        log_level=c.get("Local", "LOG_LEVEL", fallback="info"),
        elastic_search_url=c.get("ElasticSearch", "ELASTIC_SEARCH_URL"),
        elastic_search_public_index=c.get("ElasticSearch", "ELASTIC_SEARCH_PUBLIC_INDEX"),
        elastic_search_private_index=c.get("ElasticSearch", "ELASTIC_SEARCH_PRIVATE_INDEX"),
        globus_groups_token=c.get("Globus", "GLOBUS_GROUPS_TOKEN"),
        globus_public_endpoint_filepath=c.get("Globus", "GLOBUS_PUBLIC_ENDPOINT_FILEPATH"),
        globus_consortium_endpoint_filepath=c.get("Globus", "GLOBUS_CONSORTIUM_ENDPOINT_FILEPATH"),
        globus_protected_endpoint_filepath=c.get("Globus", "GLOBUS_PROTECTED_ENDPOINT_FILEPATH"),
        neo4j_uri=c.get("Neo4J", "NEO4J_URI"),
        neo4j_username=c.get("Neo4J", "NEO4J_USERNAME"),
        neo4j_password=c.get("Neo4J", "NEO4J_PASSWORD"),
        slack_notifications=c.get("Slack", "SLACK_NOTIFICATIONS", fallback=""),
        ingest_api_url=c.get("Service", "INGEST_API_URL"),
        ubkg_url=c.get("Service", "UBKG_URL"),
        uuid_api_url=c.get("Service", "UUID_API_URL"),
        dataset_uuids=dataset_uuids,
    )
config = parse_config()

log_file = os.path.join(
        "logs", f"es-file-index-{config.log_id}-{time.strftime('%Y-%m-%d-%H-%M-%S')}.log"
    )

def setup_logger(log_id: str, log_level: str) -> logging.Logger:
    global log_file

    if not os.path.exists("logs"):
        os.makedirs("logs")

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file)],
    )
    return logging.getLogger("es_file_index")


TIMEOUT = 30  # seconds

# A pause between Dataset's for calls which need to be spaced out, like
# the UUID API fetch in index_published_datasets.
UUID_API_PER_DATASET_PAUSE = 0.05

logger = setup_logger(config.log_id, config.log_level)
terminate_event = threading.Event()

# An app-specific instance constructed using the app-specific configuration, to
# minimize branching on config.app itself.
try:
    file_indexing_utils = FileIndexingUtils(
        app=config.app,
        config_file_name=config.config_file_path,
        disable_slack_notifications=(config.slack_notifications == "DISABLED"),
    )
except (ValueError, FileNotFoundError) as e:
    logger.critical(str(e))
    sys.exit(3)

session = Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[408, 429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)

try:
    file_manager = FileManager(
        ingest_api_url=config.ingest_api_url,
        ubkg_url=config.ubkg_url,
        ubkg_application_context=file_indexing_utils.ubkg_application_context,
        token=config.globus_groups_token,
        session=session,
        logger=logger,
    )
except Exception as e:
    logger.critical(f"Failed to initialize FileManager: {e}")
    sys.exit(3)


def get_ubkg_organs() -> list[dict]:
    res = session.get(
        f"{config.ubkg_url}/organs?application_context={file_indexing_utils.ubkg_application_context}",
        timeout=TIMEOUT,
    )
    if res.status_code != 200:
        msg = f"Error fetching UBKG organs: {res.status_code}"
        raise Exception(msg)

    return res.json()


def get_files_from_uuid_api(dataset_uuid: str) -> list[dict]:
    res = session.get(
        f"{config.uuid_api_url}/{dataset_uuid}/files",
        headers={"Authorization": f"Bearer {config.globus_groups_token}"},
        timeout=TIMEOUT,
    )
    if res.status_code >= 200 and res.status_code < 300:
        return res.json()
    elif res.status_code in [303]:
        # When the response is stashed in an AWS S3 Bucket, follow the
        # redirect to retrieve it.
        s3_response = session.get(res.content.decode(), timeout=TIMEOUT)
        if s3_response.status_code >= 200 and s3_response.status_code < 300:
            return s3_response.json()
        else:
            err_msg =   f"Error fetching the large response content from AWS S3 at:" \
                        f" {res.content.decode()}"
    else:
        err_msg = f"HTTP {res.status_code} error fetching files using:" \
                  f" {config.uuid_api_url}/{dataset_uuid}/files"
    raise Exception(err_msg)


def get_dataset_globus_path(dataset: dict) -> str:
    if dataset["contains_human_genetic_sequences"] is True:
        return os.path.join(
            config.globus_protected_endpoint_filepath, dataset["group_name"], dataset["uuid"]
        )

    if dataset["status"] == "Published":
        return os.path.join(config.globus_public_endpoint_filepath, dataset["uuid"])

    return os.path.join(
        config.globus_consortium_endpoint_filepath, dataset["group_name"], dataset["uuid"]
    )


def get_docs_from_es_by_scroll(index: str, dataset_uuid: str, fields: list[str]) -> list[dict]:
    # Stateful retrieval using the ElasticSearch Scroll API.
    # Use get_docs_from_es_stateless() for an alternative which does not open a
    # server-side context per call.
    scroll = "1m"  # save scroll context for 1 minute
    size = 10000
    docs = []

    # initial search with scroll and get id
    search_url = f"{config.elastic_search_url}/{index}/_search?scroll={scroll}"
    query = {
        "_source": fields,
        "size": size,
        "query": {"term": {"dataset_uuid": dataset_uuid}},
    }
    res = session.post(search_url, json=query, timeout=TIMEOUT)
    if res.status_code == 404:
        # Index doesn't exist yet - treat as "no documents" rather than an error.
        # bulk_update_es_indices's write below auto-creates the index on first use.
        return docs
    res.raise_for_status()
    data = res.json()
    scroll_id = data["_scroll_id"]
    hits = data["hits"]["hits"]
    docs.extend(
        {"_id": hit["_id"], **{field: hit["_source"].get(field) for field in fields}}
        for hit in hits
    )

    # scroll until no more hits
    while hits:
        scroll_resp = session.post(
            f"{config.elastic_search_url}/_search/scroll",
            json={"scroll": scroll, "scroll_id": scroll_id},
            timeout=TIMEOUT,
        )
        scroll_resp.raise_for_status()
        scroll_data = scroll_resp.json()
        hits = scroll_data["hits"]["hits"]
        if not hits:
            break
        docs.extend(
            {"_id": hit["_id"], **{field: hit["_source"].get(field) for field in fields}}
            for hit in hits
        )
        scroll_id = scroll_data["_scroll_id"]
        time.sleep(1)

    return docs


def get_docs_from_es_stateless(index: str, dataset_uuid: str, fields: list[str]) -> list[dict]:
    # Stateless retrieval using the ElasticSearch's search_after pagination to
    # avoid server-side context resources and sleep() pacing.
    #
    # Count on the requirement that all files for a Dataset be under that directory for
    # that Dataset, so rel_path.keyword is a valid sort for total ordering, and unique
    # within the result set.
    size = 10000
    docs = []
    search_url = f"{config.elastic_search_url}/{index}/_search"
    base_query = {
        "_source": fields,
        "size": size,
        "query": {"term": {"dataset_uuid.keyword": dataset_uuid}},
        "sort": [{"rel_path.keyword": "asc"}],
    }

    with TimedStep(logger, f"ES search_after for dataset {dataset_uuid} in {index}"):
        search_after = None
        while True:
            query = dict(base_query)
            if search_after is not None:
                query["search_after"] = search_after
            res = session.post(search_url, json=query, timeout=TIMEOUT)
            if res.status_code == 404:
                # Index doesn't exist yet - treat as "no documents" rather than an
                # error. bulk_update_es_indices's write below auto-creates the index
                # on first use.
                break
            res.raise_for_status()
            hits = res.json()["hits"]["hits"]
            if not hits:
                break
            docs.extend(
                {"_id": hit["_id"], **{field: hit["_source"].get(field) for field in fields}}
                for hit in hits
            )
            # Fewer than a full page means this was the last page.
            if len(hits) < size:
                break
            # Page again from the sort value of the last hit. No server-side
            # context, so no scroll-id to carry and no inter-page pause needed.
            search_after = hits[-1]["sort"]

    return docs


def bulk_update_es_indices(
    indices: Union[str, list[str]], upserts: list[dict], deletes: list[dict]
) -> Optional[list[str]]:
    if isinstance(indices, str):
        indices = [indices]

    # bulk update elastic search indices
    updates = [
        f'{{"delete":{{"_id":"{doc["dataset_uuid"]}/{doc["rel_path"]}"}}}}' for doc in deletes
    ]
    updates.extend(
        [
            f'{{"update":{{"_id":"{doc["dataset_uuid"]}/{doc["rel_path"]}"}}}}\n{{"doc":{json.dumps(doc, separators=(",", ":"))},"doc_as_upsert":true}}'
            for doc in upserts
        ]
    )

    # split upserts into chunks to avoid exceeding the request size limit. 100 is arbitrary
    error_msgs = []
    chunk_size = 100
    chunks = [updates[i : i + chunk_size] for i in range(0, len(updates), chunk_size)]
    for chunk in chunks:
        body = "\n".join(chunk) + "\n"
        for index in indices:
            url = f"{config.elastic_search_url}/{index}/_bulk"
            res = session.post(
                url,
                headers={"Content-Type": "application/x-ndjson"},
                data=body,
                timeout=TIMEOUT,
            )
            if res.status_code != 200:
                raise Exception(
                    f"Error indexing documents for dataset in {index}: "
                    f"{res.status_code}, {res.text}"
                )

            res_body = res.json().get("items", [])
            result_values = [item.get("update") for item in res_body if "update" in item]
            msgs = [
                f"{item['_id']}: Update - {item.get('error', {}).get('reason')}"
                for item in result_values
                if item["status"] not in [200, 201]
            ]
            if msgs:
                error_msgs.extend(msgs)

    return error_msgs if error_msgs else None


def delete_by_query_es_indices(indices: Union[str, list[str]], query: dict):
    if isinstance(indices, str):
        indices = [indices]

    for index in indices:
        url = f"{config.elastic_search_url}/{index}/_delete_by_query"
        res = session.post(url, json=query, timeout=TIMEOUT)
        res.raise_for_status()


def bulk_create_file_uuids(
    file_info: Union[list[UUIDFileInfo], tuple[UUIDFileInfo]], parent_uuid: str
):
    # split upserts into chunks to avoid exceeding gateway timeouts
    chunk_size = 1000
    chunks = [file_info[i : i + chunk_size] for i in range(0, len(file_info), chunk_size)]
    error = None
    for idx, chunk in enumerate(chunks):
        res = session.post(
            f"{config.uuid_api_url}/uuid?entity_count={len(chunk)}",
            headers={"Authorization": f"Bearer {config.globus_groups_token}"},
            json={
                "entity_type": "FILE",
                "parent_ids": [parent_uuid],
                "file_info": [item._asdict() for item in chunk],
            },
            timeout=TIMEOUT,
        )
        if res.status_code != 200:
            error = f"Error creating file uuids in uuid-api: {res.status_code}, {res.text}"

        if idx < len(chunks) - 1:
            time.sleep(1)

    if error:
        raise Exception(error)


def generate_checksums(filepath: str) -> tuple[str, str]:
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()
    read_size = 65536
    with open(filepath, "rb") as f:
        while chunk := f.read(read_size):
            md5.update(chunk)
            sha256.update(chunk)

    return md5.hexdigest(), sha256.hexdigest()


def create_file_info(file: DBFile) -> UUIDFileInfo:
    md5_checksum, sha256_checksum = generate_checksums(file.path)

    return UUIDFileInfo(
        path=file.rel_path,
        md5_checksum=md5_checksum,
        sha256_checksum=sha256_checksum,
        base_dir="DATA_UPLOAD",
        size=os.path.getsize(file.path),
    )


def get_organ_hierarchy(organ: dict) -> str:
    if category := organ.get("category"):
        return category["term"]

    return organ["term"]


PUBLIC_INDEX_EXCLUDED_EXTENSIONS = {".fastq", ".fastq.gz", ".bam"}


def _is_stale_published(
    local_file: DBFile,
    es_file: Optional[dict],
    dataset_status: str,
    force_update: bool = False,
) -> bool:
    return (
        force_update
        or es_file is None
        or es_file.get("file_uuid") is None
        or local_file.last_modified_at != es_file["last_modified_at"]
        or local_file.size != es_file["size"]
        or es_file.get("dataset_status") != dataset_status
    )


def index_published_datasets(
    ubkg_organs: dict,
    driver: Driver,
    db: Database,
) -> tuple[list[str], int]:
    num_errors = 0
    dataset_uuids = []
    with driver.session() as neo4j_session:
        # Use the app-specific query loaded from configuration at startup.
        datasets = neo4j_session.run(
            file_indexing_utils.published_query,
            dataset_uuids=config.dataset_uuids,
        )

        for raw_record in datasets:
            if terminate_event.is_set():
                logger.info("Termination signal received, stopping indexing.")
                return dataset_uuids, num_errors

            # Record this Dataset's UUID before attempting to parse, so a parse failure
            # does not look like a deleted dataset during end-of-run stale-dataset cleanup.
            dataset_uuids.append(raw_record["uuid"])

            # Converts the raw Neo4j record into a plain dict, accounting for app-specific
            # formats (e.g. JSON for SenNet, Python strings for HuBMAP.)
            # Logs Datasets which do not parse.
            dataset = file_indexing_utils.parse_dataset_record(raw_record)
            if dataset is None:
                num_errors += 1
                continue

            # add organ label and hierarchy from ubkg to each organ
            organs = [
                dict(
                    {
                        "label": ubkg_organs[o["code"]]["term"],
                        "hierarchy": get_organ_hierarchy(ubkg_organs[o["code"]]),
                    },
                    **o,
                )
                for o in dataset["organs"]
            ]

            dataset_globus_path = get_dataset_globus_path(dataset)

            try:
                # get files in local database for the dataset, filter out files with blank or
                # numeric extensions
                local_filepath_map = {
                    file.rel_path: file
                    for file in db.query_files(dataset_globus_path)
                    if (ext := os.path.splitext(file.path)[1]) and not re.match(r"^\.\d+$", ext)
                }
            except Exception as e:
                logger.error(
                    f"Error fetching files from local database for dataset {dataset['uuid']}: {e}"
                )
                num_errors += 1
                continue

            try:
                # sleep() as needed to pace UUID API usage, logging the timing.
                with TimedStep(logger, f"UUID API pause for dataset {dataset['uuid']}", threshold=0.0):
                    time.sleep(UUID_API_PER_DATASET_PAUSE)

                # get files in uuid-api for the dataset
                uuid_filepath_map = {
                    item["path"]: item
                    for item in get_files_from_uuid_api(dataset_uuid=dataset["uuid"])
                }
            except Exception as e:
                logger.error(
                    f"Error fetching files from UUID API for dataset {dataset['uuid']}: {e}"
                )
                num_errors += 1
                continue

            # log the files that are in uuid-api but not in the local database
            diff_uuid_files = set(uuid_filepath_map.keys()) - set(local_filepath_map.keys())
            if diff_uuid_files:
                logger.warning(
                    f"Files in UUID API but not in local database for dataset {dataset['uuid']}: "
                    f"{', '.join(diff_uuid_files)}"
                )

            try:
                # get files that are in the database but not the uuid-api
                diff_uuid_files = set(local_filepath_map.keys()) - set(uuid_filepath_map.keys())
                file_info = [
                    create_file_info(local_filepath_map[filepath]) for filepath in diff_uuid_files
                ]
            except Exception as e:
                logger.error(
                    "Error creating file info for UUID API creation for dataset "
                    f"{dataset['uuid']}: {e}"
                )
                num_errors += 1
                continue

            if file_info:
                # update the uuid-api with the new files
                try:
                    logger.info(
                        f"Creating file uuids in UUID API for dataset {dataset['uuid']}: "
                        f"{', '.join([f.path for f in file_info])}"
                    )
                    bulk_create_file_uuids(file_info=file_info, parent_uuid=dataset["uuid"])
                except Exception as e:
                    logger.error(
                        f"Error creating file uuids in UUID API for dataset {dataset['uuid']}: {e}"
                    )
                    num_errors += 1
                    continue

                # get updated files in uuid-api for the dataset
                uuid_filepath_map = {
                    item["path"]: item
                    for item in get_files_from_uuid_api(dataset_uuid=dataset["uuid"])
                }

            es_fields = ["rel_path", "last_modified_at", "size", "file_uuid", "dataset_status"]

            for index in [config.elastic_search_private_index, config.elastic_search_public_index]:
                es_deletes = []
                es_upserts = []

                if index == config.elastic_search_public_index:
                    # public index shouldn't have excluded file extensions
                    filtered_local_filepath_map = {
                        rel_path: file
                        for rel_path, file in local_filepath_map.items()
                        if not any(
                            rel_path.endswith(ext) for ext in PUBLIC_INDEX_EXCLUDED_EXTENSIONS
                        )
                    }
                else:
                    filtered_local_filepath_map = local_filepath_map

                try:
                    # get files in this specific elastic search index for the dataset
                    es_filepath_map = {
                        item["rel_path"]: item
                        for item in get_docs_from_es_stateless(
                            index=index,
                            dataset_uuid=dataset["uuid"],
                            fields=es_fields,
                        )
                    }
                except Exception as e:
                    logger.error(
                        f"Error fetching files from Elastic Search index {index} "
                        f"for dataset {dataset['uuid']}: {e}"
                    )
                    num_errors += 1
                    continue

                # delete files in elastic search that are not in the local database
                diff_es_files = set(es_filepath_map.keys()) - set(
                    filtered_local_filepath_map.keys()
                )
                if diff_es_files:
                    logger.info(
                        f"Deleting {len(diff_es_files)} files from ES index {index} "
                        f"for dataset {dataset['uuid']}: {', '.join(diff_es_files)}"
                    )
                    es_deletes.extend(
                        {"dataset_uuid": dataset["uuid"], "rel_path": filepath}
                        for filepath in diff_es_files
                    )

                for idx, (rel_path, local_file) in enumerate(filtered_local_filepath_map.items()):
                    if idx > 0 and idx % 100 == 0:
                        try:
                            # keep-alive ping to Neo4j to prevent timeout
                            neo4j_session.run("RETURN 1")
                        except Exception as e:
                            logger.warning(f"Neo4j keep-alive ping failed: {e}")

                    uuid_file = uuid_filepath_map[rel_path]
                    es_file = es_filepath_map.get(rel_path)

                    if _is_stale_published(
                        local_file,
                        es_file,
                        dataset["status"],
                        config.force_update,
                    ):
                        file_ext = os.path.splitext(local_file.path)[1].lower()
                        additional_info = None
                        try:
                            additional_info = file_manager.get_additional_info(
                                dataset=dataset,
                                path=rel_path,
                            )
                        except Exception as e:
                            logger.error(
                                f"Error fetching file description for {local_file.rel_path} "
                                f"in dataset {dataset['uuid']}: {e}"
                            )
                            num_errors += 1

                        logger.info(
                            f"Upserting file {local_file.rel_path} in dataset {dataset['uuid']} "
                            f"index {index}"
                        )
                        doc = {
                            "sha256_checksum": uuid_file["sha256_checksum"],
                            "md5_checksum": uuid_file["md5_checksum"],
                            "dataset_uuid": dataset["uuid"],
                            "dataset_status": dataset["status"],
                            "dataset_type": dataset["dataset_type"],
                            "data_access_level": dataset["data_access_level"],
                            "file_extension": file_ext,
                            "file_uuid": uuid_file["file_uuid"],
                            "organs": organs,
                            "rel_path": uuid_file["path"],
                            "size": uuid_file["size"],
                            "last_modified_at": local_file.last_modified_at,
                        }
                        # Update the ElasticSearch Document for the Dataset with
                        # the app-specific fields.
                        doc.update(file_indexing_utils.build_identity_fields(dataset))
                        if additional_info is not None:
                            doc.update(additional_info)

                        es_upserts.append(doc)

                if es_upserts or es_deletes:
                    try:
                        err_msgs = bulk_update_es_indices(
                            indices=index, upserts=es_upserts, deletes=es_deletes
                        )
                        if err_msgs:
                            for msg in err_msgs:
                                logger.error(f"Error indexing document: {msg}")
                    except Exception as e:
                        logger.error(
                            f"Error indexing documents for dataset {dataset['uuid']} "
                            f"index {index}: {e}"
                        )
                        num_errors += 1

    return dataset_uuids, num_errors


def index_qa_datasets(ubkg_organs: dict, driver: Driver, db: Database) -> tuple[list[str], int]:
    es_indices = [config.elastic_search_private_index]

    num_errors = 0
    dataset_uuids = []
    with driver.session() as neo4j_session:
        # Use the app-specific query loaded from configuration at startup.
        datasets = neo4j_session.run(
            file_indexing_utils.qa_query,
            dataset_uuids=config.dataset_uuids,
        )

        for raw_record in datasets:
            if terminate_event.is_set():
                logger.info("Termination signal received, stopping indexing.")
                return dataset_uuids, num_errors

            # Record this Dataset's UUID before attempting to parse, so a parse failure
            # does not look like a deleted dataset during end-of-run stale-dataset cleanup.
            dataset_uuids.append(raw_record["uuid"])

            # Converts the raw Neo4j record into a plain dict, accounting for app-specific
            # formats (e.g. JSON for SenNet, Python strings for HuBMAP.)
            # Logs Datasets which do not parse.
            dataset = file_indexing_utils.parse_dataset_record(raw_record)
            if dataset is None:
                num_errors += 1
                continue

            # add organ label and hierarchy from ubkg to each organ
            organs = [
                dict(
                    {
                        "label": ubkg_organs[o["code"]]["term"],
                        "hierarchy": get_organ_hierarchy(ubkg_organs[o["code"]]),
                    },
                    **o,
                )
                for o in dataset["organs"]
            ]

            dataset_globus_path = get_dataset_globus_path(dataset)

            es_deletes = []
            es_upserts = []
            try:
                # get files in local database for the dataset, filter out files with blank or
                # numeric extensions
                local_filepath_map = {
                    file.rel_path: file
                    for file in db.query_files(dataset_globus_path)
                    if (ext := os.path.splitext(file.path)[1]) and not re.match(r"^\.\d+$", ext)
                }
            except Exception as e:
                logger.error(
                    f"Error fetching files from local database for dataset {dataset['uuid']}: {e}"
                )
                num_errors += 1
                continue

            try:
                # get files in elastic search for the dataset
                es_filepath_map = {
                    item["rel_path"]: item
                    for item in get_docs_from_es_stateless(
                        index=config.elastic_search_private_index,
                        dataset_uuid=dataset["uuid"],
                        fields=[
                            "rel_path",
                            "last_modified_at",
                            "size",
                            "md5_checksum",
                            "dataset_status",
                        ],
                    )
                }
            except Exception as e:
                logger.error(
                    f"Error fetching files from Elastic Search for dataset {dataset['uuid']}: {e}"
                )
                num_errors += 1
                continue

            # delete files in elastic search that are not in the local database
            diff_es_files = set(es_filepath_map.keys()) - set(local_filepath_map.keys())
            if diff_es_files:
                logger.info(
                    f"Deleting {len(diff_es_files)} files from ES for dataset {dataset['uuid']}: "
                    f"{', '.join(diff_es_files)}"
                )
                es_deletes.extend(
                    [
                        {
                            "dataset_uuid": dataset["uuid"],
                            "rel_path": filepath,
                        }
                        for filepath in diff_es_files
                    ]
                )

            for idx, (rel_path, local_file) in enumerate(local_filepath_map.items()):
                if idx > 0 and idx % 100 == 0:
                    try:
                        # keep-alive ping to Neo4j to prevent timeout
                        neo4j_session.run("RETURN 1")
                    except Exception as e:
                        logger.warning(f"Neo4j keep-alive ping failed: {e}")

                # check if the file is in elastic search
                es_file = es_filepath_map.get(rel_path)
                needs_upsert = (
                    config.force_update
                    or es_file is None
                    or local_file.last_modified_at != es_file["last_modified_at"]
                    or local_file.size != es_file["size"]
                    or es_file.get("dataset_status") != dataset["status"]
                )
                if es_file and es_file.get("file_uuid"):
                    # something has gone wrong if this happens
                    logger.warning(
                        f"File {local_file.rel_path} already has a file_uuid "
                        f"{es_file['file_uuid']} for dataset {dataset['uuid']} in QA indexing."
                    )

                if needs_upsert:
                    # insert or update the file in elastic search
                    file_ext = os.path.splitext(local_file.path)[1].lower()
                    additional_info = None
                    try:
                        additional_info = file_manager.get_additional_info(
                            dataset=dataset,
                            path=rel_path,
                        )
                    except Exception as e:
                        logger.error(
                            f"Error fetching file description for {local_file.rel_path} in dataset "
                            f"{dataset['uuid']}: {e}"
                        )
                        num_errors += 1

                    logger.info(
                        f"Upserting file {local_file.rel_path} in dataset {dataset['uuid']}"
                    )
                    doc = {
                        "dataset_uuid": dataset["uuid"],
                        "dataset_status": dataset["status"],
                        "dataset_type": dataset["dataset_type"],
                        "data_access_level": dataset["data_access_level"],
                        "file_extension": file_ext,
                        "organs": organs,
                        "rel_path": rel_path,
                        "size": local_file.size,
                        "last_modified_at": local_file.last_modified_at,
                    }
                    # Update the ElasticSearch Document for the Dataset with
                    # the app-specific fields.
                    doc.update(file_indexing_utils.build_identity_fields(dataset))
                    if additional_info is not None:
                        doc.update(additional_info)

                    es_upserts.append(doc)

            # bulk update elastic search indices if necessary
            if es_upserts or es_deletes:
                try:
                    err_msgs = bulk_update_es_indices(
                        indices=es_indices, upserts=es_upserts, deletes=es_deletes
                    )
                    if err_msgs:
                        for msg in err_msgs:
                            logger.error(f"Error indexing document: {msg}")
                except Exception as e:
                    logger.error(f"Error indexing documents for dataset {dataset['uuid']}: {e}")
                    num_errors += 1

    return dataset_uuids, num_errors

def post_launch_to_slack():
    if config.app == 'hubmap':
        msg = f":file_folder: {file_indexing_utils.get_slack_host_context()} :file_folder: file-management-service :diamonds: {Path(__file__).name} :file_folder:\n" \
              f"{file_indexing_utils.slack_neutral_info_emoji} Launched to maintain Elasticsearch indices" \
              f" `{config.elastic_search_public_index}` and `{config.elastic_search_private_index}`\n" \
              f" Process logging to {log_file}\n" \
              f":file_folder:"
        try:
            file_indexing_utils.postToSlackChannel(channel=file_indexing_utils.slack_notification_channel
                                                   , msg=msg)
        except Exception as e:
            logger.exception('Unable to post Slack start notification.')

def post_conclusion_to_slack(num_errors: int):
    process_utc_finish = datetime.now(tz_utc)
    if num_errors > 0:
        try:
            if config.app == 'sennet':
                file_indexing_utils.postToSlackChannel(
                    channel=file_indexing_utils.slack_notification_channel,
                    msg=(
                        f"{num_errors} errors occurred during {config.log_id} Elastic Search "
                        "file indexing."
                    ),
                    mentions_dict=file_indexing_utils.slack_error_mentions,
                )
            elif config.app == 'hubmap':
                bad_news = f":file_folder: {file_indexing_utils.get_slack_host_context()} :file_folder: file-management-service :diamonds: {Path(__file__).name} :file_folder:\n" \
                           f"{file_indexing_utils.slack_bad_news_emoji} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}" \
                           f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds." \
                           f" {num_errors} errors occurred during {config.log_id} ElasticSearch file indexing.\n" \
                           f" Process logged to {log_file}\n" \
                           f"{':black_square_button::skull_and_crossbones: ' * 5}\n" \
                           f":file_folder:"
                try:
                    file_indexing_utils.postToSlackChannel(channel=file_indexing_utils.slack_notification_channel
                                                           , msg=bad_news)
                except Exception as e:
                    logger.exception('Unable to post Slack start notification.')
            else:
                logger.error(f"Unrecognized value for config.app={config.app}")
        except Exception as e:
            logger.error(f"Error posting to Slack: {e}")
    else:
        if config.app == 'sennet':
            pass
        elif config.app == 'hubmap':
            good_news = f":file_folder: {file_indexing_utils.get_slack_host_context()} :file_folder: file-management-service :diamonds: {Path(__file__).name} :file_folder:\n" \
                        f"{file_indexing_utils.slack_good_news_emoji} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}" \
                        f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} after" \
                        f" {int((process_utc_finish - process_utc_start).total_seconds() // 60)} minutes.\n" \
                        f" `{config.elastic_search_public_index}` and `{config.elastic_search_private_index}`\n" \
                        f" Process logged to {log_file}\n" \
                        f"{':light_blue_heart: ' * 5}\n" \
                        f":file_folder:"

            try:
                file_indexing_utils.postToSlackChannel(channel=file_indexing_utils.slack_notification_channel
                                                       , msg=good_news
                                                       , mentions_dict=file_indexing_utils.slack_success_mentions)
            except Exception as e:
                logger.exception('Unable to post Slack success notification.')

        else:
            logger.error(f"Unrecognized value for config.app={config.app}")

def main():
    post_launch_to_slack()
    try:
        # Retrieve using the app-specific key
        ubkg_organs = {o[file_indexing_utils.ubkg_organ_key]: o for o in get_ubkg_organs()}
    except Exception as e:
        logger.error(f"Error fetching UBKG organs: {e}")
        sys.exit(3)

    def handle_termination(signum, frame):
        logger.critical(f"Signal {signum} received, terminating...")
        terminate_event.set()

    signal.signal(signal.SIGTERM, handle_termination)
    signal.signal(signal.SIGINT, handle_termination)

    log_level = logger.level
    num_errors = 0
    dataset_uuids = []
    auth = (config.neo4j_username, config.neo4j_password)
    with (
        GraphDatabase.driver(config.neo4j_uri, auth=auth) as driver,
        Database(config.database, logger) as db,
    ):
        driver.verify_connectivity()

        if terminate_event.is_set():
            return

        # index Published datasets
        start_time = time.time()
        uuids, errors = index_published_datasets(ubkg_organs=ubkg_organs, driver=driver, db=db)
        logger.setLevel(logging.INFO)
        logger.info(f"Published dataset indexing took {time.time() - start_time:.2f} seconds")
        logger.setLevel(log_level)
        num_errors += errors
        dataset_uuids.extend(uuids)

        if terminate_event.is_set():
            return

        # index QA datasets
        start_time = time.time()
        uuids, errors = index_qa_datasets(ubkg_organs=ubkg_organs, driver=driver, db=db)
        logger.setLevel(logging.INFO)
        logger.info(f"QA dataset indexing took {time.time() - start_time:.2f} seconds")
        logger.setLevel(log_level)
        num_errors += errors
        dataset_uuids.extend(uuids)

    if terminate_event.is_set():
        return

    if not config.dataset_uuids:
        # Only delete stale datasets when running a full index. If specific UUIDs were
        # provided, dataset_uuids is a subset of all indexed datasets, so deleting
        # everything outside that subset would wipe out valid documents.
        try:
            es_indices = [config.elastic_search_public_index, config.elastic_search_private_index]
            query = {"query": {"bool": {"must_not": {"terms": {"dataset_uuid": dataset_uuids}}}}}
            delete_by_query_es_indices(indices=es_indices, query=query)
        except Exception as e:
            logger.error(f"Error deleting documents from Elasticsearch: {e}")
            num_errors += 1

    if terminate_event.is_set():
        return

    logger.info(f"Posting Slack message based upon {config.log_id} {config.app} ending with {num_errors} errors.")
    post_conclusion_to_slack(num_errors)

if __name__ == "__main__":
    try:
        main()
    finally:
        logger.setLevel(logging.INFO)
        logger.info("Shutting down and cleaning up.")
        logging.shutdown()
        for handler in logger.handlers:
            handler.close()
        session.close()
