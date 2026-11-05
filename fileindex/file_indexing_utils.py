"""
file_indexing_utils.py

Isolates the HuBMAP-vs-SenNet differences that fileindex.py needs, selected once at
construction time via FileIndexingUtils(app=...). Everything else in fileindex.py -
the SQLite query, UUID API calls, ES bulk update/delete, Slack notification - is
identical for both projects and untouched by this module.

Four things differ by app:

1. The Cypher dataset-discovery query, because the two projects have different graph
   shapes (HuBMAP: Donor -> organ Sample -> Activity -> Dataset; SenNet: Dataset ->
   Activity -> organ Sample -> Activity -> Source).

2. How metadata/files get parsed. HuBMAP's queries return them as raw strings
   (metadata_str/files_str) instead of letting Neo4j's apoc.convert.fromJsonMap/
   fromJsonList parse them inline the way SenNet's queries do. This is deliberate: a
   single dataset with malformed JSON in either field throws
   Neo.ClientError.Procedure.ProcedureCallFailed when parsed inline, which kills the
   entire Neo4j session - not just that one dataset. Returning raw strings and parsing
   them in Python (parse_dataset_record, below) lets one bad dataset be logged and
   skipped instead of aborting the whole run.

3. Field names for conceptually similar data: HuBMAP datasets carry "donors" and a
   human-readable "hubmap_id"; SenNet datasets carry "sources" and a "sennet_id".
   build_identity_fields() returns the right field names and values to merge into an
   Elasticsearch document.

4. UBKG (the shared ontology service) is queried with a different application_context
   per project, and returns organs keyed by a different field per project (HuBMAP:
   rui_code; SenNet: organ_uberon).
"""
import ast
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ======================================================================================
# HuBMAP Cypher queries.
#
# Both accept a $dataset_uuids list parameter; an empty list means "no filter, return
# every matching dataset" (a full index run); a non-empty list restricts to those
# specific datasets (used by -u/--uuid-filepath targeted re-indexing).
#
# NOT (ds)<-[:REVISION_OF]-(:Entity) excludes datasets superseded by a newer revision,
# so an old revision doesn't get indexed alongside its replacement. This is HuBMAP-
# specific - flagged to the SenNet developer as not applicable to SenNet's data model,
# so it is intentionally not present in the SenNet queries below.
#
# metadata_str/files_str are returned as raw strings rather than parsed inline - see
# the module docstring for why. parse_dataset_record() converts them to dict/list.
# ======================================================================================

HUBMAP_PUBLISHED_QUERY = """
    MATCH (donor:Donor)-[:ACTIVITY_INPUT]->(organ_activity:Activity)-[:ACTIVITY_OUTPUT]->(organ:Sample {sample_category:'organ'})-[*]->(a:Activity)-[:ACTIVITY_OUTPUT]->(ds:Dataset)
    WHERE a.creation_action IN ['Create Dataset Activity', 'Central Process', 'Lab Process', 'External Process']
    AND ds.status = 'Published'
    AND NOT (ds)<-[:REVISION_OF]-(:Entity)
    AND ($dataset_uuids = [] OR ds.uuid IN $dataset_uuids)
    RETURN ds.uuid AS uuid, ds.hubmap_id AS hubmap_id, ds.group_name AS group_name,
    ds.status AS status, ds.dataset_type AS dataset_type, ds.data_access_level AS data_access_level,
    ds.contains_human_genetic_sequences AS contains_human_genetic_sequences,
    ds.metadata AS metadata_str, ds.files AS files_str,
    a.creation_action AS creation_action,
    COLLECT(DISTINCT apoc.map.fromValues(['uuid', donor.uuid, 'entity_type', donor.entity_type])) AS donors,
    COLLECT(DISTINCT apoc.map.fromValues(['uuid', donor.uuid, 'code', organ.organ])) AS organs
"""

HUBMAP_QA_QUERY = """
    MATCH (donor:Donor)-[:ACTIVITY_INPUT]->(organ_activity:Activity)-[:ACTIVITY_OUTPUT]->(organ:Sample {sample_category:'organ'})-[*]->(a:Activity)-[:ACTIVITY_OUTPUT]->(ds:Dataset)
    WHERE a.creation_action IN ['Create Dataset Activity', 'Central Process', 'Lab Process', 'External Process']
    AND ds.status IN ['Approval', 'QA', 'Submitted']
    AND NOT (ds)<-[:REVISION_OF]-(:Entity)
    AND ($dataset_uuids = [] OR ds.uuid IN $dataset_uuids)
    RETURN ds.uuid AS uuid, ds.hubmap_id AS hubmap_id, ds.group_name AS group_name,
    ds.status AS status, ds.dataset_type AS dataset_type, ds.data_access_level AS data_access_level,
    ds.contains_human_genetic_sequences AS contains_human_genetic_sequences,
    ds.metadata AS metadata_str, ds.files AS files_str,
    a.creation_action AS creation_action,
    COLLECT(DISTINCT apoc.map.fromValues(['uuid', donor.uuid, 'entity_type', donor.entity_type])) AS donors,
    COLLECT(DISTINCT apoc.map.fromValues(['uuid', donor.uuid, 'code', organ.organ])) AS organs
"""

# SenNet's existing queries, copied verbatim from fileindex.py as it stood before this
# change, so both apps' Cypher lives in one place for comparison. These parse
# metadata/files inline via apoc - that's unchanged, since SenNet's data hasn't been
# reported to hit the malformed-JSON problem the HuBMAP queries above are guarding
# against, so there was no reason to alter SenNet's behavior when adding HuBMAP's.
SENNET_PUBLISHED_QUERY = """
    MATCH (d:Dataset)-[:WAS_GENERATED_BY]->(a:Activity)-[:USED|WAS_GENERATED_BY*..16]->(o:Sample{sample_category:'Organ'})-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(s:Source)
    WHERE a.creation_action IN ['Create Dataset Activity', 'Central Process']
    AND d.status = 'Published'
    AND ($dataset_uuids = [] OR d.uuid IN $dataset_uuids)
    WITH DISTINCT d, a, o, s
    WITH d, a,
    COLLECT(DISTINCT apoc.map.fromValues(['uuid', o.uuid, 'code', o.organ])) AS organs,
    COLLECT(DISTINCT apoc.map.fromValues(['uuid', s.uuid, 'source_type', s.source_type])) AS sources
    RETURN d.uuid AS uuid, d.sennet_id AS sennet_id, d.group_name AS group_name,
    d.status AS status, d.dataset_type AS dataset_type, d.data_access_level AS data_access_level,
    d.contains_human_genetic_sequences AS contains_human_genetic_sequences,
    apoc.convert.fromJsonMap(d.metadata) AS metadata, apoc.convert.fromJsonList(d.files) AS files,
    a.creation_action AS creation_action, sources, organs
"""

SENNET_QA_QUERY = """
    MATCH (d:Dataset)-[:WAS_GENERATED_BY]->(a:Activity)-[:USED|WAS_GENERATED_BY*..16]->(o:Sample{sample_category:'Organ'})-[:WAS_GENERATED_BY]->(:Activity)-[:USED]->(s:Source)
    WHERE a.creation_action IN ['Create Dataset Activity', 'Central Process']
    AND d.status IN ['Approval', 'QA', 'Submitted']
    AND ($dataset_uuids = [] OR d.uuid IN $dataset_uuids)
    WITH DISTINCT d, a, o, s
    WITH d, a,
    COLLECT(DISTINCT apoc.map.fromValues(['uuid', o.uuid, 'code', o.organ])) AS organs,
    COLLECT(DISTINCT apoc.map.fromValues(['uuid', s.uuid, 'source_type', s.source_type])) AS sources
    RETURN d.uuid AS uuid, d.sennet_id AS sennet_id, d.group_name AS group_name,
    d.status AS status, d.dataset_type AS dataset_type, d.data_access_level AS data_access_level,
    d.contains_human_genetic_sequences AS contains_human_genetic_sequences,
    apoc.convert.fromJsonMap(d.metadata) AS metadata, apoc.convert.fromJsonList(d.files) AS files,
    a.creation_action AS creation_action, sources, organs
"""


class FileIndexingUtils:
    """
    Construct once, at startup, with the app this run is indexing for:

        file_indexing_utils = FileIndexingUtils(app=config.app)

    The constructor binds the four app-specific pieces as plain instance attributes/
    methods, so the rest of fileindex.py never branches on app itself - it just reads
    file_indexing_utils.published_query, calls file_indexing_utils.parse_dataset_record(),
    etc., and the right behavior is already selected.
    """

    def __init__(self, app: str):
        if app == "hubmap":
            self.published_query = HUBMAP_PUBLISHED_QUERY
            self.qa_query = HUBMAP_QA_QUERY
            self.ubkg_application_context = "hubmap"
            self.ubkg_organ_key = "rui_code"
            self.parse_dataset_record = self._parse_hubmap_record
            self.build_identity_fields = self._build_hubmap_identity_fields
        elif app == "sennet":
            self.published_query = SENNET_PUBLISHED_QUERY
            self.qa_query = SENNET_QA_QUERY
            self.ubkg_application_context = "sennet"
            self.ubkg_organ_key = "organ_uberon"
            self.parse_dataset_record = self._parse_sennet_record
            self.build_identity_fields = self._build_sennet_identity_fields
        else:
            raise ValueError(f"Unknown app: {app!r} (expected 'hubmap' or 'sennet')")

    def _parse_hubmap_record(self, record) -> Optional[dict]:
        """
        Convert a raw HuBMAP Neo4j record's metadata_str/files_str (plain strings, by
        design - see module docstring) into real dict/list values.

        metadata_str/files_str can legitimately come back as None from Neo4j - confirmed
        against real dev data, where a Published Stanford TMC CODEX dataset came back
        with files_str=None rather than the more common "[]" - as well as the empty-
        but-present "[]"/"{}" string form. Both must fall through to an empty dict/list
        rather than being passed to ast.literal_eval, which raises TypeError on None.

        Returns None if the record can't be parsed at all (e.g. genuinely malformed
        JSON), so the caller can skip just this one dataset and keep going, instead of
        letting one bad record crash the whole run.
        """
        try:
            dataset = dict(record)
            raw_metadata = dataset.pop("metadata_str", None)
            raw_files = dataset.pop("files_str", None)
            dataset["metadata"] = ast.literal_eval(raw_metadata) if raw_metadata else {}
            dataset["files"] = ast.literal_eval(raw_files) if raw_files else []
            return dataset
        except Exception as e:
            uuid = dict(record).get("uuid", "unknown")
            logger.error(f"Skipping dataset {uuid} - failed to parse record: {e}")
            return None

    def _parse_sennet_record(self, record) -> dict:
        """
        SenNet's Cypher parses metadata/files inline via apoc.convert.fromJsonMap/
        fromJsonList, so the record already has its final shape - nothing to do here.
        Always succeeds; there is no per-record failure mode to guard against for
        SenNet the way there is for HuBMAP (see _parse_hubmap_record).
        """
        return dict(record)

    def _build_hubmap_identity_fields(self, dataset: dict) -> dict:
        """The HuBMAP-specific fields to merge into an Elasticsearch document."""
        return {"dataset_hubmap_id": dataset["hubmap_id"], "donors": dataset["donors"]}

    def _build_sennet_identity_fields(self, dataset: dict) -> dict:
        """The SenNet-specific fields to merge into an Elasticsearch document."""
        return {"dataset_sennet_id": dataset["sennet_id"], "sources": dataset["sources"]}
