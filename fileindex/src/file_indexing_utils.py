"""
file_indexing_utils.py

Shared utility class for the file-management-service indexing pipeline,
covering two independent concerns:

1. App switching (HuBMAP vs SenNet) - the Cypher queries, dataset-record
   parsing, UBKG context, and ES document identity fields all differ by app.
   FileIndexingUtils binds the correct implementation for each at construction
   time (based on the `app` argument), so callers never branch on app
   themselves - e.g. `utils.parse_dataset_record(record)` always does the
   right thing once the object is constructed.

2. Slack notification (webhook vs bot-token) - unrelated to which app is
   running. Whichever style has its config keys populated is used; this is
   detected from config, not from `app`, so either app can use either style.
   See the [Slack] section of config.ini.
"""
import ast
import logging
import os
import socket
import sys
from configparser import ConfigParser
from typing import Optional

import requests

logger = logging.getLogger(__name__)


# ======================================================================================
# App-specific Cypher queries
#
# Each query accepts a $dataset_uuids list parameter; an empty list means "no filter,
# return every matching dataset" (used for a full index run), a non-empty list restricts
# to those specific datasets (used by -u/--uuid-filepath targeted re-indexing).
# ======================================================================================

_HUBMAP_PUBLISHED_QUERY = """
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

_HUBMAP_QA_QUERY = """
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

# SenNet's existing queries, unchanged - kept here only so both apps' Cypher lives in
# one place. apoc.convert.fromJsonMap/fromJsonList runs inline, so SN records arrive
# pre-parsed (see _parse_sennet_record below) - unlike HuBMAP's queries, which return
# raw metadata_str/files_str strings specifically to avoid one malformed dataset's JSON
# aborting the whole Neo4j session (Neo.ClientError.Procedure.ProcedureCallFailed).
_SENNET_PUBLISHED_QUERY = """
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

_SENNET_QA_QUERY = """
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
    def __init__(self, app: str, config_file_name: str, disable_slack_notifications: bool = False):
        self.app = app
        self.slack_notification_disabled = disable_slack_notifications

        if not os.path.exists(config_file_name):
            raise FileNotFoundError(f"Unable to find configuration file {config_file_name}.")

        config = ConfigParser()
        config.read(config_file_name)

        self._load_slack_config(config)
        self._bind_app_specifics(app)

        print(f"FileIndexingUtils instantiated for app={app!r}, config={config_file_name}.")

    # ==================================================================================
    # App switching
    # ==================================================================================

    def _bind_app_specifics(self, app: str):
        """Bind the app-specific query strings and method implementations as instance
        attributes, once, at construction - so every other method on this object (and
        every caller of it) can just use self.published_query / self.parse_dataset_record
        / etc. without ever checking which app is active."""
        if app == "hubmap":
            self.published_query = _HUBMAP_PUBLISHED_QUERY
            self.qa_query = _HUBMAP_QA_QUERY
            self.ubkg_application_context = "hubmap"
            self.ubkg_organ_key = "rui_code"
            self.parse_dataset_record = self._parse_hubmap_record
            self.build_identity_fields = self._build_hubmap_identity_fields
        elif app == "sennet":
            self.published_query = _SENNET_PUBLISHED_QUERY
            self.qa_query = _SENNET_QA_QUERY
            self.ubkg_application_context = "sennet"
            self.ubkg_organ_key = "organ_uberon"
            self.parse_dataset_record = self._parse_sennet_record
            self.build_identity_fields = self._build_sennet_identity_fields
        else:
            raise ValueError(f"Unknown app: {app!r} (expected 'hubmap' or 'sennet')")

    def _parse_hubmap_record(self, record) -> Optional[dict]:
        """Parse a raw Neo4j dataset record, converting metadata_str/files_str from
        strings to Python objects. Returns None if the record cannot be parsed,
        letting the caller skip just this one dataset rather than abort the whole run.

        metadata_str/files_str can legitimately come back as None from Neo4j (confirmed
        against real data - e.g. a Published Stanford TMC CODEX dataset) as well as the
        more common "[]"/"{}" empty-but-present string. Both must fall through to an
        empty dict/list rather than being handed to ast.literal_eval, which raises
        TypeError on None."""
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
        """SenNet's Cypher parses metadata/files inline via apoc.convert.fromJsonMap/
        fromJsonList, so the record is already in its final shape - no further parsing
        needed. Always succeeds (no per-record failure mode to guard against here)."""
        return dict(record)

    def _build_hubmap_identity_fields(self, dataset: dict) -> dict:
        """Return the app-specific identity fields to merge into an ES document."""
        return {"dataset_hubmap_id": dataset["hubmap_id"], "donors": dataset["donors"]}

    def _build_sennet_identity_fields(self, dataset: dict) -> dict:
        return {"dataset_sennet_id": dataset["sennet_id"], "sources": dataset["sources"]}

    # ==================================================================================
    # Slack notification - webhook vs bot-token, selected by which config keys are
    # populated, independent of which app is running.
    # ==================================================================================

    def _load_slack_config(self, config: ConfigParser):
        self.slack_webhook_url = config.get("Slack", "SLACK_WEBHOOK_URL", fallback="").strip()
        self.slack_channel_token = config.get("Slack", "SLACK_CHANNEL_TOKEN", fallback="").strip()
        supported = config.get("Slack", "SLACK_SUPPORTED_CHANNELS", fallback="").strip()
        self.slack_supported_channels = [c.strip() for c in supported.split(",") if c.strip()]
        self.slack_notification_channel = config.get("Slack", "SLACK_NOTIFICATION_CHANNEL", fallback="")
        self.slack_max_msg_length = int(config.get("Slack", "SLACK_MAX_MSG_LENGTH", fallback="2048"))
        raw_mentions = config.get("Slack", "SLACK_USER_ID_MENTIONS_ON_ERROR", fallback="{}")
        
        try:
            self.slack_error_mentions = ast.literal_eval(raw_mentions)
        except Exception as e:
            logger.error(f"Skipping @mentions for errors posted to Slack."
                         f" Unable to configure SLACK_USER_ID_MENTIONS_ON_ERROR due to: {e}")
            self.slack_error_mentions = {}
        if self.slack_channel_token:
            self._notify = self._notify_via_channel_token
        elif self.slack_webhook_url:
            self._notify = self._notify_via_webhook
        else:
            self._notify = self._notify_noop

    def _build_mention_text(self, mentions_dict: Optional[dict]) -> str:
        if not mentions_dict:
            return ""
        mention_text = "\n\nAttn:"
        for _dev_name, slack_user_id in mentions_dict.items():
            mention_text += f" <@{slack_user_id}>"
        return mention_text

    def postToSlackChannel(self, channel: str, msg: str, mentions_dict: dict = None):
        """Post msg to channel, using whichever Slack style is configured. A no-op,
        logged at debug and otherwise silent, if neither style is configured or
        notifications are disabled - callers never need to check first."""
        if self.slack_notification_disabled:
            return
        if not msg:
            raise Exception("The message to post to Slack must be a non-blank string.")
        return self._notify(channel, msg, mentions_dict)

    def _notify_via_channel_token(self, channel: str, msg: str, mentions_dict: Optional[dict]):
        if not channel:
            raise Exception("The Slack channel to post a message to must be specified.")
        if channel not in self.slack_supported_channels:
            raise Exception(f"Channel {channel!r} is not in SLACK_SUPPORTED_CHANNELS.")
        if len(msg) > self.slack_max_msg_length:
            raise Exception(
                f"The message to post to Slack must be {self.slack_max_msg_length} "
                f"characters or less."
            )

        mention_text = self._build_mention_text(mentions_dict)
        json_to_post = {"channel": channel, "text": msg + mention_text}
        logger.debug(f"postToSlackChannel (channel-token) json_to_post={json_to_post}")

        response = requests.post(
            url="https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {self.slack_channel_token}"},
            json=json_to_post,
        )
        if response.status_code != 200:
            raise Exception("Failed to send a request to Slack API")

        result = response.json()
        if not result.get("ok"):
            # https://api.slack.com/methods/chat.postMessage#errors
            raise Exception(result.get("error", "Slack API rejected the request"))

        output = {"channel": channel, "message": msg}
        logger.info(output)
        return output

    def _notify_via_webhook(self, channel: str, msg: str, mentions_dict: Optional[dict]):
        # A webhook URL is tied to one destination at creation time in Slack, so
        # `channel` is informational only here - there's nothing to route or validate.
        mention_text = self._build_mention_text(mentions_dict)
        response = requests.post(self.slack_webhook_url, json={"text": msg + mention_text})
        if response.status_code != 200:
            raise Exception(f"Webhook POST to Slack failed: {response.status_code}")
        output = {"channel": channel, "message": msg}
        logger.info(output)
        return output

    def _notify_noop(self, channel: str, msg: str, mentions_dict: Optional[dict]):
        logger.debug(
            f"postToSlackChannel called but no Slack style is configured "
            f"(SLACK_CHANNEL_TOKEN/SLACK_WEBHOOK_URL both blank) - skipping. "
            f"channel={channel!r} msg={msg!r}"
        )

    def exit_if_halt_reason(
        self,
        halt_reasons: list,
        slack_channel: Optional[str] = None,
        mentions_dict: Optional[dict] = None,
        process_bad_news_emoji: str = ":bangbang:",
        exit_code: int = 2,
    ):
        """If halt_reasons is non-empty, log each reason, optionally notify Slack, then
        exit the process. Returns silently (no exit) if halt_reasons is empty."""
        if not halt_reasons:
            return
        bad_news = f"{process_bad_news_emoji} Processing halted for the following logged reasons:"
        logger.error("Processing halted for the following reasons:")
        for idx, halt_reason in enumerate(halt_reasons):
            logger.error(f"\t{halt_reason}")
            bad_news += f"\n{idx + 1}) {halt_reason}"
        if slack_channel:
            self.postToSlackChannel(channel=slack_channel, msg=bad_news, mentions_dict=mentions_dict)
        sys.exit(exit_code)

    # ==================================================================================
    # Host / container context - handy to fold into Slack messages so you know which
    # machine/environment a notification came from at a glance.
    # ==================================================================================

    def get_short_hostname(self) -> str:
        """Return the short hostname (no domain suffix), e.g. "dtn03".

        Inside a container the kernel-level hostname is normally the container's own
        (often random) hostname, not the physical/VM host's. Export HOST_HOSTNAME
        (e.g. `-e HOST_HOSTNAME=$(hostname -s)` on `docker run`) to override."""
        override = os.environ.get("HOST_HOSTNAME")
        if override:
            return override.split(".")[0]
        return socket.gethostname().split(".")[0]

    def is_running_in_container(self) -> bool:
        if os.path.exists("/.dockerenv"):
            return True
        try:
            with open("/proc/1/cgroup", "rt") as f:
                content = f.read()
            if any(marker in content for marker in ("docker", "containerd", "kubepods")):
                return True
        except (FileNotFoundError, PermissionError):
            pass
        return False

    def get_slack_host_context(self) -> str:
        """e.g. "dtn03 (bare metal)" or "dtn03 (container)" - handy to append to a
        Slack message so it's clear at a glance where it came from."""
        host = self.get_short_hostname()
        location = "container" if self.is_running_in_container() else "bare metal"
        return f"{host} ({location})"
