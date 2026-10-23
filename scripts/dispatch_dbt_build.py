"""Start one dbt build after a verified incremental workflow succeeds.

The EventBridge rule receives all successful Step Functions executions. This
handler inspects the execution output, ignores no-work/duplicate runs, and
starts a build only after the batch ledger reports COMPLETED. A project-level
CodeBuild concurrency limit is a second safety layer; a running build already
sees the latest Iceberg source state, so skipping a duplicate trigger is safe.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

RUNNING_STATES = {"IN_PROGRESS"}


def _execution_output(event: dict[str, Any]) -> dict[str, Any]:
    detail = event.get("detail", {})
    raw_output = detail.get("output", "{}")
    if isinstance(raw_output, str):
        return json.loads(raw_output or "{}")
    if isinstance(raw_output, dict):
        return raw_output
    return {}


def _has_running_build(codebuild: Any, project_name: str) -> bool:
    build_ids = codebuild.list_builds_for_project(
        projectName=project_name,
        sortOrder="DESCENDING",
    ).get("ids", [])[:20]
    if not build_ids:
        return False
    builds = codebuild.batch_get_builds(ids=build_ids).get("builds", [])
    return any(build.get("buildStatus") in RUNNING_STATES for build in builds)


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, str]:
    """Dispatch a serialized dbt build only for verified completed batches."""
    project_name = os.environ["CODEBUILD_PROJECT"]
    output = _execution_output(event)
    registration = output.get("registration", {})
    verification = output.get("verification", {})

    if registration.get("action") != "START_GLUE":
        return {"action": "IGNORED", "reason": "no_work_or_duplicate"}
    if verification.get("action") != "COMPLETED":
        return {"action": "IGNORED", "reason": "ledger_not_completed"}

    codebuild = boto3.client("codebuild")
    if _has_running_build(codebuild, project_name):
        LOGGER.info("A dbt build is already running; current source state is covered.")
        return {"action": "IGNORED", "reason": "build_already_running"}

    try:
        build = codebuild.start_build(projectName=project_name)["build"]
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"InvalidInputException", "ResourceUnavailableException"}:
            LOGGER.info("CodeBuild concurrency protection rejected duplicate start: %s", code)
            return {"action": "IGNORED", "reason": "concurrency_protected"}
        raise

    return {"action": "STARTED", "build_id": build["id"]}
