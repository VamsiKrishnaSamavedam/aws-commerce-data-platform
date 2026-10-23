"""Unit tests for the event-driven dbt build dispatcher."""

import os
import unittest
from unittest.mock import Mock, patch

from scripts import dispatch_dbt_build


VERIFIED_EVENT = {
    "detail": {
        "output": '{"registration":{"action":"START_GLUE"},"verification":{"action":"COMPLETED"}}'
    }
}


class DbtBuildDispatcherTests(unittest.TestCase):
    @patch.dict(os.environ, {"CODEBUILD_PROJECT": "commerce-dbt-athena-build"})
    @patch("scripts.dispatch_dbt_build.boto3.client")
    def test_starts_build_for_verified_completion(self, client_factory):
        codebuild = Mock()
        codebuild.list_builds_for_project.return_value = {"ids": []}
        codebuild.start_build.return_value = {"build": {"id": "build-123"}}
        client_factory.return_value = codebuild

        result = dispatch_dbt_build.lambda_handler(VERIFIED_EVENT, None)

        self.assertEqual(result, {"action": "STARTED", "build_id": "build-123"})
        codebuild.start_build.assert_called_once_with(
            projectName="commerce-dbt-athena-build"
        )

    @patch.dict(os.environ, {"CODEBUILD_PROJECT": "commerce-dbt-athena-build"})
    @patch("scripts.dispatch_dbt_build.boto3.client")
    def test_ignores_no_work_execution(self, client_factory):
        event = {"detail": {"output": '{"registration":{"action":"NO_WORK"}}'}}

        result = dispatch_dbt_build.lambda_handler(event, None)

        self.assertEqual(result, {"action": "IGNORED", "reason": "no_work_or_duplicate"})
        client_factory.assert_not_called()

    @patch.dict(os.environ, {"CODEBUILD_PROJECT": "commerce-dbt-athena-build"})
    @patch("scripts.dispatch_dbt_build.boto3.client")
    def test_ignores_duplicate_when_build_is_running(self, client_factory):
        codebuild = Mock()
        codebuild.list_builds_for_project.return_value = {"ids": ["build-123"]}
        codebuild.batch_get_builds.return_value = {
            "builds": [{"buildStatus": "IN_PROGRESS"}]
        }
        client_factory.return_value = codebuild

        result = dispatch_dbt_build.lambda_handler(VERIFIED_EVENT, None)

        self.assertEqual(result, {"action": "IGNORED", "reason": "build_already_running"})
        codebuild.start_build.assert_not_called()
