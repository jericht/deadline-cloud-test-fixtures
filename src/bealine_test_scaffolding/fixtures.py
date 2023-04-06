# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os

import pytest

from .bealine_manager import BealineManager
from .job_attachment_manager import JobAttachmentManager


@pytest.fixture(scope="session")
def bealine_manager_fixture():
    bealine_manager = BealineManager(should_add_bealine_models=True)
    yield bealine_manager


@pytest.fixture(scope="session")
def job_attachment_manager_fixture(stage: str, account_id: str):
    job_attachment_manager = JobAttachmentManager(stage, account_id)
    yield job_attachment_manager


@pytest.fixture(scope="session")
def bealine_scaffolding(bealine_manager_fixture: BealineManager):
    bealine_manager_fixture.create_scaffolding()
    yield bealine_manager_fixture
    bealine_manager_fixture.cleanup_scaffolding()


@pytest.fixture(scope="session")
def deploy_job_attachment_resources(job_attachment_manager_fixture: JobAttachmentManager):
    job_attachment_manager_fixture.deploy_resources()
    yield job_attachment_manager_fixture
    job_attachment_manager_fixture.cleanup_resources()


@pytest.fixture(scope="session")
def stage() -> str:
    if os.getenv("LOCAL_DEVELOPMENT", "false").lower() == "true":
        return "dev"
    else:
        return os.environ["STAGE"]


@pytest.fixture(scope="session")
def account_id() -> str:
    return os.environ["SERVICE_ACCOUNT_ID"]
