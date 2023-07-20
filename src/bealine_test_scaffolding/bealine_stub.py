# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import dataclasses
from dataclasses import dataclass
from typing import Optional
from bealine_test_scaffolding.constants import JOB_ATTACHMENTS_ROOT_PREFIX

from botocore.exceptions import ClientError as OriginalClientError


class ClientError(OriginalClientError):
    def __init__(self, errmsg, operation_name):
        super().__init__(
            error_response={"Error": {"Message": errmsg}},
            operation_name=operation_name,
        )


@dataclass
class FarmInfo:
    """
    Dataclass used to build list-farm responses
    """

    displayName: str
    farmId: str = "farm-01234567890123456789012345678901"  # pylint: disable=invalid-name
    status: str = "ACTIVE"


@dataclass
class QueueInfo:
    """
    Dataclass used to build list-queues responses
    """

    displayName: str
    queueId: str = "queue-01234567890123456789012345678901"  # pylint: disable=invalid-name
    status: str = "ACTIVE"
    farmId: str = "farm-01234567890123456789012345678901"  # pylint: disable=invalid-name


@dataclass
class JobInfo:
    jobId: str
    farmId: str
    queueId: str
    jobTemplate: str
    jobTemplateType: str
    priority: str
    attachments: dict


@dataclass
class StubBealineClient:
    """
    Stub implementation of the Bealine client generated by botocore.
    """

    farm: FarmInfo
    queue: QueueInfo
    job: Optional[JobInfo] = None
    job_attachments_bucket_name: Optional[str] = None

    def create_job(self, **kwargs) -> dict:
        self.job = JobInfo(jobId="job-123", **kwargs)
        return {
            "jobId": self.job.jobId,
            "state": "CREATING",
        }

    def get_queue(self, *, farmId: str, queueId: str) -> dict:
        if farmId != self.farm.farmId:
            raise ClientError(
                f"Wrong farm ID. Expected {self.farm.farmId}, got {farmId}", "GetQueue"
            )
        if queueId != self.queue.queueId:
            raise ClientError(
                f"Wrong queue ID. Expected {self.queue.queueId}, got {queueId}", "GetQueue"
            )

        return {
            **dataclasses.asdict(self.queue),
            "fleets": [],
            "jobAttachmentSettings": {
                "s3BucketName": self.job_attachments_bucket_name,
                "rootPrefix": JOB_ATTACHMENTS_ROOT_PREFIX,
            },
        }

    def list_queues(self, *, farmId: str) -> dict:
        return {"queues": [dataclasses.asdict(self.queue)] if farmId == self.queue.farmId else []}

    def list_farms(self, *, isMemberOf: bool = False) -> dict:
        return {"farms": [dataclasses.asdict(self.farm)]}
