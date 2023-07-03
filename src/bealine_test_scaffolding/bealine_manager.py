# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import os
import posixpath
import sys
import tempfile
from time import sleep
from typing import Any, Dict, Optional

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.loaders import Loader
from botocore.model import ServiceModel, OperationModel


class BealineManager:
    """This class is responsible for setting up and tearing down the required components
    for the tests to be run."""

    bealine_service_model_bucket: Optional[str] = None
    bealine_endpoint: Optional[str] = None

    kms_client: BaseClient
    kms_key_metadata: Optional[Dict[str, Any]]

    bealine_client: BealineClient
    farm_id: Optional[str]
    queue_id: Optional[str]
    fleet_id: Optional[str]
    additional_queues: list[dict[str, Any]]
    bealine_model_dir: Optional[tempfile.TemporaryDirectory] = None

    MOCKED_SERVICE_VERSION = "2020-08-21"

    def __init__(self, should_add_bealine_models: bool = False) -> None:
        """
        Initializing the Bealine Manager
        """
        self.bealine_service_model_bucket = os.getenv("BEALINE_SERVICE_MODEL_BUCKET")
        self.bealine_endpoint = os.getenv("BEALINE_ENDPOINT")

        # Installing the bealine service models.
        if should_add_bealine_models:
            self.get_bealine_models()

        self.bealine_client = self._get_bealine_client(self.bealine_endpoint)

        # Create the KMS client
        self.kms_client = boto3.client("kms")

        self.farm_id: Optional[str] = None
        self.queue_id: Optional[str] = None
        self.fleet_id: Optional[str] = None
        self.additional_queues: list[dict[str, Any]] = []
        self.kms_key_metadata: Optional[dict[str, Any]] = None

    def get_bealine_models(self):
        """
        This function will download and install the models for bealine so we can use the bealine
        client.
        """
        if self.bealine_service_model_bucket is None:
            raise ValueError(
                "Environment variable BEALINE_SERVICE_MODEL_BUCKET is not set. "
                "Unable to get bealine service model."
            )

        # Create the S3 client
        s3_client: BaseClient = boto3.client("s3")

        # Create a temp directory to store the model file
        self.bealine_model_dir = tempfile.TemporaryDirectory()
        service_model_dir = posixpath.join(
            self.bealine_model_dir.name, "bealine", self.MOCKED_SERVICE_VERSION
        )
        os.makedirs(service_model_dir)

        # Downloading the bealine models.
        s3_client.download_file(
            self.bealine_service_model_bucket,
            "service-2.json",
            posixpath.join(service_model_dir, "service-2.json"),
        )
        os.environ["AWS_DATA_PATH"] = self.bealine_model_dir.name

    def create_scaffolding(
        self,
        farm_name: str = "test_farm",
        queue_name: str = "test_queue",
        fleet_name: str = "test_fleet",
    ) -> None:
        self.create_kms_key()
        self.create_farm(farm_name)

        # TODO: Remove sleep once we have proper queuing of commands
        # to the scheduler (currently there is a race condition between
        # CreateFarm and CreateQueue)
        sleep(120)

        self.create_queue(queue_name)

        # TODO: Remove sleep once we have proper queuing of commands
        # to the scheduler (currently there is a race condition between
        # CreateQueue and CreateJob)
        sleep(15)

        self.create_fleet(fleet_name)
        sleep(15)

        self.bealine_client.update_queue(
            farmId=self.farm_id,
            queueId=self.queue_id,
            fleets=[{"fleetId": self.fleet_id, "priority": 1}],
        )

    def create_kms_key(self) -> None:
        try:
            response: Dict[str, Any] = self.kms_client.create_key(
                Description="The KMS used for testing created by the "
                "BealineClientSoftwareTestScaffolding.",
                Tags=[{"TagKey": "Name", "TagValue": "BealineClientSoftwareTestScaffolding"}],
            )
        except ClientError as e:
            print("Failed to create CMK.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            self.kms_key_metadata = response["KeyMetadata"]

            # We should always get a metadata when successful, this is for mypy.
            if self.kms_key_metadata:  # pragma: no cover
                print(f"Created CMK with id = {self.kms_key_metadata['KeyId']}")
                self.kms_client.enable_key(KeyId=self.kms_key_metadata["KeyId"])
                print(f"Enabled CMK with id = {self.kms_key_metadata['KeyId']}")

    def delete_kms_key(self) -> None:
        if (
            not hasattr(self, "kms_key_metadata")
            or self.kms_key_metadata is None
            or "KeyId" not in self.kms_key_metadata
        ):
            raise Exception("ERROR: Attempting to delete a KMS key when None was created!")

        try:
            # KMS keys by default are deleted in 30 days (this is their pending window).
            # 7 days is the fastest we can clean them up.
            pending_window = 7
            self.kms_client.schedule_key_deletion(
                KeyId=self.kms_key_metadata["KeyId"], PendingWindowInDays=pending_window
            )
        except ClientError as e:
            print(
                "Failed to schedule the deletion of CMK with id = "
                f"{self.kms_key_metadata['KeyId']}",
                file=sys.stderr,
            )
            print(f"The following error was raised: {e}", file=sys.stderr)
            raise
        else:
            print(f"Scheduled deletion of CMK with id = {self.kms_key_metadata['KeyId']}")
            self.kms_key_metadata = None

    def create_farm(self, farm_name: str) -> None:
        if (
            not hasattr(self, "kms_key_metadata")
            or self.kms_key_metadata is None
            or "Arn" not in self.kms_key_metadata
        ):
            raise Exception("ERROR: Attempting to create a farm without having creating a CMK.")

        try:
            response = self.bealine_client.create_farm(
                displayName=farm_name, kmsKeyArn=self.kms_key_metadata["Arn"]
            )
        except ClientError as e:
            print("Failed to create a farm.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            self.farm_id = response["farmId"]
            print(f"Successfully create farm with id = {self.farm_id}")

    def delete_farm(self) -> None:
        if not hasattr(self, "farm_id") or not self.farm_id:
            raise Exception("ERROR: Attempting to delete a farm without having created one.")

        try:
            self.bealine_client.delete_farm(farmId=self.farm_id)
        except ClientError as e:
            print(f"Failed to delete farm with id = {self.farm_id}.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            print(f"Successfully deleted farm with id = {self.farm_id}")
            self.farm_id = None

    def create_queue(
        self, queue_name: str, log_bucket_name: str = "BealineClientSoftwareTestScaffolding"
    ) -> None:
        if not hasattr(self, "farm_id") or self.farm_id is None:
            raise Exception(
                "ERROR: Attempting to create a queue without having had created a farm!"
            )

        try:
            response = self.bealine_client.create_queue(
                name=queue_name,
                farmId=self.farm_id,
            )
        except ClientError as e:
            print(f"Failed to create queue with name = {queue_name}.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            self.queue_id = response["queueId"]
            print(f"Successfully created queue with id = {self.queue_id}")

    def create_additional_queue(self, **kwargs) -> Dict[str, Any]:
        """Create and add another queue to the bealine manager"""
        input = {"farmId": self.farm_id}
        input.update(kwargs)
        response = self.bealine_client.create_queue(**input)
        response = self.bealine_client.get_queue(
            farmId=input["farmId"], queueId=response["queueId"]
        )
        self.additional_queues.append(response)
        return response

    def delete_queue(self) -> None:
        if not hasattr(self, "farm_id") or not self.farm_id:
            raise Exception(
                "ERROR: Attempting to delete a queue without having had created a farm!"
            )

        if not hasattr(self, "queue_id") or not self.queue_id:
            raise Exception("ERROR: Attempting to delete a queue without having had created one!")

        try:
            self.bealine_client.delete_queue(queueId=self.queue_id, farmId=self.farm_id)
        except ClientError as e:
            print(f"Failed to delete queue with id = {self.queue_id}.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            print(f"Successfully deleted queue with id = {self.queue_id}")
            self.queue_id = None

    def delete_additional_queues(self) -> None:
        """Delete all additional queues that have been added."""
        for queue in self.additional_queues:
            try:
                self.bealine_client.delete_queue(farmId=queue["farmId"], queueId=queue["queueId"])
            except Exception as e:
                print(f"delete queue exception {str(e)}")
                continue

    def create_fleet(self, fleet_name: str) -> None:
        if not hasattr(self, "farm_id") or not self.farm_id:
            raise Exception(
                "ERROR: Attempting to create a fleet without having had created a farm!"
            )

        try:
            response = self.bealine_client.create_fleet(farmId=self.farm_id, name=fleet_name)
        except ClientError as e:
            print(f"Failed to create fleet with name = {fleet_name}.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            self.fleet_id = response["fleetId"]
            print(f"Successfully created a fleet with id = {self.fleet_id}")

    def delete_fleet(self) -> None:
        if not hasattr(self, "farm_id") or not self.farm_id:
            raise Exception(
                "ERROR: Attempting to delete a fleet without having had created a farm!"
            )

        if not hasattr(self, "fleet_id") or not self.fleet_id:
            raise Exception("ERROR: Attempting to delete a fleet when none was created!")

        try:
            # We need to disable the fleet before deleting it.
            self.bealine_client.update_fleet(
                farmId=self.farm_id, fleetId=self.fleet_id, state="DISABLED"
            )

            # Deleting the fleet.
            self.bealine_client.delete_fleet(farmId=self.farm_id, fleetId=self.fleet_id)
        except ClientError as e:
            print(
                f"ERROR: Failed to delete delete fleet with id = {self.fleet_id}", file=sys.stderr
            )
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            print(f"Successfully deleted fleet with id = {self.fleet_id}")
            self.fleet_id = None

    def cleanup_scaffolding(self) -> None:
        # If we have a farm, then we want to delete fleet, queue and farm.
        self.delete_additional_queues()

        if hasattr(self, "farm_id") and self.farm_id:
            # Only deleting the queue if we have a queue.
            if hasattr(self, "queue_id") and self.queue_id:
                self.delete_queue()

            # Only deleting the fleet if we have a fleet.
            if hasattr(self, "fleet_id") and self.fleet_id:
                self.delete_fleet()

            self.delete_farm()

        # Only deleting the kms key if we have a kms key.
        if hasattr(self, "kms_key_metadata") and self.kms_key_metadata:
            self.delete_kms_key()

    def _get_bealine_client(self, bealine_endpoint: Optional[str]) -> BealineClient:
        """Create a BealineClient shim layer over an actual boto client"""
        session = boto3.Session()
        real_bealine_client = session.client(
            "bealine",
            endpoint_url=bealine_endpoint,
        )
        return BealineClient(real_bealine_client)


class BealineClient:
    """
    A shim layer for boto Bealine client. This class will check if a method exists on the real
    boto3 Bealine client and call it if it exists. If it doesn't exist, an AttributeError will be raised.
    """

    _real_client: Any

    def __init__(self, real_client: Any) -> None:
        self._real_client = real_client

    def create_farm(self, *args, **kwargs) -> Any:
        create_farm_input_members = self._get_bealine_api_input_shape("CreateFarm")
        if "displayName" not in create_farm_input_members and "name" in create_farm_input_members:
            kwargs["name"] = kwargs.pop("displayName")
        return self._real_client.create_farm(*args, **kwargs)

    def _get_bealine_api_input_shape(self, api_name: str) -> dict[str, Any]:
        """
        Given a string name of an API e.g. CreateJob, returns the shape of the
        inputs to that API.
        """
        api_model = self._get_bealine_api_model(api_name)
        if api_model:
            return api_model.input_shape.members
        return {}

    def _get_bealine_api_model(self, api_name: str) -> Optional[OperationModel]:
        """
        Given a string name of an API e.g. CreateJob, returns the OperationModel
        for that API from the service model.
        """
        loader = Loader()
        bealine_service_description = loader.load_service_model("bealine", "service-2")
        bealine_service_model = ServiceModel(bealine_service_description, service_name="bealine")
        return OperationModel(
            bealine_service_description["operations"][api_name], bealine_service_model
        )

    def __getattr__(self, __name: str) -> Any:
        """
        Respond to unknown method calls by calling the underlying _real_client
        If the underlying _real_client does not have a given method, an AttributeError
        will be raised.
        Note that __getattr__ is only called if the attribute cannot otherwise be found,
        so if this class alread has the called method defined, __getattr__ will not be called.
        This is in opposition to __getattribute__ which is called by default.
        """

        def method(*args, **kwargs):
            return getattr(self._real_client, __name)(*args, **kwargs)

        return method
