# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import botocore.client
import botocore.exceptions
import json
import logging
import os
import posixpath
import time
from dataclasses import dataclass, field
from typing import ClassVar, Optional

from ..models import CommandResult, Host
from ..util import call_api

LOG = logging.getLogger(__name__)


class InstanceProps:
    subnet_id: str
    security_group_id: str
    instance_profile_name: str
    instance_type: str
    bootstrap_bucket_name: str
    user_data_commands: Optional[list[str]]
    override_ami_id: Optional[str]
    """
    Option to override the AMI ID for the EC2 instance. The latest AL2023 is used by default.
    Note that the scripting to configure the EC2 instance is only verified to work on AL2023.
    """
    os_user: str
    """The OS user to chown the copied files to. Defaults to ec2-user"""
    file_mappings: Optional[dict[str, str]]
    """Mapping of files to copy from deployment environment to host environment"""

    def __init__(
        self,
        *,
        subnet_id: str,
        security_group_id: str,
        instance_profile_name: str,
        instance_type: str,
        bootstrap_bucket_name: str,
        user_data_commands: Optional[list[str]] = None,
        override_ami_id: Optional[str] = None,
        os_user: str = "ec2-user",
        file_mappings: Optional[dict[str, str]] = None,
    ) -> None:
        self.subnet_id = subnet_id
        self.security_group_id = security_group_id
        self.instance_profile_name = instance_profile_name
        self.instance_type = instance_type
        self.bootstrap_bucket_name = bootstrap_bucket_name
        self.user_data_commands = user_data_commands
        self.override_ami_id = override_ami_id
        self.os_user = os_user
        self.file_mappings = file_mappings


@dataclass
class Instance(Host):
    AL2023_AMI_NAME: ClassVar[str] = "al2023-ami-kernel-6.1-x86_64"

    props: InstanceProps
    s3_client: botocore.client.BaseClient
    ec2_client: botocore.client.BaseClient
    ssm_client: botocore.client.BaseClient

    instance_id: Optional[str] = field(init=False, default=None)

    def __post_init__(self, override_ami_id: Optional[str] = None):
        if override_ami_id:
            self._ami_id = override_ami_id

    def start(self) -> None:
        s3_files = self._stage_s3_bucket()
        self._launch_instance(s3_files=s3_files)

    def stop(self) -> None:
        LOG.info(f"Terminating EC2 instance {self.instance_id}")
        self.ec2_client.terminate_instances(InstanceIds=[self.instance_id])
        self.instance_id = None

    def send_command(
        self,
        command: str,
        *,
        wait_delay: Optional[int] = None,
        wait_max_attempts: Optional[int] = None,
    ) -> CommandResult:
        """Send a command via SSM to a shell on a launched EC2 instance. Once the command has fully
        finished the result of the invocation is returned.
        """
        ssm_waiter = self.ssm_client.get_waiter("command_executed")

        # To successfully send an SSM Command to an instance the instance must:
        #  1) Be in RUNNING state;
        #  2) Have the AWS Systems Manager (SSM) Agent running; and
        #  3) Have had enough time for the SSM Agent to connect to System's Manager
        #
        # If we send an SSM command then we will get an InvalidInstanceId error
        # if the instance isn't in that state.
        NUM_RETRIES = 10
        SLEEP_INTERVAL_S = 5
        for i in range(0, NUM_RETRIES):
            LOG.info(f"Sending SSM command to instance {self.instance_id}")
            try:
                send_command_response = self.ssm_client.send_command(
                    InstanceIds=[self.instance_id],
                    DocumentName="AWS-RunShellScript",
                    Parameters={"commands": [command]},
                )
                # Successfully sent. Bail out of the loop.
                break
            except botocore.exceptions.ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code == "InvalidInstanceId" and i < NUM_RETRIES - 1:
                    LOG.warning(
                        f"Instance {self.instance_id} is not ready for SSM command (received InvalidInstanceId error). Retrying in {SLEEP_INTERVAL_S}s."
                    )
                    time.sleep(SLEEP_INTERVAL_S)
                    continue
                raise

        command_id = send_command_response["Command"]["CommandId"]

        LOG.info(f"Waiting for SSM command {command_id} to reach a terminal state")
        try:
            ssm_waiter.wait(
                InstanceId=self.instance_id,
                CommandId=command_id,
                WaiterConfig={
                    # Total of 5 minutes
                    "Delay": wait_delay if wait_delay is not None else 5,
                    "MaxAttempts": wait_max_attempts if wait_max_attempts is not None else 60,
                },
            )
        except botocore.exceptions.WaiterError:  # pragma: no cover
            # Swallow exception, we're going to check the result anyway
            pass

        ssm_command_result = self.ssm_client.get_command_invocation(
            InstanceId=self.instance_id,
            CommandId=command_id,
        )
        result = CommandResult(
            exit_code=ssm_command_result["ResponseCode"],
            stdout=ssm_command_result["StandardOutputContent"],
            stderr=ssm_command_result["StandardErrorContent"],
        )
        if result.exit_code == -1:  # pragma: no cover
            # Response code of -1 in a terminal state means the command was not received by the node
            LOG.error(f"Failed to send SSM command {command_id} to {self.instance_id}: {result}")
            LOG.error(f"SSM SendCommand result: {json.dumps(ssm_command_result)}")

        LOG.info(f"SSM command {command_id} completed with exit code: {result.exit_code}")
        return result

    def _stage_s3_bucket(self) -> dict[str, str] | None:
        """Stages file_mappings to an S3 bucket and returns the mapping of S3 URI to dest path"""
        if not self.props.file_mappings:
            LOG.info("No file mappings to stage to S3")
            return None

        srcpath_s3uri_map = upload_files_to_s3(
            s3_client=self.s3_client,
            bucket=self.props.bootstrap_bucket_name,
            files=list(self.props.file_mappings.keys()),
        )

        return {s3uri: self.props.file_mappings[path] for path, s3uri in srcpath_s3uri_map.items()}

    def _launch_instance(
        self,
        *,
        s3_files: dict[str, str] | None = None,
    ) -> None:
        assert (
            not self.instance_id
        ), "Attempted to launch EC2 instance when one was already launched"

        copy_s3_command = ""
        if s3_files:
            copy_s3_command = " && ".join(
                [
                    f"aws s3 cp {s3_uri} {dst} --quiet && chown {self.props.os_user} {dst}"
                    for s3_uri, dst in s3_files.items()
                ]
            )

        LOG.info("Launching EC2 instance")
        newline = "\n"
        run_instance_response = self.ec2_client.run_instances(
            MinCount=1,
            MaxCount=1,
            ImageId=self.ami_id,
            InstanceType=self.props.instance_type,
            IamInstanceProfile={"Name": self.props.instance_profile_name},
            SubnetId=self.props.subnet_id,
            SecurityGroupIds=[self.props.security_group_id],
            MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {
                            "Key": "InstanceIdentification",
                            "Value": "DeadlineScaffoldingInstance",
                        },
                    ],
                },
            ],
            BlockDeviceMappings=[
                {
                    "DeviceName": "/dev/xvda",
                    "Ebs": {
                        "DeleteOnTermination": True,
                        "VolumeSize": 100,
                    },
                },
            ],
            UserData=f"""#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
set -x

# Set a max bandwidth otherwise aws s3 cp hangs when downloading the Maya installer
aws configure set default.s3.max_bandwidth 10MB/s
{copy_s3_command}
{newline.join(self.props.user_data_commands or [])}
""",
        )

        self.instance_id = run_instance_response["Instances"][0]["InstanceId"]
        LOG.info(f"Launched EC2 instance {self.instance_id}")

        LOG.info(f"Waiting for EC2 instance {self.instance_id} status to be OK")
        instance_running_waiter = self.ec2_client.get_waiter("instance_status_ok")
        instance_running_waiter.wait(
            InstanceIds=[self.instance_id],
            WaiterConfig={"Delay": 15, "MaxAttempts": 60},
        )
        LOG.info(f"EC2 instance {self.instance_id} status is OK")

        LOG.info("Waiting for cloud-init to complete")
        cmd_result = self.send_command("cloud-init status --wait")
        if cmd_result.exit_code != 0:
            raise Exception(f"Failed waiting for cloud-init to complete: {cmd_result}")
        LOG.info("cloud-init complete")

    @property
    def ami_id(self) -> str:
        if not hasattr(self, "_ami_id"):
            # Grab the latest AL2023 AMI
            # https://aws.amazon.com/blogs/compute/query-for-the-latest-amazon-linux-ami-ids-using-aws-systems-manager-parameter-store/
            ssm_param_name = f"/aws/service/ami-amazon-linux-latest/{Instance.AL2023_AMI_NAME}"
            response = call_api(
                description=f"Getting latest AL2023 AMI ID from SSM parameter {ssm_param_name}",
                fn=lambda: self.ssm_client.get_parameters(Names=[ssm_param_name]),
            )

            parameters = response.get("Parameters", [])
            assert (
                len(parameters) == 1
            ), f"Received incorrect number of SSM parameters. Expected 1, got response: {response}"
            self._ami_id = parameters[0]["Value"]
            LOG.info(f"Using latest AL2023 AMI {self._ami_id}")

        return self._ami_id


def upload_files_to_s3(
    *,
    s3_client: botocore.client.BaseClient,
    bucket: str,
    files: list[str],
    prefix: Optional[str] = None,
) -> dict[str, str]:
    key_prefix = prefix or "instance/"
    if not key_prefix.endswith("/"):
        key_prefix += "/"

    srcpath_s3uri_map: dict[str, str] = {}
    for file in files:
        key = key_prefix + os.path.basename(file)
        uri = f"s3://{bucket}/{key}"
        srcpath_s3uri_map[file] = uri

        LOG.info(f"Uploading file {file} to {uri}")
        try:
            with open(file, mode="rb") as f:
                s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=f,
                )
        except botocore.exceptions.ClientError as e:
            LOG.exception(f"Failed to upload file {file} to {uri}: {e}")
            raise

    return srcpath_s3uri_map
