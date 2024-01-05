from __future__ import annotations

import glob
import logging
import os
import time
from dataclasses import (
    dataclass,
    field,
    ClassVar,
    InitVar,
)
from typing import (
    Optional,
)

from ..models import CommandResult
from ..util import call_api

import botocore.client
import botocore.exceptions

LOG = logging.getLogger(__name__)

@dataclass
class InstanceConfiguration:
    file_mappings: list[tuple[str, str]] | None = None
    """Mapping of files to copy from host environment to EC2 instance environment"""
    user_data: list[str] | None = None
    """List of user data commands to run on the instance"""

@dataclass
class Instance:
    AL2023_AMI_NAME: ClassVar[str] = "al2023-ami-kernel-6.1-x86_64"

    subnet_id: str
    security_group_id: str
    instance_profile_name: str
    bootstrap_bucket_name: str
    s3_client: botocore.client.BaseClient
    ec2_client: botocore.client.BaseClient
    ssm_client: botocore.client.BaseClient

    instance_id: Optional[str] = field(init=False, default=None)

    override_ami_id: InitVar[Optional[str]] = None
    """
    Option to override the AMI ID for the EC2 instance. The latest AL2023 is used by default.
    Note that the scripting to configure the EC2 instance is only verified to work on AL2023.
    """

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

    def send_command(self, command: str) -> CommandResult:
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

        LOG.info(f"SSM command {command_id} completed with exit code: {result.exit_code}")
        return result

    def _stage_s3_bucket(self) -> list[tuple[str, str]] | None:
        """Stages file_mappings to an S3 bucket and returns the mapping of S3 URI to dest path"""
        if not self.configuration.file_mappings:
            LOG.info("No file mappings to stage to S3")
            return None

        s3_to_src_mapping: dict[str, str] = {}
        s3_to_dst_mapping: dict[str, str] = {}
        for src_glob, dst in self.configuration.file_mappings:
            for src_file in glob.glob(src_glob):
                s3_key = f"instance/{os.path.basename(src_file)}"
                assert s3_key not in s3_to_src_mapping, (
                    "Duplicate S3 keys generated for file mappings. All source files must have unique "
                    + f"filenames. Mapping: {self.configuration.file_mappings}"
                )
                s3_to_src_mapping[s3_key] = src_file
                s3_to_dst_mapping[f"s3://{self.bootstrap_bucket_name}/{s3_key}"] = dst

        for key, local_path in s3_to_src_mapping.items():
            LOG.info(f"Uploading file {local_path} to s3://{self.bootstrap_bucket_name}/{key}")
            try:
                with open(local_path, mode="rb") as f:
                    self.s3_client.put_object(
                        Bucket=self.bootstrap_bucket_name,
                        Key=key,
                        Body=f,
                    )
            except botocore.exceptions.ClientError as e:
                LOG.exception(
                    f"Failed to upload file {local_path} to s3://{self.bootstrap_bucket_name}/{key}: {e}"
                )
                raise

        return list(s3_to_dst_mapping.items())

    def _launch_instance(self, *, s3_files: list[tuple[str, str]] | None = None) -> None:
        assert (
            not self.instance_id
        ), "Attempted to launch EC2 instance when one was already launched"

        copy_s3_command = ""
        if s3_files:
            copy_s3_command = " && ".join(
                [
                    f"aws s3 cp {s3_uri} {dst} && chown {self.configuration.user} {dst}"
                    for s3_uri, dst in s3_files
                ]
            )

        job_users_cmds = []
        for job_user in self.configuration.job_users:
            job_users_cmds.append(f"groupadd {job_user.group}")
            job_users_cmds.append(
                f"useradd --create-home --system --shell=/bin/bash --groups={self.configuration.group} -g {job_user.group} {job_user.user}"
            )
            job_users_cmds.append(f"usermod -a -G {job_user.group} {self.configuration.user}")

        sudoer_rule_users = ",".join(
            [
                self.configuration.user,
                *[job_user.user for job_user in self.configuration.job_users],
            ]
        )
        job_users_cmds.append(
            f'echo "{self.configuration.user} ALL=({sudoer_rule_users}) NOPASSWD: ALL" > /etc/sudoers.d/{self.configuration.user}'
        )

        configure_job_users = "\n".join(job_users_cmds)

        LOG.info("Launching EC2 instance")
        run_instance_response = self.ec2_client.run_instances(
            MinCount=1,
            MaxCount=1,
            ImageId=self.ami_id,
            InstanceType="t3.micro",
            IamInstanceProfile={"Name": self.instance_profile_name},
            SubnetId=self.subnet_id,
            SecurityGroupIds=[self.security_group_id],
            MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {
                            "Key": "InstanceIdentification",
                            "Value": "DeadlineScaffoldingWorker",
                        }
                    ],
                }
            ],
            UserData=f"""#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
set -x
groupadd --system {self.configuration.group}
useradd --create-home --system --shell=/bin/bash --groups={self.configuration.group} {self.configuration.user}
{configure_job_users}
{copy_s3_command}

runuser --login {self.configuration.user} --command 'python3 -m venv $HOME/.venv && echo ". $HOME/.venv/bin/activate" >> $HOME/.bashrc'
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

    @property
    def ami_id(self) -> str:
        if not hasattr(self, "_ami_id"):
            # Grab the latest AL2023 AMI
            # https://aws.amazon.com/blogs/compute/query-for-the-latest-amazon-linux-ami-ids-using-aws-systems-manager-parameter-store/
            ssm_param_name = (
                f"/aws/service/ami-amazon-linux-latest/{Instance.AL2023_AMI_NAME}"
            )
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
