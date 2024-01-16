# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import abc
import botocore.client
import botocore.exceptions
import json
import logging
import os
import pathlib
import posixpath
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field, replace
from typing import Any, Optional, cast

from ..models import CommandResult

from .client import DeadlineClient
from ..ec2 import Instance, InstanceProps
from ..models import (
    CommandResult,
    Host,
    PipInstall,
    PosixSessionUser,
    ServiceModel,
)
from ..util import wait_for

LOG = logging.getLogger(__name__)

# Hardcoded to default posix path for worker.json file which has the worker ID in it
WORKER_JSON_PATH = "/var/lib/deadline/worker.json"
DOCKER_CONTEXT_DIR = os.path.join(os.path.dirname(__file__), "..", "containers", "worker")


def configure_worker_command(*, config: DeadlineWorkerConfiguration) -> str:  # pragma: no cover
    """Get the command to configure the Worker. This must be run as root."""
    cmds = [
        config.worker_agent_install.install_command,
        *(config.pre_install_commands or []),
        # fmt: off
        (
            "install-deadline-worker "
            + "-y "
            + f"--farm-id {config.farm_id} "
            + f"--fleet-id {config.fleet_id} "
            + f"--region {config.region} "
            + f"--user {config.user} "
            + f"--group {config.group} "
            + f"{'--allow-shutdown ' if config.allow_shutdown else ''}"
            + f"{'--no-install-service ' if config.no_install_service else ''}"
            + f"{'--start ' if config.start_service else ''}"
        ),
        # fmt: on
    ]

    if config.service_model:
        cmds.append(
            f"runuser -l {config.user} -s /bin/bash -c '{config.service_model.install_command}'"
        )

    return " && ".join(cmds)


class DeadlineWorker(Host):
    @abc.abstractproperty
    def worker_id(self) -> str:
        pass


@dataclass(frozen=True)
class DeadlineWorkerConfiguration:
    farm_id: str
    fleet_id: str
    region: str
    user: str
    group: str
    allow_shutdown: bool
    worker_agent_install: PipInstall
    job_users: list[PosixSessionUser] = field(
        default_factory=lambda: [PosixSessionUser("jobuser", "jobuser")]
    )
    start_service: bool = False
    no_install_service: bool = False
    service_model: ServiceModel | None = None
    file_mappings: list[tuple[str, str]] | None = None
    """Mapping of files to copy from host environment to worker environment"""
    pre_install_commands: list[str] | None = None
    """Commands to run before installing the Worker agent"""


class EC2InstanceWorkerProps(InstanceProps):
    deadline_client: DeadlineClient
    configuration: DeadlineWorkerConfiguration

    def __init__(
        self,
        *,
        deadline_client: DeadlineClient,
        configuration: DeadlineWorkerConfiguration,
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
        super().__init__(
            subnet_id=subnet_id,
            security_group_id=security_group_id,
            instance_profile_name=instance_profile_name,
            instance_type=instance_type,
            bootstrap_bucket_name=bootstrap_bucket_name,
            user_data_commands=user_data_commands,
            override_ami_id=override_ami_id,
            os_user=os_user,
            file_mappings=file_mappings,
        )
        self.deadline_client = deadline_client
        self.configuration = configuration


class EC2InstanceWorker(DeadlineWorker):
    props: EC2InstanceWorkerProps
    configuration: DeadlineWorkerConfiguration
    instance: Instance

    def __init__(
        self,
        *,
        props: EC2InstanceWorkerProps,
        s3_client: botocore.client.BaseClient,
        ec2_client: botocore.client.BaseClient,
        ssm_client: botocore.client.BaseClient,
    ) -> None:
        self.props = props
        self.configuration = props.configuration
        self.instance = Instance(
            props,
            s3_client=s3_client,
            ec2_client=ec2_client,
            ssm_client=ssm_client,
        )

    def start(self) -> None:
        self._add_user_data()
        self.instance.start()
        self._start_worker_agent()

    def stop(self) -> None:
        self.instance.stop()

    def send_command(self, command: str) -> CommandResult:
        return self.instance.send_command(command)

    def _add_user_data(self) -> None:
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

        self.props.user_data_commands = [
            *(self.props.user_data_commands or []),
            f"groupadd --system {self.configuration.group}",
            f"useradd --create-home --system --shell=/bin/bash --groups={self.configuration.group} {self.configuration.user}",
            # Set a max bandwidth otherwise aws s3 cp hangs when downloading the Maya installer
            "aws configure set default.s3.max_bandwidth 50MB/s",
            *job_users_cmds,
            f"runuser --login {self.configuration.user} --command 'python3 -m venv $HOME/.venv && echo \". $HOME/.venv/bin/activate\" >> $HOME/.bashrc'",
        ]

    def _start_worker_agent(self) -> None:  # pragma: no cover
        assert self.instance.instance_id

        LOG.info("Sending SSM command to configure Worker agent")
        cmd_result = self.send_command(
            f"cd /home/{self.configuration.user}; . .venv/bin/activate; AWS_DEFAULT_REGION={self.configuration.region} {configure_worker_command(config=self.configuration)}"
        )
        assert cmd_result.exit_code == 0, f"Failed to configure Worker agent: {cmd_result}"
        LOG.info("Successfully configured Worker agent")

        LOG.info("Sending SSM command to start Worker agent")
        cmd_result = self.send_command(
            " && ".join(
                [
                    f"nohup runuser --login {self.configuration.user} -c 'AWS_DEFAULT_REGION={self.configuration.region} deadline-worker-agent --allow-instance-profile >/dev/null 2>&1 &'",
                    # Verify Worker is still running
                    "echo Waiting 5s for agent to get started",
                    "sleep 5",
                    "echo 'Running pgrep to see if deadline-worker-agent is running'",
                    f"pgrep --count --full -u {self.configuration.user} deadline-worker-agent",
                ]
            ),
        )
        assert cmd_result.exit_code == 0, f"Failed to start Worker agent: {cmd_result}"
        LOG.info("Successfully started Worker agent")

    @property
    def worker_id(self) -> str:
        cmd_result = self.send_command("cat /var/lib/deadline/worker.json  | jq -r '.worker_id'")
        assert cmd_result.exit_code == 0, f"Failed to get Worker ID: {cmd_result}"

        worker_id = cmd_result.stdout.rstrip("\n\r")
        assert re.match(
            r"^worker-[0-9a-f]{32}$", worker_id
        ), f"Got nonvalid Worker ID from command stdout: {cmd_result}"
        return worker_id


@dataclass
class DockerContainerWorker(DeadlineWorker):
    configuration: DeadlineWorkerConfiguration

    _container_id: Optional[str] = field(init=False, default=None)

    def __post_init__(self) -> None:
        # Do not install Worker agent service since it's recommended to avoid systemd usage on Docker containers
        self.configuration = replace(self.configuration, no_install_service=True)

    def start(self) -> None:
        self._tmpdir = pathlib.Path(tempfile.mkdtemp())

        # TODO: Support multiple job users on Docker
        assert (
            len(self.configuration.job_users) == 1
        ), f"Multiple job users not supported on Docker worker: {self.configuration.job_users}"
        # Environment variables for "run_container.sh"
        run_container_env = {
            **os.environ,
            "FARM_ID": self.configuration.farm_id,
            "FLEET_ID": self.configuration.fleet_id,
            "AGENT_USER": self.configuration.user,
            "SHARED_GROUP": self.configuration.group,
            "JOB_USER": self.configuration.job_users[0].user,
            "CONFIGURE_WORKER_AGENT_CMD": configure_worker_command(
                config=self.configuration,
            ),
        }

        LOG.info(f"Staging Docker build context directory {str(self._tmpdir)}")
        shutil.copytree(DOCKER_CONTEXT_DIR, str(self._tmpdir), dirs_exist_ok=True)

        if self.configuration.file_mappings:
            # Stage a special dir with files to copy over to a temp folder in the Docker container
            # The container is responsible for copying files from that temp folder into the final destinations
            file_mappings_dir = self._tmpdir / "file_mappings"
            os.makedirs(str(file_mappings_dir))

            # Mapping of files in temp Docker container folder to their final destination
            docker_file_mappings: dict[str, str] = {}
            for src, dst in self.configuration.file_mappings:
                src_file_name = os.path.basename(src)

                # The Dockerfile copies the file_mappings dir in the build context to "/file_mappings" in the container
                # Build up an array of mappings from "/file_mappings" to their final destination
                src_docker_path = posixpath.join("/file_mappings", src_file_name)
                assert src_docker_path not in docker_file_mappings, (
                    "Duplicate paths generated for file mappings. All source files must have unique "
                    + f"filenames. Mapping: {self.configuration.file_mappings}"
                )
                docker_file_mappings[src_docker_path] = dst

                # Copy the file over to the stage directory
                staged_dst = str(file_mappings_dir / src_file_name)
                LOG.info(f"Copying file {src} to {staged_dst}")
                shutil.copyfile(src, staged_dst)

            run_container_env["FILE_MAPPINGS"] = json.dumps(docker_file_mappings)

        # Build and start the container
        LOG.info("Starting Docker container")
        try:
            proc = subprocess.Popen(
                args="./run_container.sh",
                cwd=str(self._tmpdir),
                env=run_container_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
            )

            # Live logging of Docker build
            assert proc.stdout
            with proc.stdout:
                for line in iter(proc.stdout.readline, ""):
                    LOG.info(line.rstrip("\r\n"))
        except Exception as e:  # pragma: no cover
            LOG.exception(f"Failed to start Worker agent Docker container: {e}")
            _handle_subprocess_error(e)
            raise
        else:
            exit_code = proc.wait(timeout=60)
            assert exit_code == 0, f"Process failed with exit code {exit_code}"

        # Grab the container ID from --cidfile
        try:
            self._container_id = subprocess.check_output(
                args=["cat", ".container_id"],
                cwd=str(self._tmpdir),
                text=True,
                encoding="utf-8",
                timeout=1,
            ).rstrip("\r\n")
        except Exception as e:  # pragma: no cover
            LOG.exception(f"Failed to get Docker container ID: {e}")
            _handle_subprocess_error(e)
            raise
        else:
            LOG.info(f"Started Docker container {self._container_id}")

    def stop(self) -> None:
        assert (
            self._container_id
        ), "Cannot stop Docker container: Container ID is not set. Has the Docker container been started yet?"

        LOG.info(f"Terminating Worker agent process in Docker container {self._container_id}")
        try:
            self.send_command(f"pkill --signal term -f {self.configuration.user}")
        except Exception as e:  # pragma: no cover
            LOG.exception(f"Failed to terminate Worker agent process: {e}")
            raise
        else:
            LOG.info("Worker agent process terminated")

        LOG.info(f"Stopping Docker container {self._container_id}")
        try:
            subprocess.check_output(
                args=["docker", "container", "stop", self._container_id],
                cwd=str(self._tmpdir),
                text=True,
                encoding="utf-8",
                timeout=30,
            )
        except Exception as e:  # pragma: noc over
            LOG.exception(f"Failed to stop Docker container {self._container_id}: {e}")
            _handle_subprocess_error(e)
            raise
        else:
            LOG.info(f"Stopped Docker container {self._container_id}")
            self._container_id = None

    def send_command(self, command: str, *, quiet: bool = False) -> CommandResult:
        assert (
            self._container_id
        ), "Container ID not set. Has the Docker container been started yet?"

        if not quiet:  # pragma: no cover
            LOG.info(f"Sending command '{command}' to Docker container {self._container_id}")
        try:
            result = subprocess.run(
                args=[
                    "docker",
                    "exec",
                    self._container_id,
                    "/bin/bash",
                    "-euo",
                    "pipefail",
                    "-c",
                    command,
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
            )
        except Exception as e:
            if not quiet:  # pragma: no cover
                LOG.exception(f"Failed to run command: {e}")
                _handle_subprocess_error(e)
            raise
        else:
            return CommandResult(
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )

    @property
    def worker_id(self) -> str:
        cmd_result: Optional[CommandResult] = None

        def got_worker_id() -> bool:
            nonlocal cmd_result
            try:
                cmd_result = self.send_command(
                    "cat /var/lib/deadline/worker.json | jq -r '.worker_id'",
                    quiet=True,
                )
            except subprocess.CalledProcessError as e:
                LOG.warning(f"Worker ID retrieval failed: {e}")
                return False
            else:
                return cmd_result.exit_code == 0

        wait_for(
            description="retrieval of worker ID from /var/lib/deadline/worker.json",
            predicate=got_worker_id,
            interval_s=10,
            max_retries=6,
        )

        assert isinstance(cmd_result, CommandResult)
        cmd_result = cast(CommandResult, cmd_result)
        assert cmd_result.exit_code == 0, f"Failed to get Worker ID: {cmd_result}"

        worker_id = cmd_result.stdout.rstrip("\r\n")
        assert re.match(
            r"^worker-[0-9a-f]{32}$", worker_id
        ), f"Got nonvalid Worker ID from command stdout: {cmd_result}"

        return worker_id

    @property
    def container_id(self) -> str | None:
        return self._container_id


def _handle_subprocess_error(e: Any) -> None:  # pragma: no cover
    if hasattr(e, "stdout"):
        LOG.error(f"Command stdout: {e.stdout}")
    if hasattr(e, "stderr"):
        LOG.error(f"Command stderr: {e.stderr}")
