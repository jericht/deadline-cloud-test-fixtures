# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import os

STAGE = os.environ.get("STAGE", "Prod")

BOOTSTRAP_CLOUDFORMATION_STACK_NAME = f"TestScaffoldingStack{STAGE}"

# Role Names
BEALINE_WORKER_BOOTSTRAP_ROLE = f"BealineWorkerBootstrapRole{STAGE}"
BEALINE_WORKER_BOOSTRAP_INSTANCE_PROFILE_NAME = f"BealineWorkerBootstrapInstanceProfile{STAGE}"
BEALINE_WORKER_ROLE = f"BealineWorkerTestRole{STAGE}"
BEALINE_QUEUE_SESSION_ROLE = f"BealineScaffoldingQueueSessionRole{STAGE}"

# Job Attachments
JOB_ATTACHMENTS_BUCKET_RESOURCE = "ScaffoldingJobAttachmentsBucket"
JOB_ATTACHMENTS_BUCKET_NAME = os.environ.get(
    "JOB_ATTACHMENTS_BUCKET_NAME", "scaffolding-job-attachments-bucket"
)
JOB_ATTACHMENTS_BUCKET_POLICY_RESOURCE = f"JobAttachmentsPolicy{STAGE}"
JOB_ATTACHMENTS_ROOT_PREFIX = "root"
JOB_ATTACHMENTS_CAS_PREFIX = "cas"
JOB_ATTACHMENTS_OUTPUT_PREFIX = "output"

# Worker Agent Configurations
DEFAULT_CMF_CONFIG = {
    "customerManaged": {
        "autoScalingConfiguration": {
            "autoScalingMode": "NO_SCALING",
        },
        "workerRequirements": {
            "vCpuCount": {"min": 1},
            "memoryMiB": {"min": 1024},
            "osFamily": "linux",
            "cpuArchitectureType": "x86_64",
        },
    }
}

# Service Principals
CREDENTIAL_VENDING_PRINCIPAL = os.environ.get(
    "CREDENTIAL_VENDING_PRINCIPAL", "credential-vending.bealine-closed-beta.amazonaws.com"
)

# Temporary constants
BEALINE_SERVICE_MODEL_BUCKET = os.environ.get("BEALINE_SERVICE_MODEL_BUCKET", "")
CODEARTIFACT_DOMAIN = os.environ.get("CODEARTIFACT_DOMAIN", "")
CODEARTIFACT_ACCOUNT_ID = os.environ.get("CODEARTIFACT_ACCOUNT_ID", "")
CODEARTIFACT_REPOSITORY = os.environ.get("CODEARTIFACT_REPOSITORY", "")
