# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import os
from typing import Any
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from bealine_test_scaffolding import BealineManager

from shared_constants import (
    MOCK_FARM_ID,
    MOCK_FARM_NAME,
    MOCK_FLEET_ID,
    MOCK_FLEET_NAME,
    MOCK_QUEUE_ID,
    MOCK_QUEUE_NAME,
)


class TestBealineManager:
    @pytest.fixture(autouse=True)
    def setup_test(self, mock_get_bealine_models):
        pass

    @pytest.fixture(scope="function")
    def mock_bealine_manager(self) -> BealineManager:
        """
        Returns a BealineManager where any boto3 clients are mocked, including
        the bealine_client that is part of the BealineManager.
        """
        with mock.patch.object(BealineManager, "_get_bealine_client"), mock.patch(
            "bealine_test_scaffolding.bealine_manager.boto3.client"
        ):
            return BealineManager()

    ids = [
        pytest.param(None, None, None, None, id="NoKMSKey"),
        pytest.param({"KeyId": "FakeKMSKeyID"}, None, None, None, id="KMSKeyNoFarm"),
        pytest.param({"KeyId": "FakeKMSKeyID"}, MOCK_FARM_ID, None, None, id="KMSKeyFarmNoFleet"),
        pytest.param(
            {"KeyId": "FakeKMSKeyID"},
            MOCK_FARM_ID,
            MOCK_FLEET_ID,
            None,
            id="KMSKeyFarmFleetNoQueue",
        ),
        pytest.param(
            {"KeyId": "FakeKMSKeyID"},
            MOCK_FARM_ID,
            MOCK_FLEET_ID,
            MOCK_QUEUE_ID,
            id="KMSKeyFarmFleetQueue",
        ),
        pytest.param(
            {"KeyId": "FakeKMSKeyID"},
            MOCK_FARM_ID,
            None,
            MOCK_QUEUE_ID,
            id="KMSKeyFarmQueueNoFleet",
        ),
    ]

    @mock.patch("bealine_test_scaffolding.bealine_manager.sleep")
    @mock.patch.object(BealineManager, "create_fleet")
    @mock.patch.object(BealineManager, "create_queue")
    @mock.patch.object(BealineManager, "create_farm")
    @mock.patch.object(BealineManager, "create_kms_key")
    def test_create_scaffolding(
        self,
        mocked_create_kms_key: mock.Mock,
        mocked_create_farm: mock.Mock,
        mocked_create_queue: mock.Mock,
        mocked_create_fleet: mock.Mock,
        mocked_sleep: mock.Mock,
        mock_bealine_manager: BealineManager,
    ) -> None:
        # GIVEN
        mock_bealine_manager.farm_id = MOCK_FARM_ID
        mock_bealine_manager.fleet_id = MOCK_FLEET_ID
        mock_bealine_manager.queue_id = MOCK_QUEUE_ID

        # WHEN
        mock_bealine_manager.create_scaffolding()

        # THEN
        assert mocked_sleep.call_count == 3

        mocked_create_kms_key.assert_called_once()
        mocked_create_farm.assert_called_once()
        mocked_create_queue.assert_called_once()
        mocked_create_fleet.assert_called_once()

        mock_bealine_manager.bealine_client.update_queue.assert_called_once_with(
            farmId=mock_bealine_manager.farm_id,
            queueId=mock_bealine_manager.queue_id,
            fleets=[{"fleetId": mock_bealine_manager.fleet_id, "priority": 1}],
        )

    @mock.patch.object(BealineManager, "delete_fleet")
    @mock.patch.object(BealineManager, "delete_queue")
    @mock.patch.object(BealineManager, "delete_farm")
    @mock.patch.object(BealineManager, "delete_kms_key")
    @pytest.mark.parametrize("kms_key_metadata, farm_id, fleet_id, queue_id", ids)
    def test_cleanup_scaffolding(
        self,
        mocked_delete_kms_key: mock.Mock,
        mocked_delete_farm: mock.Mock,
        mocked_delete_queue: mock.Mock,
        mocked_delete_fleet: mock.Mock,
        kms_key_metadata: dict[str, Any] | None,
        farm_id: str | None,
        fleet_id: str | None,
        queue_id: str | None,
        mock_bealine_manager: BealineManager,
    ) -> None:
        # GIVEN
        mock_bealine_manager.kms_key_metadata = kms_key_metadata
        mock_bealine_manager.farm_id = farm_id
        mock_bealine_manager.fleet_id = fleet_id
        mock_bealine_manager.queue_id = queue_id

        # WHEN
        mock_bealine_manager.cleanup_scaffolding()

        # THEN
        if kms_key_metadata:
            mocked_delete_kms_key.assert_called_once()

        if farm_id:
            mocked_delete_farm.assert_called_once()

        if queue_id:
            mocked_delete_queue.assert_called_once()

        if fleet_id:
            mocked_delete_fleet.assert_called_once()

    def test_create_kms_key(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        fake_kms_metadata = {"KeyMetadata": {"KeyId": "Foo"}}
        mock_bealine_manager.kms_client.create_key.return_value = fake_kms_metadata

        # WHEN
        mock_bealine_manager.create_kms_key()

        # THEN
        mock_bealine_manager.kms_client.create_key.assert_called_once_with(
            Description="The KMS used for testing created by the "
            "BealineClientSoftwareTestScaffolding.",
            Tags=[{"TagKey": "Name", "TagValue": "BealineClientSoftwareTestScaffolding"}],
        )

        assert mock_bealine_manager.kms_key_metadata == fake_kms_metadata["KeyMetadata"]

        mock_bealine_manager.kms_client.enable_key.assert_called_once_with(
            KeyId=fake_kms_metadata["KeyMetadata"]["KeyId"]
        )

    def test_delete_kms_key(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        fake_kms_metadata = {"KeyId": "Foo"}
        mock_bealine_manager.kms_key_metadata = fake_kms_metadata

        # WHEN
        mock_bealine_manager.delete_kms_key()

        # THEN
        mock_bealine_manager.kms_client.schedule_key_deletion.assert_called_once_with(
            KeyId=fake_kms_metadata["KeyId"], PendingWindowInDays=7
        )

        assert mock_bealine_manager.kms_key_metadata is None

    key_metadatas = [
        pytest.param(None, id="NoMetadata"),
        pytest.param({"Foo": "Bar"}, id="NoKeyInMetadata"),
    ]

    @pytest.mark.parametrize("key_metadatas", key_metadatas)
    def test_delete_kms_key_no_key(
        self,
        key_metadatas: dict[str, Any] | None,
        mock_bealine_manager: BealineManager,
    ) -> None:
        # GIVEN
        mock_bealine_manager.kms_key_metadata = key_metadatas

        # WHEN / THEN
        with pytest.raises(Exception):
            mock_bealine_manager.delete_kms_key()

        assert not mock_bealine_manager.kms_client.schedule_key_deletion.called

    def test_create_farm(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        fake_kms_metadata = {"Arn": "fake_kms_arn"}

        mock_bealine_manager.kms_key_metadata = fake_kms_metadata
        mock_bealine_manager.bealine_client.create_farm.return_value = {"farmId": MOCK_FARM_ID}  # type: ignore[attr-defined]

        # WHEN
        mock_bealine_manager.create_farm(MOCK_FARM_NAME)

        # THEN
        mock_bealine_manager.bealine_client.create_farm.assert_called_once_with(  # type: ignore[attr-defined] # noqa
            displayName=MOCK_FARM_NAME, kmsKeyArn=fake_kms_metadata["Arn"]
        )
        assert mock_bealine_manager.farm_id == MOCK_FARM_ID

    key_metadatas = [
        pytest.param(None, id="NoMetadata"),
        pytest.param({"Foo": "Bar"}, id="NoKeyInMetadata"),
    ]

    @pytest.mark.parametrize("key_metadatas", key_metadatas)
    def test_create_farm_kms_not_valid(
        self,
        key_metadatas: dict[str, Any] | None,
        mock_bealine_manager: BealineManager,
    ) -> None:
        # GIVEN
        mock_bealine_manager.kms_key_metadata = key_metadatas

        # WHEN / THEN
        with pytest.raises(Exception):
            mock_bealine_manager.create_farm(MOCK_FARM_NAME)

        assert not mock_bealine_manager.bealine_client.create_farm.called  # type: ignore[attr-defined] # noqa
        assert mock_bealine_manager.farm_id is None

    def test_delete_farm(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        mock_bealine_manager.farm_id = MOCK_FARM_ID

        # WHEN
        mock_bealine_manager.delete_farm()

        # THEN
        mock_bealine_manager.bealine_client.delete_farm.assert_called_once_with(farmId=MOCK_FARM_ID)

        assert mock_bealine_manager.farm_id is None

    def test_delete_farm_not_created(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        # mock_bealine_manager fixture
        # WHEN / THEN
        with pytest.raises(Exception):
            mock_bealine_manager.delete_farm()

        # THEN
        assert not mock_bealine_manager.bealine_client.delete_farm.called

    def test_create_queue(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        mock_bealine_manager.farm_id = MOCK_FARM_ID
        mock_bealine_manager.bealine_client.create_queue.return_value = {"queueId": MOCK_QUEUE_ID}

        # WHEN
        mock_bealine_manager.create_queue(MOCK_QUEUE_NAME)

        # THEN
        mock_bealine_manager.bealine_client.create_queue.assert_called_once_with(
            name=MOCK_QUEUE_NAME,
            farmId=MOCK_FARM_ID,
        )

        assert mock_bealine_manager.queue_id == MOCK_QUEUE_ID

    def test_create_queue_no_farm(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        mock_bealine_manager.bealine_client.create_queue.return_value = {"queueId": MOCK_QUEUE_ID}

        # WHEN
        with pytest.raises(Exception):
            mock_bealine_manager.create_queue(MOCK_QUEUE_NAME)

        # THEN
        assert not mock_bealine_manager.bealine_client.create_queue.called

        assert mock_bealine_manager.queue_id is None

    def test_delete_queue(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        mock_bealine_manager.queue_id = MOCK_QUEUE_ID
        mock_bealine_manager.farm_id = MOCK_FARM_ID

        # WHEN
        mock_bealine_manager.delete_queue()

        # THEN
        mock_bealine_manager.bealine_client.delete_queue.assert_called_once_with(
            queueId=MOCK_QUEUE_ID, farmId=MOCK_FARM_ID
        )

        assert mock_bealine_manager.queue_id is None

    farm_queue_ids = [
        pytest.param(MOCK_QUEUE_ID, None, id="NoFarmId"),
        pytest.param(None, MOCK_FARM_ID, id="NoQueueId"),
    ]

    @pytest.mark.parametrize("fake_queue_id, fake_farm_id", farm_queue_ids)
    def test_delete_queue_no_farm_queue(
        self,
        fake_queue_id: str | None,
        fake_farm_id: str | None,
        mock_bealine_manager: BealineManager,
    ) -> None:
        # GIVEN
        mock_bealine_manager.queue_id = fake_queue_id
        mock_bealine_manager.farm_id = fake_farm_id

        # WHEN / THEN
        with pytest.raises(Exception):
            mock_bealine_manager.delete_queue()

        assert not mock_bealine_manager.bealine_client.delete_queue.called

    def test_create_fleet(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        mock_bealine_manager.farm_id = MOCK_FARM_ID
        mock_bealine_manager.bealine_client.create_fleet.return_value = {"fleetId": MOCK_FLEET_ID}

        # WHEN
        mock_bealine_manager.create_fleet(MOCK_FLEET_NAME)

        # THEN
        mock_bealine_manager.bealine_client.create_fleet.assert_called_once_with(
            farmId=MOCK_FARM_ID, name=MOCK_FLEET_NAME
        )

        assert mock_bealine_manager.fleet_id == MOCK_FLEET_ID

    def test_create_fleet_no_farm(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        # mock_bealine_manager fixture

        # WHEN / THEN
        with pytest.raises(Exception):
            mock_bealine_manager.create_fleet(MOCK_FLEET_NAME)

        assert not mock_bealine_manager.bealine_client.create_fleet.called
        assert mock_bealine_manager.fleet_id is None

    def test_delete_fleet(self, mock_bealine_manager: BealineManager) -> None:
        # GIVEN
        mock_bealine_manager.farm_id = MOCK_FARM_ID
        mock_bealine_manager.fleet_id = MOCK_FLEET_ID

        # WHEN
        mock_bealine_manager.delete_fleet()

        # THEN
        mock_bealine_manager.bealine_client.update_fleet.assert_called_once_with(
            farmId=MOCK_FARM_ID, fleetId=MOCK_FLEET_ID, state="DISABLED"
        )
        mock_bealine_manager.bealine_client.delete_fleet.assert_called_once_with(
            farmId=MOCK_FARM_ID, fleetId=MOCK_FLEET_ID
        )

        assert mock_bealine_manager.fleet_id is None

    farm_queue_ids = [
        pytest.param(MOCK_FARM_ID, None, id="NoFleetId"),
        pytest.param(None, MOCK_FLEET_ID, id="NoFarmId"),
    ]

    @pytest.mark.parametrize("fake_farm_id, fake_fleet_id", farm_queue_ids)
    def test_delete_fleet_no_farm_fleet(
        self,
        fake_farm_id: str | None,
        fake_fleet_id: str | None,
        mock_bealine_manager: BealineManager,
    ) -> None:
        # GIVEN
        mock_bealine_manager.farm_id = fake_farm_id
        mock_bealine_manager.fleet_id = fake_fleet_id

        # WHEN / THEN
        with pytest.raises(Exception):
            mock_bealine_manager.delete_fleet()

    farm_queue_ids = [
        pytest.param(
            "kms_client",
            {},
            "create_key",
            "create_kms_key",
            [],
            "kms_key_metadata",
            id="FailedCreateKMSKey",
        ),
        pytest.param(
            "kms_client",
            {"kms_key_metadata": {"KeyId": "TestKeyId"}},
            "schedule_key_deletion",
            "delete_kms_key",
            [],
            None,
            id="FailedDeleteKMSKey",
        ),
        pytest.param(
            "bealine_client",
            {"kms_key_metadata": {"Arn": "TestArn"}},
            "create_farm",
            "create_farm",
            ["TestFarm"],
            "farm_id",
            id="FailedCreateFarm",
        ),
        pytest.param(
            "bealine_client",
            {"farm_id": "fake_farm_id"},
            "delete_farm",
            "delete_farm",
            [],
            None,
            id="FailedDeleteFarm",
        ),
        pytest.param(
            "bealine_client",
            {"farm_id": "fake_farm_id"},
            "create_queue",
            "create_queue",
            ["TestQueue"],
            "queue_id",
            id="FailedCreateQueue",
        ),
        pytest.param(
            "bealine_client",
            {"farm_id": "fake_farm_id", "queue_id": "fake_queue_id"},
            "delete_queue",
            "delete_queue",
            [],
            None,
            id="FailedDeleteQueue",
        ),
        pytest.param(
            "bealine_client",
            {"farm_id": "fake_farm_id"},
            "create_fleet",
            "create_fleet",
            ["TestFleet"],
            "fleet_id",
            id="FailedCreateFleet",
        ),
        pytest.param(
            "bealine_client",
            {"farm_id": "fake_farm_id", "fleet_id": "fake_fleet_id"},
            "delete_fleet",
            "delete_fleet",
            [],
            None,
            id="FailedDeleteFleet",
        ),
    ]

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    @pytest.mark.parametrize(
        "client, bm_properties, client_function_name, manager_function_name, args,"
        "expected_parameter",
        farm_queue_ids,
    )
    def test_failure_with_boto(
        self,
        _: mock.Mock,
        mocked_boto_session: mock.MagicMock,
        client: str,
        bm_properties: dict[str, Any],
        client_function_name: str,
        manager_function_name: str,
        args: list[Any],
        expected_parameter: str,
    ) -> None:
        """This test will confirm that when a ClientError is raised when we use the boto3
        clients for bealine and kms

        Args:
            _ (mock.Mock): _description_
            client (str): _description_
            bm_properties (dict[str, Any]): _description_
            client_function_name (str): _description_
            manager_function_name (str): _description_
            args (list[Any]): _description_
            expected_parameter (str): _description_
        """

        # GIVEN
        mocked_function = mock.Mock(
            side_effect=ClientError(
                {
                    "Error": {
                        "Code": "TestException",
                        "Message": "This is a test exception to simulate an exception being "
                        "raised.",
                    }
                },
                "TestException",
            )
        )
        mocked_client = mock.Mock()
        setattr(mocked_client, client_function_name, mocked_function)

        bm = BealineManager()
        setattr(bm, client, mocked_client)

        for property, value in bm_properties.items():
            setattr(bm, property, value)

        # WHEN
        with pytest.raises(ClientError):
            manager_function = getattr(bm, manager_function_name)
            manager_function(*args)

        # THEN
        if expected_parameter:
            assert getattr(bm, expected_parameter) is None


class TestBealineManagerAddModels:
    """This class is here because the tests above are mocking out the add_bealine_models method
    using a fixture."""

    @mock.patch.dict(os.environ, {"BEALINE_SERVICE_MODEL_BUCKET": "test-bucket"})
    @mock.patch("os.makedirs")
    @mock.patch("tempfile.TemporaryDirectory")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_get_bealine_models(
        self,
        mocked_boto_client: mock.MagicMock,
        mocked_boto_session: mock.MagicMock,
        mocked_temp_dir: mock.MagicMock,
        mocked_mkdir: mock.MagicMock,
    ):
        # GIVEN
        temp_path = "/tmp/test"
        mocked_temp_dir.return_value.name = temp_path

        # WHEN
        manager = BealineManager(should_add_bealine_models=True)

        # THEN
        mocked_boto_client.assert_any_call("s3")
        mocked_temp_dir.assert_called_once()
        mocked_mkdir.assert_called_once_with(
            f"{temp_path}/bealine/{BealineManager.MOCKED_SERVICE_VERSION}"
        )
        mocked_boto_client.return_value.download_file.assert_called_with(
            "test-bucket",
            "service-2.json",
            f"{temp_path}/bealine/{BealineManager.MOCKED_SERVICE_VERSION}/service-2.json",
        )
        mocked_boto_session.return_value.client.assert_called_with("bealine", endpoint_url=None)
        assert manager.bealine_model_dir is not None
        assert manager.bealine_model_dir.name == temp_path
