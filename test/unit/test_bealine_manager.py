# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import os
from typing import Any
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from bealine_test_scaffolding import BealineManager


class TestBealineManager:
    @pytest.fixture(autouse=True)
    def setup_test(self, mock_get_bealine_models):
        pass

    ids = [
        pytest.param(None, None, None, None, id="NoKMSKey"),
        pytest.param({"KeyId": "FakeKMSKeyID"}, None, None, None, id="KMSKeyNoFarm"),
        pytest.param({"KeyId": "FakeKMSKeyID"}, "FakeFarmID", None, None, id="KMSKeyFarmNoFleet"),
        pytest.param(
            {"KeyId": "FakeKMSKeyID"},
            "FakeFarmID",
            "FakeFleetID",
            None,
            id="KMSKeyFarmFleetNoQueue",
        ),
        pytest.param(
            {"KeyId": "FakeKMSKeyID"},
            "FakeFarmID",
            "FakeFleetID",
            "FakeQueueID",
            id="KMSKeyFarmFleetQueue",
        ),
        pytest.param(
            {"KeyId": "FakeKMSKeyID"},
            "FakeFarmID",
            None,
            "FakeQueueID",
            id="KMSKeyFarmQueueNoFleet",
        ),
    ]

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
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
        _: mock.Mock,
        mocked_boto_session: mock.MagicMock,
    ) -> None:
        # GIVEN
        bm = BealineManager()
        bm.farm_id = "TestFarm"
        bm.fleet_id = "TestFleet"
        bm.queue_id = "TestQueue"
        bm.bealine_client = mock.Mock()

        # WHEN
        bm.create_scaffolding()

        # THEN
        assert mocked_sleep.call_count == 3

        mocked_create_kms_key.assert_called_once()
        mocked_create_farm.assert_called_once()
        mocked_create_queue.assert_called_once()
        mocked_create_fleet.assert_called_once()

        bm.bealine_client.update_queue.assert_called_once_with(
            farmId=bm.farm_id, queueId=bm.queue_id, fleets=[{"fleetId": bm.fleet_id, "priority": 1}]
        )

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
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
        _: mock.Mock,
        mocked_boto_session: mock.MagicMock,
        kms_key_metadata: dict[str, Any] | None,
        farm_id: str | None,
        fleet_id: str | None,
        queue_id: str | None,
    ) -> None:
        # GIVEN
        bm = BealineManager()
        bm.kms_key_metadata = kms_key_metadata
        bm.farm_id = farm_id
        bm.fleet_id = fleet_id
        bm.queue_id = queue_id

        # WHEN
        bm.cleanup_scaffolding()

        # THEN
        if kms_key_metadata:
            mocked_delete_kms_key.assert_called_once()

        if farm_id:
            mocked_delete_farm.assert_called_once()

        if queue_id:
            mocked_delete_queue.assert_called_once()

        if fleet_id:
            mocked_delete_fleet.assert_called_once()

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_create_kms_key(self, _: mock.Mock, mocked_boto_session: mock.MagicMock) -> None:
        # GIVEN
        bm = BealineManager()

        fake_kms_metadata = {"KeyMetadata": {"KeyId": "Foo"}}
        bm.kms_client.create_key.return_value = fake_kms_metadata

        # WHEN
        bm.create_kms_key()

        # THEN
        bm.kms_client.create_key.assert_called_once_with(
            Description="The KMS used for testing created by the "
            "BealineClientSoftwareTestScaffolding.",
            Tags=[{"TagKey": "Name", "TagValue": "BealineClientSoftwareTestScaffolding"}],
        )

        assert bm.kms_key_metadata == fake_kms_metadata["KeyMetadata"]

        bm.kms_client.enable_key.assert_called_once_with(
            KeyId=fake_kms_metadata["KeyMetadata"]["KeyId"]
        )

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_delete_kms_key(self, _: mock.Mock, mocked_boto_session: mock.MagicMock) -> None:
        # GIVEN
        fake_kms_metadata = {"KeyId": "Foo"}
        bm = BealineManager()
        bm.kms_key_metadata = fake_kms_metadata

        # WHEN
        bm.delete_kms_key()

        # THEN
        bm.kms_client.schedule_key_deletion.assert_called_once_with(
            KeyId=fake_kms_metadata["KeyId"], PendingWindowInDays=7
        )

        assert bm.kms_key_metadata is None

    key_metadatas = [
        pytest.param(None, id="NoMetadata"),
        pytest.param({"Foo": "Bar"}, id="NoKeyInMetadata"),
    ]

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    @pytest.mark.parametrize("key_metadatas", key_metadatas)
    def test_delete_kms_key_no_key(
        self,
        _: mock.Mock,
        mocked_boto_session: mock.MagicMock,
        key_metadatas: dict[str, Any] | None,
    ) -> None:
        # GIVEN
        bm = BealineManager()
        bm.kms_key_metadata = key_metadatas

        # WHEN / THEN
        with pytest.raises(Exception):
            bm.delete_kms_key()

        assert not bm.kms_client.schedule_key_deletion.called

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_create_farm(self, _: mock.Mock, mocked_boto_session: mock.MagicMock) -> None:
        # GIVEN
        fake_farm_name = "test_farm"
        fake_kms_metadata = {"Arn": "fake_kms_arn"}
        fake_farm_id = "fake_farm_id"

        bm = BealineManager()
        bm.kms_key_metadata = fake_kms_metadata
        bm.bealine_client.create_farm.return_value = {"farmId": fake_farm_id}

        # WHEN
        bm.create_farm(fake_farm_name)

        # THEN
        bm.bealine_client.create_farm.assert_called_once_with(
            name=fake_farm_name, kmsKeyArn=fake_kms_metadata["Arn"]
        )
        assert bm.farm_id == fake_farm_id

    key_metadatas = [
        pytest.param(None, id="NoMetadata"),
        pytest.param({"Foo": "Bar"}, id="NoKeyInMetadata"),
    ]

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    @pytest.mark.parametrize("key_metadatas", key_metadatas)
    def test_create_farm_kms_not_valid(
        self,
        _: mock.Mock,
        mocked_boto_session: mock.MagicMock,
        key_metadatas: dict[str, Any] | None,
    ) -> None:
        # GIVEN
        fake_farm_name = "test_farm"
        bm = BealineManager()
        bm.kms_key_metadata = key_metadatas

        # WHEN / THEN
        with pytest.raises(Exception):
            bm.create_farm(fake_farm_name)

        assert not bm.bealine_client.create_farm.called
        assert bm.farm_id is None

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_delete_farm(self, _: mock.Mock, mocked_boto_session: mock.MagicMock) -> None:
        # GIVEN
        fake_farm_id = "fake_farm_id"
        bm = BealineManager()
        bm.farm_id = fake_farm_id

        # WHEN
        bm.delete_farm()

        # THEN
        bm.bealine_client.delete_farm.assert_called_once_with(farmId=fake_farm_id)

        assert bm.farm_id is None

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_delete_farm_not_created(
        self, _: mock.Mock, mocked_boto_session: mock.MagicMock
    ) -> None:
        # GIVEN
        bm = BealineManager()

        # WHEN / THEN
        with pytest.raises(Exception):
            bm.delete_farm()

        # THEN
        assert not bm.bealine_client.delete_farm.called

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_create_queue(self, _: mock.Mock, mocked_boto_session: mock.MagicMock) -> None:
        # GIVEN
        fake_farm_id = "fake_farm_id"
        fake_queue_name = "fake_queue_name"
        fake_queue_id = "fake_queue_id"
        bm = BealineManager()
        bm.farm_id = fake_farm_id
        bm.bealine_client.create_queue.return_value = {"queueId": fake_queue_id}

        # WHEN
        bm.create_queue(fake_queue_name)

        # THEN
        bm.bealine_client.create_queue.assert_called_once_with(
            name=fake_queue_name,
            farmId=fake_farm_id,
        )

        assert bm.queue_id == fake_queue_id

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_create_queue_no_farm(self, _: mock.Mock, mocked_boto_session: mock.MagicMock) -> None:
        # GIVEN
        fake_queue_name = "fake_queue_name"
        fake_queue_id = "fake_queue_id"
        bm = BealineManager()
        bm.bealine_client.create_queue.return_value = {"queueId": fake_queue_id}

        # WHEN
        with pytest.raises(Exception):
            bm.create_queue(fake_queue_name)

        # THEN
        assert not bm.bealine_client.create_queue.called

        assert bm.queue_id is None

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_delete_queue(self, _: mock.Mock, mocked_boto_session: mock.MagicMock) -> None:
        # GIVEN
        fake_queue_id = "fake_queue_id"
        fake_farm_id = "fake_farm_id"

        bm = BealineManager()
        bm.queue_id = fake_queue_id
        bm.farm_id = fake_farm_id

        # WHEN
        bm.delete_queue()

        # THEN
        bm.bealine_client.delete_queue.assert_called_once_with(
            queueId=fake_queue_id, farmId=fake_farm_id
        )

        assert bm.queue_id is None

    farm_queue_ids = [
        pytest.param("fake_queue_id", None, id="NoFarmId"),
        pytest.param(None, "fake_farm_id", id="NoQueueId"),
    ]

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    @pytest.mark.parametrize("fake_queue_id, fake_farm_id", farm_queue_ids)
    def test_delete_queue_no_farm_queue(
        self,
        _: mock.Mock,
        mocked_boto_session: mock.MagicMock,
        fake_queue_id: str | None,
        fake_farm_id: str | None,
    ) -> None:
        # GIVEN
        bm = BealineManager()
        bm.queue_id = fake_queue_id
        bm.farm_id = fake_farm_id

        # WHEN / THEN
        with pytest.raises(Exception):
            bm.delete_queue()

        assert not bm.bealine_client.delete_queue.called

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_create_fleet(self, _: mock.Mock, mocked_boto_session: mock.MagicMock) -> None:
        # GIVEN
        fake_fleet_name = "fake_fleet_name"
        fake_farm_id = "fake_farm_id"
        fake_fleet_id = "fake_fleet_id"
        bm = BealineManager()
        bm.farm_id = fake_farm_id
        bm.bealine_client.create_fleet.return_value = {"fleetId": fake_fleet_id}

        # WHEN
        bm.create_fleet(fake_fleet_name)

        # THEN
        bm.bealine_client.create_fleet.assert_called_once_with(
            farmId=fake_farm_id, name=fake_fleet_name
        )

        assert bm.fleet_id == fake_fleet_id

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_create_fleet_no_farm(self, _: mock.Mock, mocked_boto_session: mock.MagicMock) -> None:
        # GIVEN
        fake_fleet_name = "fake_fleet_name"
        bm = BealineManager()

        # WHEN / THEN
        with pytest.raises(Exception):
            bm.create_fleet(fake_fleet_name)

        assert not bm.bealine_client.create_fleet.called
        assert bm.fleet_id is None

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    def test_delete_fleet(self, _: mock.Mock, mocked_boto_session: mock.MagicMock) -> None:
        # GIVEN
        fake_farm_id = "fake_farm_id"
        fake_fleet_id = "fake_fleet_id"

        bm = BealineManager()
        bm.farm_id = fake_farm_id
        bm.fleet_id = fake_fleet_id

        # WHEN
        bm.delete_fleet()

        # THEN
        bm.bealine_client.update_fleet.assert_called_once_with(
            farmId=fake_farm_id, fleetId=fake_fleet_id, state="DISABLED"
        )
        bm.bealine_client.delete_fleet.assert_called_once_with(
            farmId=fake_farm_id, fleetId=fake_fleet_id
        )

        assert bm.fleet_id is None

    farm_queue_ids = [
        pytest.param("fake_farm_id", None, id="NoFleetId"),
        pytest.param(None, "fake_fleet_id", id="NoFarmId"),
    ]

    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.Session")
    @mock.patch("bealine_test_scaffolding.bealine_manager.boto3.client")
    @pytest.mark.parametrize("fake_farm_id, fake_fleet_id", farm_queue_ids)
    def test_delete_fleet_no_farm_fleet(
        self,
        _: mock.Mock,
        mocked_boto_session: mock.MagicMock,
        fake_farm_id: str | None,
        fake_fleet_id: str | None,
    ) -> None:
        # GIVEN
        bm = BealineManager()
        bm.farm_id = fake_farm_id
        bm.fleet_id = fake_fleet_id

        # WHEN / THEN
        with pytest.raises(Exception):
            bm.delete_fleet()

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
