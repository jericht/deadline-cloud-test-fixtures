# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import pytest
from unittest.mock import MagicMock, patch
from bealine_test_scaffolding import BealineClient
from shared_constants import MOCK_FARM_NAME, MOCK_FLEET_NAME, MOCK_QUEUE_NAME


class FakeClient:
    def fake_bealine_client_has_this(self) -> str:
        return "from fake client"

    def but_not_this(self) -> str:
        return "from fake client"


class FakeBealineClient(BealineClient):
    def fake_bealine_client_has_this(self) -> str:
        return "from fake bealine client"


class TestBealineShim:
    def test_bealine_client_pass_through(self) -> None:
        """
        Confirm that BealineClient passes through unknown methods to the underlying client
        but just executes known methods.
        """
        fake_client = FakeClient()
        bealine_client = FakeBealineClient(fake_client)

        assert bealine_client.fake_bealine_client_has_this() == "from fake bealine client"
        assert bealine_client.but_not_this() == "from fake client"

    @pytest.mark.parametrize(
        "kwargs_input, name_in_model, kwargs_output",
        [
            pytest.param(
                {"displayName": MOCK_FARM_NAME},
                "name",
                {"name": MOCK_FARM_NAME},
                id="DisplayNameInSubmissionNotModel",
            ),
            pytest.param(
                {"displayName": MOCK_FARM_NAME},
                "displayName",
                {"displayName": MOCK_FARM_NAME},
                id="DisplayNameInSubmissionAndModel",
            ),
        ],
    )
    def test_create_farm_name_to_display_name(
        self, kwargs_input, name_in_model, kwargs_output
    ) -> None:
        """
        create_farm will be updated so that name is renamed to displayName. Here we
        make sure that the shim is doing its job of:
        1. Calling the underlying client method
        2. Replacing the appropriate key if needed
        """
        fake_client = MagicMock()
        bealine_client = BealineClient(fake_client)

        with patch.object(bealine_client, "_get_bealine_api_input_shape") as input_shape_mock:
            input_shape_mock.return_value = {name_in_model: MOCK_FARM_NAME}
            bealine_client.create_farm(**kwargs_input)
        fake_client.create_farm.assert_called_once_with(**kwargs_output)

    @pytest.mark.parametrize(
        "kwargs_input, name_in_model, kwargs_output",
        [
            pytest.param(
                {"displayName": MOCK_FLEET_NAME},
                "name",
                {"name": MOCK_FLEET_NAME},
                id="DisplayNameInSubmissionNotModel",
            ),
            pytest.param(
                {"displayName": MOCK_FLEET_NAME},
                "displayName",
                {"displayName": MOCK_FLEET_NAME},
                id="DisplayNameInSubmissionAndModel",
            ),
        ],
    )
    def test_create_fleet_name_to_display_name(
        self, kwargs_input, name_in_model, kwargs_output
    ) -> None:
        """
        create_fleet will be updated so that name is renamed to displayName.
        Here we make sure that the shim is doing its job of:
        1. Calling the underlying client method
        2. Replacing the appropriate key if needed
        """
        fake_client = MagicMock()
        bealine_client = BealineClient(fake_client)

        with patch.object(bealine_client, "_get_bealine_api_input_shape") as input_shape_mock:
            input_shape_mock.return_value = {name_in_model: MOCK_FLEET_NAME}
            bealine_client.create_fleet(**kwargs_input)
        fake_client.create_fleet.assert_called_once_with(**kwargs_output)

    @pytest.mark.parametrize(
        "kwargs_input, name_in_model, kwargs_output",
        [
            pytest.param(
                {"displayName": MOCK_QUEUE_NAME},
                "name",
                {"name": MOCK_QUEUE_NAME},
                id="DisplayNameInSubmissionNotModel",
            ),
            pytest.param(
                {"displayName": MOCK_QUEUE_NAME},
                "displayName",
                {"displayName": MOCK_QUEUE_NAME},
                id="DisplayNameInSubmissionAndModel",
            ),
        ],
    )
    def test_create_queue_name_to_display_name(
        self, kwargs_input, name_in_model, kwargs_output
    ) -> None:
        """
        create_queue will be updated so that name is renamed to displayName.
        Here we make sure that the shim is doing its job of:
        1. Calling the underlying client method
        2. Replacing the appropriate key if needed
        """
        fake_client = MagicMock()
        bealine_client = BealineClient(fake_client)

        with patch.object(bealine_client, "_get_bealine_api_input_shape") as input_shape_mock:
            input_shape_mock.return_value = {name_in_model: MOCK_QUEUE_NAME}
            bealine_client.create_queue(**kwargs_input)
        fake_client.create_queue.assert_called_once_with(**kwargs_output)
