#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
import pytest
from furl import furl
from hopsworks_common.client import exceptions, online_store_rest_client
from hopsworks_common.client.online_store_rest_client import (
    OnlineStoreRestClientSingleton,
)


class TestOnlineStoreRestClient:
    def test_setup_rest_client_external(self, mocker):
        # Arrange
        online_store_rest_client._online_store_rest_client = None

        mocker.patch("hopsworks_common.client._is_external", return_value=True)
        mocker.patch("hopsworks_common.client.get_instance")
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi.get_loadbalancer_external_domain",
            return_value="app.hopsworks.ai",
        )
        ping_rdrs_mock = mocker.patch(
            "hopsworks_common.client.online_store_rest_client.OnlineStoreRestClientSingleton.is_connected",
        )

        # Act
        online_store_rest_client.init_or_reset_online_store_rest_client()
        online_store_rest_client_instance = online_store_rest_client.get_instance()

        # Assert
        variable_api_mock.assert_called_once()
        assert (
            online_store_rest_client_instance._current_config["host"]
            == "app.hopsworks.ai"
        )
        assert online_store_rest_client_instance._current_config["port"] == 4406
        assert online_store_rest_client_instance._current_config["verify_certs"] is True
        assert online_store_rest_client_instance._base_url == furl(
            "https://app.hopsworks.ai:4406/0.1.0"
        )
        assert ping_rdrs_mock.call_count == 1

    def test_setup_online_store_rest_client_internal(self, mocker):
        # Arrange
        online_store_rest_client._online_store_rest_client = None

        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mocker.patch("hopsworks_common.client.get_instance")
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi.get_service_discovery_domain",
            return_value="consul",
        )
        optional_config = {"api_key": "provided_api_key"}
        ping_rdrs_mock = mocker.patch(
            "hopsworks_common.client.online_store_rest_client.OnlineStoreRestClientSingleton.is_connected",
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException):
            online_store_rest_client.init_or_reset_online_store_rest_client()
        online_store_rest_client.init_or_reset_online_store_rest_client(
            optional_config=optional_config
        )
        online_store_rest_client_instance = online_store_rest_client.get_instance()

        # Assert
        assert variable_api_mock.call_count == 2
        assert (
            online_store_rest_client_instance._current_config["host"]
            == "rdrs.service.consul"
        )
        assert online_store_rest_client_instance._current_config["port"] == 4406
        assert online_store_rest_client_instance._current_config["verify_certs"] is True
        assert online_store_rest_client_instance._base_url == furl(
            "https://rdrs.service.consul:4406/0.1.0"
        )
        assert online_store_rest_client_instance._auth._token == "provided_api_key"
        assert ping_rdrs_mock.call_count == 1


def _create_client_stub(mocker, optional_config=None):
    """Create an OnlineStoreRestClientSingleton with all external deps stubbed out."""
    online_store_rest_client._online_store_rest_client = None
    mocker.patch("hopsworks_common.client._is_external", return_value=False)
    mocker.patch("hopsworks_common.client.get_instance")
    mocker.patch(
        "hopsworks_common.core.variable_api.VariableApi.get_service_discovery_domain",
        return_value="consul",
    )
    mocker.patch(
        "hopsworks_common.client.online_store_rest_client.OnlineStoreRestClientSingleton.is_connected",
    )
    base_config = {"api_key": "test_key"}
    if optional_config:
        base_config.update(optional_config)
    online_store_rest_client.init_or_reset_online_store_rest_client(
        optional_config=base_config
    )
    return online_store_rest_client.get_instance()


class TestHttpEngineConfig:
    def test_default_engine_is_requests(self, mocker):
        instance = _create_client_stub(mocker)
        assert instance._current_config["http_engine"] == "requests"

    def test_config_engine_pycurl(self, mocker):
        instance = _create_client_stub(mocker, {"http_engine": "pycurl"})
        assert instance._current_config["http_engine"] == "pycurl"

    def test_config_engine_invalid_raises(self, mocker):
        with pytest.raises(ValueError, match="Unsupported http_engine 'invalid'"):
            _create_client_stub(mocker, {"http_engine": "invalid"})


class TestSendRequestDispatch:
    def test_send_request_dispatches_to_session_by_default(self, mocker):
        instance = _create_client_stub(mocker)
        session_mock = mocker.patch.object(instance, "send_request_session")
        pycurl_mock = mocker.patch.object(instance, "send_request_pycurl")

        instance.send_request("POST", ["feature_store"], headers={"h": "v"}, data="d")

        session_mock.assert_called_once_with(
            method="POST",
            path_params=["feature_store"],
            headers={"h": "v"},
            data="d",
            profiling_hook=None,
        )
        pycurl_mock.assert_not_called()

    def test_send_request_dispatches_to_pycurl(self, mocker):
        instance = _create_client_stub(mocker, {"http_engine": "pycurl"})
        session_mock = mocker.patch.object(instance, "send_request_session")
        pycurl_mock = mocker.patch.object(instance, "send_request_pycurl")

        instance.send_request("GET", ["ping"])

        pycurl_mock.assert_called_once_with(
            method="GET",
            path_params=["ping"],
            headers=None,
            data=None,
            profiling_hook=None,
        )
        session_mock.assert_not_called()

    def test_send_request_passes_profiling_hook(self, mocker):
        instance = _create_client_stub(mocker)
        session_mock = mocker.patch.object(instance, "send_request_session")

        hook = mocker.Mock()
        instance.send_request("GET", ["ping"], profiling_hook=hook)

        session_mock.assert_called_once_with(
            method="GET",
            path_params=["ping"],
            headers=None,
            data=None,
            profiling_hook=hook,
        )


class TestSendRequestSession:
    def test_send_request_session_builds_url_and_sends(self, mocker):
        instance = _create_client_stub(mocker)
        mock_response = mocker.Mock()
        mock_response.elapsed.total_seconds.return_value = 0.05
        send_mock = mocker.patch.object(
            instance._session, "send", return_value=mock_response
        )

        response = instance.send_request_session("POST", ["feature_store"])

        assert response is mock_response
        send_mock.assert_called_once()
        prepped = send_mock.call_args[0][0]
        assert "feature_store" in prepped.url

    def test_send_request_session_profiling_hooks_called(self, mocker):
        instance = _create_client_stub(mocker)
        mock_response = mocker.Mock()
        mock_response.elapsed.total_seconds.return_value = 0.01
        mocker.patch.object(
            instance._session, "send", return_value=mock_response
        )

        recorded = {}

        def hook(label, value):
            recorded[label] = value

        instance.send_request_session("GET", ["ping"], profiling_hook=hook)

        assert "http_build_url" in recorded
        assert "http_prepare_request" in recorded
        assert "http_send" in recorded
        assert "http_ttfb" in recorded
        assert "http_body_download" in recorded


class TestSendRequestPycurl:
    def test_send_request_pycurl_returns_response(self, mocker):
        instance = _create_client_stub(mocker, {"http_engine": "pycurl"})

        mock_curl = mocker.Mock()
        mock_curl.TOTAL_TIME = 1
        mock_curl.STARTTRANSFER_TIME = 2
        mock_curl.RESPONSE_CODE = 3
        mock_curl.URL = 10001
        mock_curl.WRITEDATA = 10002
        mock_curl.HEADERFUNCTION = 10003
        mock_curl.POST = 10004
        mock_curl.POSTFIELDS = 10005
        mock_curl.HTTPHEADER = 10006
        mock_curl.SSL_VERIFYPEER = 10007
        mock_curl.SSL_VERIFYHOST = 10008
        mock_curl.CAINFO = 10009
        mock_curl.TIMEOUT = 10010
        mock_curl.getinfo.side_effect = lambda key: {
            1: 0.123,
            2: 0.05,
            3: 200,
        }[key]

        mock_pycurl_module = mocker.Mock()
        mock_pycurl_module.Curl.return_value = mock_curl
        mocker.patch.dict("sys.modules", {"pycurl": mock_pycurl_module})

        response = instance.send_request_pycurl(
            "POST",
            ["feature_store"],
            headers={"Content-Type": "application/json"},
            data='{"key": "value"}',
        )

        mock_curl.perform.assert_called_once()
        mock_curl.close.assert_not_called()
        assert response.status_code == 200
        assert "feature_store" in response.url

    def test_send_request_pycurl_sends_api_key_header(self, mocker):
        instance = _create_client_stub(mocker, {"http_engine": "pycurl"})

        mock_curl = mocker.Mock()
        mock_curl.TOTAL_TIME = 1
        mock_curl.STARTTRANSFER_TIME = 2
        mock_curl.RESPONSE_CODE = 3
        mock_curl.URL = 10001
        mock_curl.WRITEDATA = 10002
        mock_curl.HEADERFUNCTION = 10003
        mock_curl.HTTPGET = 10004
        mock_curl.HTTPHEADER = 10006
        mock_curl.SSL_VERIFYPEER = 10007
        mock_curl.SSL_VERIFYHOST = 10008
        mock_curl.CAINFO = 10009
        mock_curl.TIMEOUT = 10010
        mock_curl.getinfo.side_effect = lambda key: {
            1: 0.1, 2: 0.05, 3: 200
        }[key]

        mock_pycurl_module = mocker.Mock()
        mock_pycurl_module.Curl.return_value = mock_curl
        mocker.patch.dict("sys.modules", {"pycurl": mock_pycurl_module})

        instance.send_request_pycurl("GET", ["ping"])

        # Find the HTTPHEADER setopt call and verify X-API-KEY is present
        httpheader_calls = [
            call[0][1]
            for call in mock_curl.setopt.call_args_list
            if call[0][0] == mock_curl.HTTPHEADER
        ]
        assert len(httpheader_calls) == 1
        header_list = httpheader_calls[0]
        api_key_headers = [h for h in header_list if h.startswith("X-API-KEY:")]
        assert len(api_key_headers) == 1
        assert "test_key" in api_key_headers[0]

    def test_send_request_pycurl_ssl_verify_disabled(self, mocker):
        instance = _create_client_stub(
            mocker, {"http_engine": "pycurl", "verify_certs": False}
        )

        mock_curl = mocker.Mock()
        mock_curl.TOTAL_TIME = 1
        mock_curl.STARTTRANSFER_TIME = 2
        mock_curl.RESPONSE_CODE = 3
        mock_curl.URL = 10001
        mock_curl.WRITEDATA = 10002
        mock_curl.HEADERFUNCTION = 10003
        mock_curl.HTTPGET = 10004
        mock_curl.SSL_VERIFYPEER = 10007
        mock_curl.SSL_VERIFYHOST = 10008
        mock_curl.TIMEOUT = 10010
        mock_curl.getinfo.side_effect = lambda key: {
            1: 0.1, 2: 0.05, 3: 200
        }[key]

        mock_pycurl_module = mocker.Mock()
        mock_pycurl_module.Curl.return_value = mock_curl
        mocker.patch.dict("sys.modules", {"pycurl": mock_pycurl_module})

        instance.send_request_pycurl("GET", ["ping"])

        setopt_calls = {call[0][0]: call[0][1] for call in mock_curl.setopt.call_args_list}
        assert setopt_calls[mock_curl.SSL_VERIFYPEER] == 0
        assert setopt_calls[mock_curl.SSL_VERIFYHOST] == 0

    def test_send_request_pycurl_ssl_verify_with_ca_certs(self, mocker):
        instance = _create_client_stub(
            mocker, {"http_engine": "pycurl", "verify_certs": True}
        )

        mock_curl = mocker.Mock()
        mock_curl.TOTAL_TIME = 1
        mock_curl.STARTTRANSFER_TIME = 2
        mock_curl.RESPONSE_CODE = 3
        mock_curl.URL = 10001
        mock_curl.WRITEDATA = 10002
        mock_curl.HEADERFUNCTION = 10003
        mock_curl.HTTPGET = 10004
        mock_curl.SSL_VERIFYPEER = 10007
        mock_curl.SSL_VERIFYHOST = 10008
        mock_curl.CAINFO = 10009
        mock_curl.TIMEOUT = 10010
        mock_curl.getinfo.side_effect = lambda key: {
            1: 0.1, 2: 0.05, 3: 200
        }[key]

        mock_pycurl_module = mocker.Mock()
        mock_pycurl_module.Curl.return_value = mock_curl
        mocker.patch.dict("sys.modules", {"pycurl": mock_pycurl_module})

        instance.send_request_pycurl("GET", ["ping"])

        setopt_calls = {call[0][0]: call[0][1] for call in mock_curl.setopt.call_args_list}
        assert setopt_calls[mock_curl.SSL_VERIFYPEER] == 1
        assert setopt_calls[mock_curl.SSL_VERIFYHOST] == 2
        assert mock_curl.CAINFO in setopt_calls

    def test_send_request_pycurl_profiling_hooks_called(self, mocker):
        instance = _create_client_stub(mocker, {"http_engine": "pycurl"})

        mock_curl = mocker.Mock()
        mock_curl.TOTAL_TIME = 1
        mock_curl.STARTTRANSFER_TIME = 2
        mock_curl.RESPONSE_CODE = 3
        mock_curl.URL = 10001
        mock_curl.WRITEDATA = 10002
        mock_curl.HEADERFUNCTION = 10003
        mock_curl.HTTPGET = 10004
        mock_curl.SSL_VERIFYPEER = 10007
        mock_curl.SSL_VERIFYHOST = 10008
        mock_curl.CAINFO = 10009
        mock_curl.TIMEOUT = 10010
        mock_curl.getinfo.side_effect = lambda key: {
            1: 0.123, 2: 0.05, 3: 200
        }[key]

        mock_pycurl_module = mocker.Mock()
        mock_pycurl_module.Curl.return_value = mock_curl
        mocker.patch.dict("sys.modules", {"pycurl": mock_pycurl_module})

        recorded = {}

        def hook(label, value):
            recorded[label] = value

        instance.send_request_pycurl("GET", ["ping"], profiling_hook=hook)

        assert "http_build_url" in recorded
        assert "http_prepare_request" in recorded
        assert "http_send" in recorded
        assert recorded["http_send"] == 0.123
        assert "http_ttfb" in recorded
        assert recorded["http_ttfb"] == 0.05
        assert "http_body_download" in recorded
        assert recorded["http_body_download"] == pytest.approx(0.123 - 0.05)

    def test_send_request_pycurl_error_raises_request_exception(self, mocker):
        import requests

        instance = _create_client_stub(mocker, {"http_engine": "pycurl"})

        mock_pycurl_error = type("error", (Exception,), {})

        mock_curl = mocker.Mock()
        mock_curl.URL = 10001
        mock_curl.WRITEDATA = 10002
        mock_curl.HEADERFUNCTION = 10003
        mock_curl.HTTPGET = 10004
        mock_curl.SSL_VERIFYPEER = 10007
        mock_curl.SSL_VERIFYHOST = 10008
        mock_curl.CAINFO = 10009
        mock_curl.TIMEOUT = 10010
        mock_curl.perform.side_effect = mock_pycurl_error("(60, 'SSL error')")

        mock_pycurl_module = mocker.Mock()
        mock_pycurl_module.Curl.return_value = mock_curl
        mock_pycurl_module.error = mock_pycurl_error
        mocker.patch.dict("sys.modules", {"pycurl": mock_pycurl_module})

        with pytest.raises(requests.RequestException, match="PycURL error"):
            instance.send_request_pycurl("GET", ["ping"])
