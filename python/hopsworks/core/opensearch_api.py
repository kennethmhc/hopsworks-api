#
#   Copyright 2022 Logical Clocks AB
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

import os
from furl import furl

from hopsworks import client, constants


class OpenSearchApi:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name

    def _get_opensearch_url(self):
        return os.environ[constants.ENV_VARS.ELASTIC_ENDPOINT_ENV_VAR]

    def get_project_index(self, index):
        """
        This helper method prefixes the supplied index name with the project name to avoid index name clashes.

        Args:
            :index: the opensearch index to interact with.

        Returns:
            A valid opensearch index name.
        """
        return (self._project_name + "_" + index).lower()

    def get_default_py_config(self):
        """
        Get the required opensearch configuration to setup a connection using the *opensearch-py* library.

        ```python

        import hopsworks
        from opensearchpy import OpenSearch

        connection = hopsworks.connection()

        project = connection.get_project()

        opensearch_api = project.get_opensearch_api()

        client = OpenSearch(**opensearch_api.get_default_py_config())

        ```
        Returns:
            A dictionary with required configuration.
        """
        url = furl(self._get_opensearch_url())
        return {
            constants.OPENSEARCH_CONFIG.HOSTS: [{"host": url.host, "port": url.port}],
            constants.OPENSEARCH_CONFIG.HTTP_COMPRESS: False,
            constants.OPENSEARCH_CONFIG.HEADERS: {
                "Authorization": self._get_authorization_token()
            },
            constants.OPENSEARCH_CONFIG.USE_SSL: True,
            constants.OPENSEARCH_CONFIG.VERIFY_CERTS: True,
            constants.OPENSEARCH_CONFIG.SSL_ASSERT_HOSTNAME: False,
            constants.OPENSEARCH_CONFIG.CA_CERTS: client.get_instance()._get_ca_chain_path(
                self._project_name
            ),
        }

    def _get_authorization_token(self):

        """Get opensearch jwt token.

        # Returns
            `str`: OpenSearch jwt token
        # Raises
            `RestAPIError`: If unable to get the token
        """

        _client = client.get_instance()
        path_params = ["elastic", "jwt", self._project_id]

        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)["token"]
