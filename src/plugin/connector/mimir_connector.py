from spaceone.core.connector import BaseConnector
from spaceone.core.error import ERROR_REQUIRED_PARAMETER


class MimirConnector(BaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mimir_endpoint = None
        self.mimir_headers = {
            "Content-Type": "application/json",
            "X_Scope_OrgID": "",
        }

    def create_session(
        self, domain_id: str, options: dict, secret_data: dict, schema: str = None
    ) -> None:
        self._check_secret_data(secret_data)

        self.mimir_endpoint = secret_data["mimir_endpoint"]
        self.mimir_headers["X_Scope_OrgID"] = secret_data["X_Scope_OrgID"]

    @staticmethod
    def _check_secret_data(secret_data):
        if "mimir_endpoint" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.mimir_endpoint")

        if "X_Scope_OrgID" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.X_Scope_OrgID")
