import logging
from typing import Generator, List, Union

import pandas as pd
import requests
from pandas._libs.tslibs.offsets import MonthEnd
from spaceone.core.connector import BaseConnector
from spaceone.core.error import ERROR_REQUIRED_PARAMETER

_LOGGER = logging.getLogger("spaceone")
_PAGE_SIZE = 1000

__all__ = ["MimirConnector"]


class MimirConnector(BaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.mimir_endpoint = None
        self.mimir_headers = {
            "Content-Type": "application/json",
            "X-Scope-OrgID": "",
        }
        self.mimir_promql = None

        self.field_mapper = None
        self.default_vars = None
        self.client = None

    def create_session(
        self,
        domain_id: str,
        service_account_id: str,
        options: dict,
        secret_data: dict,
        schema: str = None,
    ) -> None:
        if "mimir_endpoint" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.mimir_endpoint")

        if "promql" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.promql")

        self.mimir_endpoint = secret_data["mimir_endpoint"]
        self.mimir_headers["X-Scope-OrgID"] = service_account_id
        self.mimir_promql = secret_data["promql"]

        self.field_mapper = options.get("field_mapper", None)
        self.default_vars = options.get("default_vars", None)

    def get_promql_response(
        self,
        prometheus_query_range_endpoint: str,
        start: str,
        service_account_id: str,
        promql: str,
    ) -> Union[List[dict], None]:
        start_unix_timestamp, end_unix_timestamp = self._get_unix_timestamp(start)

        self.mimir_headers = {
            "Content-Type": "application/json",
            "X-Scope-OrgID": service_account_id,
        }

        try:
            response = requests.get(
                prometheus_query_range_endpoint,
                headers=self.mimir_headers,
                params={
                    "query": promql,
                    "start": start_unix_timestamp,
                    "end": end_unix_timestamp,
                    "step": "1d",
                },
            )

            response.raise_for_status()  # Raise Errors if status code >= 400

            result = response.json().get("data", {}).get("result")
            return result
        except requests.HTTPError as http_err:
            _LOGGER.error(f"[get_promql_response] HTTP error occurred: {http_err}")
            _LOGGER.error(
                """[get_promql_response] Modify accuracy of the data to adjust precision:
                    Decrease (e.g., to 1m): Enhances accuracy. It's typically not recommended to set it below the Prometheus scraping interval (1m by default)
                    Increase Enhances the performance of the query.
                """
            )
        except Exception as err:
            _LOGGER.error(f"[get_promql_response] error occurred: {err}")

    @staticmethod
    def _get_unix_timestamp(start: str) -> (str, str):
        start = pd.Timestamp(start)
        end = (pd.Timestamp(start) + MonthEnd(0)).replace(hour=23, minute=59, second=59)

        return str(start.timestamp()), str(end.timestamp())

    def get_kubecost_cluster_info(
        self,
        prometheus_query_endpoint: str,
        service_account_id: str,
        secret_data: dict,
    ) -> dict:
        self.mimir_headers = {
            "Content-Type": "application/json",
            "X-Scope-OrgID": service_account_id,
        }
        try:
            response = requests.get(
                prometheus_query_endpoint,
                headers=self.mimir_headers,
                params={
                    "query": secret_data["cluster_info_query"],
                },
            )

            response.raise_for_status()

            result = response.json()
            return result
        except requests.HTTPError as http_err:
            _LOGGER.error(
                f"[get_kubecost_cluster_info] HTTP error occurred: {http_err}"
            )
        except Exception as err:
            _LOGGER.error(f"[get_kubecost_cluster_info] error occurred: {err}")

    @staticmethod
    def get_cost_data(promql_response: List[dict]) -> Generator[List[dict], None, None]:
        page_count = int(len(promql_response) / _PAGE_SIZE) + 1

        for page_num in range(page_count):
            offset = _PAGE_SIZE * page_num
            yield promql_response[offset : offset + _PAGE_SIZE]
