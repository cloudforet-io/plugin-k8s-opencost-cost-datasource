import logging
from typing import Generator, Union

import pandas as pd
import requests
from pandas._libs.tslibs.offsets import MonthEnd
from spaceone.core.connector import BaseConnector
from spaceone.core.error import ERROR_REQUIRED_PARAMETER

_LOGGER = logging.getLogger(__name__)
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

        self.field_mapper = None
        self.default_vars = None

    def create_session(
        self, domain_id: str, options: dict, secret_data: dict, schema: str = None
    ) -> None:
        if "mimir_endpoint" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.mimir_endpoint")

        if "X_Scope_OrgID" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.X_Scope_OrgID")

        self.mimir_endpoint = secret_data["mimir_endpoint"]
        self.mimir_headers["X-Scope-OrgID"] = secret_data["X_Scope_OrgID"]

        self.field_mapper = options.get("field_mapper", None)
        self.default_vars = options.get("default_vars", None)

    def get_promql_response(
        self, promql_query_range: str, start: str
    ) -> Union[list[dict], None]:
        start_unix_timestamp, end_unix_timestamp = self._get_unix_timestamp(start)

        response = requests.get(
            promql_query_range,
            headers=self.mimir_headers,
            params={
                "query": self._create_promql(start),
                "start": start_unix_timestamp,
                "end": end_unix_timestamp,
                "step": "1d",
            },
        )

        try:
            response.raise_for_status()
            return response.json().get("data", {}).get("result")
        except requests.HTTPError as http_err:
            _LOGGER.error(f"[get_promql_response] HTTP error occurred: {http_err}")
            _LOGGER.error(response.text)
            return None

    @staticmethod
    def _get_unix_timestamp(start: str) -> (str, str):
        start = pd.Timestamp(start)
        end = (pd.Timestamp(start) + MonthEnd(0)).replace(hour=23, minute=59, second=59)

        return str(start.timestamp()), str(end.timestamp())

    @staticmethod
    def _create_promql(start: str) -> str:
        days = int((pd.Timestamp(start) + MonthEnd(0)).strftime("%d"))

        return f"""
            sum by (cluster, node, namespace, pod) (
                sum_over_time (
                    (
                        label_replace (
                            (
                                avg by (container, cluster, node, namespace, pod) (container_cpu_allocation)
                                * on (node) group_left ()
                                avg by (node) (node_cpu_hourly_cost)
                            ),
                            "type",
                            "CPU",
                            "",
                            ""
                        )
                        or
                        label_replace (
                            (
                                (
                                    avg by (container, cluster, node, namespace, pod) (container_memory_allocation_bytes)
                                    * on (node) group_left ()
                                    avg by (node) (node_ram_hourly_cost)
                                )
                                /
                                (1024 * 1024 * 1024)
                            ),
                            "type",
                            "RAM",
                            "",
                            ""
                        )
                        or
                        label_replace(
                            (
                                avg by (container, cluster, node, namespace, pod) (container_gpu_allocation)
                                * on (node) group_left ()
                                avg by (node) (node_gpu_hourly_cost)
                            ),
                            "type",
                            "GPU",
                            "",
                            ""
                        )
                        or
                        label_replace (
                            (
                                (
                                    avg by (persistentvolume, cluster, node, namespace, pod) (pod_pvc_allocation)
                                    * on (persistentvolume) group_left ()
                                    avg by (persistentvolume) (pv_hourly_cost)
                                )
                                /
                                (1024 * 1024 * 1024)
                            ),
                            "type",
                            "PV",
                            "",
                            ""
                        )
                    )[{days}d:10m]
                )
                /
                scalar(count_over_time(vector(1)[{days}d:10m]))
                * 24 * {days}
            )
        """

    @staticmethod
    def get_cost_data(promql_response: list[dict]) -> Generator[list[dict], None, None]:
        _LOGGER.debug(f"[get_cost_data] promql_response: {promql_response}")
        # Paginate
        page_count = int(len(promql_response) / _PAGE_SIZE) + 1

        for page_num in range(page_count):
            offset = _PAGE_SIZE * page_num
            yield promql_response[offset : offset + _PAGE_SIZE]
