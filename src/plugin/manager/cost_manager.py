import logging
from typing import Generator, List, Union

import pandas as pd
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import ERROR_REQUIRED_PARAMETER

from ..connector.mimir_connector import MimirConnector
from ..connector.spaceone_connector import SpaceONEConnector

_LOGGER = logging.getLogger("spaceone")

_REQUIRED_FIELDS = [
    "cost",
]

AWS_REGION_MAP = {
    "af-south-1": "Africa (Cape Town)",
    "ap-east-1": "Asia Pacific (Hong Kong)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-northeast-3": "Asia Pacific (Osaka)",
    "ap-south-1": "Asia Pacific (Mumbai)",
    "ap-south-2": "Asia Pacific (Hyderabad)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-southeast-3": "Asia Pacific (Jakarta)",
    "ap-southeast-4": "Asia Pacific (Melbourne)",
    "ca-central-1": "Canada (Central)",
    "ca-west-1": "Canada West (Calgary)",
    "eu-central-1": "Europe (Frankfurt)",
    "eu-central-2": "Europe (Zurich)",
    "eu-north-1": "Europe (Stockholm)",
    "eu-south-1": "Europe (Milan)",
    "eu-south-2": "Europe (Spain)",
    "eu-west-1": "Europe (Ireland)",
    "eu-west-2": "Europe (London)",
    "eu-west-3": "Europe (Paris)",
    "il-central-1": "Israel (Tel Aviv)",
    "me-central-1": "Middle East (UAE)",
    "me-south-1": "Middle East (Bahrain)",
    "sa-east-1": "South America (São Paulo)",
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-gov-east-1": "AWS GovCloud (US-East)",
    "us-gov-west-1": "AWS GovCloud (US-West)",
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
}


class CostManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mimir_connector: MimirConnector = MimirConnector()
        self.spaceone_connector: SpaceONEConnector = SpaceONEConnector()

    def get_data(
        self,
        domain_id: str,
        options: dict,
        secret_data: dict,
        schema: Union[str, None],
        task_options: Union[dict, None],
    ) -> Generator[dict, None, None]:
        self.spaceone_connector.init_client(options, secret_data, schema)

        start = task_options.get("start")
        service_account_id = task_options.get("service_account_id")

        self._check_resource_group(domain_id, options)

        try:
            prometheus_query_range_endpoint = (
                f"{secret_data['mimir_endpoint']}/api/v1/query_range"
            )
            prometheus_query_range_response = self.mimir_connector.get_promql_response(
                prometheus_query_range_endpoint, start, service_account_id, secret_data
            )

            prometheus_query_endpoint = f"{secret_data['mimir_endpoint']}/api/v1/query"
            cluster_info = self.mimir_connector.get_kubecost_cluster_info(
                prometheus_query_endpoint, service_account_id, secret_data
            )

            if prometheus_query_range_response:
                prometheus_query_range_response_stream = (
                    self.mimir_connector.get_cost_data(prometheus_query_range_response)
                )

                yield from self._process_response_stream(
                    prometheus_query_range_response_stream,
                    cluster_info,
                    service_account_id,
                )
            else:
                _LOGGER.error(
                    "[get_data] The Prometheus query returned no data since your opencost configuration is not ready yet"
                )
                _LOGGER.error(
                    "Or the SpaceONE Agent has not been installed yet. Please install the agent on your cluster."
                )
                yield {"results": []}
        except Exception as e:
            _LOGGER.error("Error processing data: %s", str(e), exc_info=True)
            yield {"results": []}

    def _check_resource_group(self, domain_id: str, options: dict):
        if options.get("resource_group", None) == "DOMAIN":
            response = self.spaceone_connector.list_agents()
            self._has_agent(response, domain_id)
        elif options.get("resource_group", None) == "WORKSPACE":
            workspace_id = options.get("workspace_id", None)
            response = self.spaceone_connector.list_agents(workspace_id)
            self._has_agent(response, workspace_id)

    @staticmethod
    def _has_agent(
        response: dict,
        domain_id: str = None,
        workspace_id: str = None,
    ):
        if not response.get("total_count"):
            if domain_id:
                _LOGGER.debug(
                    f"No Kubernetes agent service account: domain_id = {domain_id}"
                )
            else:
                _LOGGER.debug(
                    f"No Kubernetes agent service account: workspace_id = {workspace_id}"
                )
            yield {"results": []}

    def _process_response_stream(
        self,
        prometheus_query_range_response_stream: Generator,
        cluster_info: dict,
        service_account_id: str,
    ) -> Generator[dict, None, None]:
        for prometheus_query_range_results in prometheus_query_range_response_stream:
            yield self._make_cost_data(
                prometheus_query_range_results, cluster_info, service_account_id
            )
        yield {"results": []}

    def _make_cost_data(
        self,
        prometheus_query_range_results: List[dict],
        cluster_info: dict,
        x_scope_orgid: str,
    ) -> dict:
        cluster_metric = (
            cluster_info.get("data", {}).get("result", [])[0].get("metric", {})
        )
        costs_data = []
        for prometheus_query_range_result in prometheus_query_range_results:
            for i in range(len(prometheus_query_range_result["values"])):
                data = {}
                prometheus_query_range_result["cost"] = float(
                    prometheus_query_range_result["values"][i][1]
                )
                prometheus_query_range_result["billed_date"] = pd.to_datetime(
                    prometheus_query_range_result["values"][i][0], unit="s"
                ).strftime("%Y-%m-%d")

                additional_info = self._make_additional_info(
                    prometheus_query_range_result, x_scope_orgid
                )
                try:
                    data.update(
                        {
                            "cost": prometheus_query_range_result.get("cost"),
                            "billed_date": prometheus_query_range_result["billed_date"],
                            "product": prometheus_query_range_result.get("product"),
                            "provider": cluster_metric.get("provider", "kubernetes"),
                            "region_code": self._get_region_code(
                                cluster_metric.get("region", "Unknown")
                            ),
                            "usage_quantity": prometheus_query_range_result.get(
                                "usage_quantity", 0
                            ),
                            "usage_type": prometheus_query_range_result["metric"][
                                "type"
                            ],
                            "usage_unit": prometheus_query_range_result.get(
                                "usage_unit"
                            ),
                            "additional_info": additional_info,
                            "tags": prometheus_query_range_result.get("tags", {}),
                        }
                    )
                except Exception as e:
                    _LOGGER.error(
                        f"[_make_cost_data] make data error: {e}", exc_info=True
                    )
                    raise e
                costs_data.append(data)

        return {"results": costs_data}

    @staticmethod
    def _strip_dict_keys(result: dict) -> dict:
        return {
            key: value.strip() if isinstance(value, str) else value
            for key, value in result.items()
        }

    @staticmethod
    def _strip_dict_values(result: dict) -> dict:
        return {
            key: value.strip() if isinstance(value, str) else value
            for key, value in result.items()
        }

    @staticmethod
    def _check_required_fields(result: dict):
        for field in _REQUIRED_FIELDS:
            if field not in result:
                raise ERROR_REQUIRED_PARAMETER(key=field)

    @staticmethod
    def _make_additional_info(result: dict, service_account_id: str) -> dict:
        additional_info = {
            "Cluster": result["metric"].get("cluster", ""),
            "Node": result["metric"].get("node", "PVs"),
            "Namespace": result["metric"].get("namespace", ""),
            "Pod": result["metric"].get("pod", ""),
            "X-Scope-OrgID": service_account_id,
        }

        if container := result["metric"].get("container"):
            additional_info["Container"] = container

        if pv := result["metric"].get("persistentvolume"):
            additional_info["PV"] = pv

        return additional_info

    @staticmethod
    def _get_region_code(region: str) -> str:
        region_name = AWS_REGION_MAP.get(region, "Unknown")

        return region_name
