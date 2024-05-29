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

            promql_response = self.mimir_connector.get_promql_response(
                prometheus_query_range_endpoint,
                start,
                service_account_id,
                secret_data["promql"],
            )

            prometheus_query_endpoint = f"{secret_data['mimir_endpoint']}/api/v1/query"
            cluster_info = self.mimir_connector.get_kubecost_cluster_info(
                prometheus_query_endpoint, start, service_account_id, secret_data
            )

            if promql_response:
                promql_response_stream = self.mimir_connector.get_cost_data(
                    promql_response
                )

                yield from self._process_response_stream(
                    cluster_info,
                    service_account_id,
                    promql_response_stream=promql_response_stream,
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
        cluster_info: dict,
        service_account_id: str,
        promql_response_stream: Generator,
    ) -> Generator[dict, None, None]:
        for results in promql_response_stream:
            yield self._make_cost_data(results, cluster_info, service_account_id)

        yield {"results": []}

    def _make_cost_data(
        self,
        results: List[dict],
        cluster_info: dict,
        x_scope_orgid: str,
    ) -> dict:
        cluster_metric = (
            cluster_info.get("data", {}).get("result", [{}])[0].get("metric", {})
        )
        costs_data = []
        for result in results:
            for i in range(len(result["values"])):
                additional_info = self._make_additional_info(result, x_scope_orgid)

                data = {}
                idle = result["metric"].get("type") == "idle"
                load_balancer = result["metric"].get("type") == "Load Balancer"
                if (not idle) and (not load_balancer):
                    data["usage_type"] = result["metric"].get("type")

                result["cost"] = float(result["values"][i][1])
                result["billed_date"] = pd.to_datetime(
                    result["values"][i][0], unit="s"
                ).strftime("%Y-%m-%d")

                try:
                    data.update(
                        {
                            "cost": result.get("cost"),
                            "billed_date": result["billed_date"],
                            "product": cluster_metric.get("provisioner", "kubernetes"),
                            "provider": "kubernetes",
                            "region_code": self._get_region_code(
                                cluster_metric.get("region", "Unknown")
                            ),
                            "usage_quantity": result.get("usage_quantity", 0),
                            "usage_unit": result.get("usage_unit"),
                            "additional_info": additional_info,
                            "tags": result.get("tags", {}),
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
            "X-Scope-OrgID": service_account_id,
        }

        if cluster := result["metric"].get("cluster"):
            additional_info["Cluster"] = cluster

        if node := result["metric"].get("node"):
            additional_info["Node"] = node

        if namespace := result["metric"].get("namespace"):
            additional_info["Namespace"] = namespace

        if pod := result["metric"].get("pod"):
            additional_info["Pod"] = pod

        if container := result["metric"].get("container"):
            additional_info["Container"] = container

        if pv := result["metric"].get("persistentvolume"):
            additional_info["PV"] = pv

        if service := result["metric"].get("service_name"):
            additional_info["Load Balancer"] = service

        if result["metric"].get("type") == "idle":
            additional_info["Idle"] = "__idle__"

        return additional_info

    @staticmethod
    def _get_region_code(region: str) -> str:
        region_name = AWS_REGION_MAP.get(region, "Unknown")

        return region_name
