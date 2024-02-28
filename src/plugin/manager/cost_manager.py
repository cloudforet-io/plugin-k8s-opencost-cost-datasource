import logging
from typing import Generator, List, Union

import pandas as pd
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import ERROR_REQUIRED_PARAMETER

from ..connector.mimir_connector import MimirConnector

_LOGGER = logging.getLogger(__name__)

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
        self.mimir_connector = MimirConnector()

    def get_data(
        self,
        domain_id: str,
        options: dict,
        secret_data: dict,
        schema: Union[str, None],
        task_options: Union[dict, None],
    ) -> Generator[dict, None, None]:
        self.mimir_connector.create_session(domain_id, options, secret_data, schema)

        promql_query_range = f"{secret_data['mimir_endpoint']}/api/v1/query_range"
        promql_response = self.mimir_connector.get_promql_response(
            promql_query_range, task_options["start"]
        )

        response_stream = self.mimir_connector.get_cost_data(promql_response)
        for results in response_stream:
            yield self._make_cost_data(results)

        yield {"results": []}

    def _make_cost_data(self, results: List[dict]) -> dict:
        costs_data = []
        for result in results:
            for i in range(len(result["values"])):
                result["cost"] = float(result["values"][i][1])
                result["billed_date"] = pd.to_datetime(
                    result["values"][i][0], unit="s"
                ).strftime("%Y-%m-%d")

                self._check_required_fields(result)

                additional_info = self._make_additional_info(result)
                region_code = self._get_region_code(result)
                # data = self._make_data(result)

                try:
                    data = {
                        "cost": result["cost"],
                        "usage_quantity": result.get("usage_quantity", 0),
                        "usage_type": result.get("usage_type"),
                        "usage_unit": result.get("usage_unit"),
                        "provider": "Kubernetes",
                        "region_code": region_code,
                        "product": result.get("product"),
                        "billed_date": result["billed_date"],
                        # "data": result.get("data", {}),
                        "additional_info": additional_info,
                        "tags": result.get("tags", {}),
                    }
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
    def _make_additional_info(result: dict) -> dict:
        additional_info = {
            "Cluster": result["metric"].get("cluster", ""),
            "Node": result["metric"].get("node", "Unknown"),
            "Namespace": result["metric"].get("namespace", ""),
            "Pod": result["metric"].get("pod", ""),
            # "Type": result["metric"]["type"],
        }

        return additional_info

    # @staticmethod
    # def _make_data(result: dict) -> dict:
    #     data = {"Change Percent": float(result.get("Change Percent", 0.0))}
    #
    #     return data

    @staticmethod
    def _get_region_code(result: dict) -> str:
        node = result["metric"].get("node", "")
        if node:
            region = node.split(".")[1]
        else:
            region = "Unknown"

        region_name = AWS_REGION_MAP.get(region, "Unknown")

        return region_name
