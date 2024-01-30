from spaceone.core.manager import BaseManager

from ..connector.mimir_connector import MimirConnector


class DataSourceManager(BaseManager):
    @staticmethod
    def init_response(domain_id: str, options: dict) -> dict:
        return {
            "metadata": {
                "currency": "USD",
                "supported_secret_types": ["MANUAL"],
                "data_source_rules": [
                    {
                        "name": "match_service_account",
                        "conditions_policy": "ALWAYS",
                        "actions": {
                            # "match_service_account": {
                            #     "source": "additional_info.Cluster IP",
                            #     "target": "data.cluster_ip",
                            # }
                        },
                        "options": {"stop_processing": True},
                    }
                ],
            }
        }

    @staticmethod
    def verify_plugin(
        domain_id: str, options: dict, secret_data: dict, schema: str = None
    ) -> None:
        mimir_connector = MimirConnector()
        mimir_connector.create_session(domain_id, options, secret_data, schema)
