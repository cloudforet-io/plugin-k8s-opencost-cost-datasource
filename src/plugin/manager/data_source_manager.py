from spaceone.core.manager import BaseManager


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
