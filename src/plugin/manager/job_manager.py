import logging
from datetime import datetime, timedelta

import pandas as pd
from spaceone.core.error import ERROR_INVALID_PARAMETER_TYPE
from spaceone.core.manager import BaseManager

from ..connector.mimir_connector import MimirConnector
from ..connector.spaceone_connector import SpaceONEConnector

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mimir_connector: MimirConnector = MimirConnector()
        self.spaceone_connector: SpaceONEConnector = SpaceONEConnector()

    def get_tasks(
        self,
        domain_id: str,
        options: dict,
        secret_data: dict,
        schema: str = None,
        start: str = None,
        last_synchronized_at: datetime = None,
    ):
        self.spaceone_connector.init_client(options, secret_data, schema)

        tasks, changed = [], []
        if options.get("resource_group", None) == "DOMAIN":
            response = self.spaceone_connector.list_service_accounts()

            tasks, changed = self._get_tasks_changed(
                response, start, last_synchronized_at, domain_id=domain_id
            )
        elif options.get("resource_group", None) == "WORKSPACE":
            workspace_id = options.get("workspace_id", None)
            response = self.spaceone_connector.list_service_accounts(
                workspace_id=workspace_id
            )

            tasks, changed = self._get_tasks_changed(
                response, start, last_synchronized_at, workspace_id=workspace_id
            )

        _LOGGER.debug(f"Tasks: {tasks}, Changed: {changed}")
        return {"tasks": tasks, "changed": changed}

    def _get_tasks_changed(
        self,
        response: dict,
        start: str,
        last_synchronized_at: datetime,
        domain_id: str = None,
        workspace_id: str = None,
    ):
        if not response.get("total_count"):
            if domain_id:
                _LOGGER.debug(f"No Kubernetes service account: domain_id = {domain_id}")
            else:
                _LOGGER.debug(
                    f"No Kubernetes service account: workspace_id = {workspace_id}"
                )
            return {"tasks": [], "changed": []}

        start_month = self._get_start_month(start, last_synchronized_at)
        tasks, changed = self._generate_tasks(response, start_month)

        return tasks, changed

    def _get_start_month(self, start, last_synchronized_at=None):
        if start:
            start_time: datetime = self.__parse_start_time(start)
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
            start_time = start_time.replace(day=1)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )

        return start_time.strftime("%Y-%m")

    @staticmethod
    def _generate_tasks(response, start_month):
        end_time = datetime.utcnow()
        date_range = pd.date_range(start=start_month, end=end_time, freq="MS").strftime(
            "%Y-%m"
        )

        tasks, changed = [], []
        results = response.get("results", [])
        for account_info in results:
            if not account_info.get("app_id", None):
                continue

            for date in date_range:
                task_options = {
                    "service_account_id": account_info["service_account_id"],
                    "service_account_name": account_info["name"],
                    "cluster_name": account_info.get("options", "").get(
                        "cluster_name", ""
                    ),
                    "start": date,
                }
                tasks.append({"task_options": task_options})
                changed.append(
                    {
                        "start": date,
                        "filter": {
                            "service_account_id": account_info["service_account_id"]
                        },
                    }
                )

        return tasks, changed

    @staticmethod
    def __parse_start_time(start_month: str, date_format: str = "%Y-%m"):
        try:
            return datetime.strptime(start_month, date_format)
        except ValueError:
            raise ERROR_INVALID_PARAMETER_TYPE(key="start", type=date_format)
