import logging
from datetime import datetime, timedelta

import pandas as pd
from spaceone.core.error import ERROR_INVALID_PARAMETER_TYPE
from spaceone.core.manager import BaseManager

from ..connector.mimir_connector import MimirConnector

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mimir_connector: MimirConnector = MimirConnector()

    def get_tasks(
        self,
        domain_id: str,
        options: dict,
        secret_data: dict,
        schema: str = None,
        start: str = None,
        last_synchronized_at: datetime = None,
    ) -> dict:
        self.mimir_connector.create_session(domain_id, options, secret_data, schema)

        tasks = []
        changed = []

        start_year, start_month = self._get_start_month(
            start, last_synchronized_at
        ).split("-")

        today = datetime.utcnow()

        this_year, this_month = str(today.year), str(today.month)

        year_months = pd.date_range(
            start=f"{start_year}-{start_month}",
            end=f"{this_year}-{this_month}",
            freq="MS",
        ).to_period("M")

        if start_year == this_year:
            for month in range(int(start_month), int(this_month) + 1):
                task_options = {"start": f"{start_year}-{month:02d}"}
                tasks.append({"task_options": task_options})
                changed.append({"start": f"{start_year}-{month:02d}"})
        else:
            for year_month in year_months:
                task_options = {"start": str(year_month)}
                tasks.append({"task_options": task_options})
                changed.append({"start": str(year_month)})

        _LOGGER.debug(f"[get_tasks] tasks: {tasks}")
        _LOGGER.debug(f"[get_tasks] changed: {changed}")

        return {"tasks": tasks, "changed": changed}

    def _get_start_month(
        self, start: str, last_synchronized_at: datetime = None
    ) -> str:
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
    def __parse_start_time(start_str: str) -> datetime:
        date_format = "%Y-%m"

        try:
            return datetime.strptime(start_str, date_format)
        except Exception:
            raise ERROR_INVALID_PARAMETER_TYPE(key="start", type=date_format)
