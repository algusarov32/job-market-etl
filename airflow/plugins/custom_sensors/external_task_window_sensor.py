from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State


class ExternalTaskWindowSensor(ExternalTaskSensor):
    """
    Сенсор, который ждет успешного завершения задачи в другом DAG'е
    в пределах заданного временного окна от текущего момента.
    """

    def __init__(
        self,
        time_window: timedelta = timedelta(hours=2),
        *args,
        **kwargs
    ):
        """
        :param time_window: временной интервал от текущего момента,
                            в пределах которого ищем успешный запуск.
        """
        super().__init__(*args, **kwargs)
        self.time_window = time_window

    def poke(self, context: dict) -> bool:
        """
        Переопределяем poke для поиска успешного запуска
        в пределах time_window.
        """
        from airflow.utils.db import create_session
        from airflow.models import DagRun, TaskInstance

        # Определяем временной диапазон:
        # от 'time_window' назад до текущего момента
        now = datetime.now(timezone.utc)
        window_start = now - self.time_window

        with create_session() as session:
            # Ищем успешный DagRun для внешнего DAG'а,
            # который завершился в нашем временном окне.
            successful_dr = session.query(DagRun).filter(
                DagRun.dag_id == self.external_dag_id,
                DagRun.state == State.SUCCESS,
                DagRun.execution_date >= window_start
            ).order_by(DagRun.execution_date.desc()).first()

            if not successful_dr:
                # Подходящих успешных запусков нет
                return False

            # Если нашли успешный DagRun, проверяем статус целевой задачи в нём
            ti = session.query(TaskInstance).filter(
                TaskInstance.dag_id == self.external_dag_id,
                TaskInstance.task_id == self.external_task_id,
                TaskInstance.run_id == successful_dr.run_id
            ).first()

            if ti and ti.state in self.allowed_states:
                # Задача успешна
                return True
            elif ti and ti.state in self.failed_states:
                # Задача упала
                raise AirflowException(
                    f"External task {self.external_task_id} in DAG {self.external_dag_id} failed."
                )
            else:
                # Задача ещё выполняется
                return False