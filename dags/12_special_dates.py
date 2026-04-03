from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable( event_dates=[
    datetime(2026,4,4),
    datetime(2026,4,6),
    datetime(2026,4,9)
]
)

@dag(
    schedule=special_dates,
    start_date=datetime(year=2026, month=4, day=3, tz="CET"),
    end_date=datetime(year=2026, month=4, day=10, tz="CET"),
    catchup=True
)
def special_dates_dag():

    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs['logical_date']
        print(f"Running task for special event on {execution_date}")

        special_event = special_event_task()

special_dates_dag()




