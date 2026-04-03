from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
schedule=CronDataIntervalTimetable("@daily", timezone="CET"),
start_date=datetime(year=2026, month=4, day=3, tz="CET"),
end_date=datetime(year=2026, month=4, day=8, tz="CET"),
catchup=True
)
def incremental_load_dag():

    @task.python
    def incremental_data_fecth(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from from {date_interval_start} to {date_interval_end}")

    @task.bash
    def incremental_data_process():
        return "echo 'Processing incremental data from {{data_interval_start}} to {{data_interval_end}}'"
    
    fetch_task = incremental_data_fecth()
    process_task = incremental_data_process()

    fetch_task >> process_task

incremental_load_dag()



