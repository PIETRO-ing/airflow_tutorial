from airflow.sdk import dag, task

@dag
def versioned_dag():
    
    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task. DAG complted!")

    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()


    first >> second >> third

versioned_dag()

