from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="hello_airflow_python314",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "python314"],
)
def hello_airflow_python314():
    @task
    def show_runtime():
        import platform
        print(f"Python runtime: {platform.python_version()}")
        return platform.python_version()

    show_runtime()


hello_airflow_python314()
