import logging
import datetime
from airflow.decorators import task, dag
from airflow.models.param import Param

__dag_name__ = "calculate_sum"
__description__ = "Just an example DAG"
__owner__ = 'minhpc@ikameglobal.com'
__schedule__ = None
__start_date__ = datetime.datetime(2024, 1, 1)
__tags__ = ["example"]


@task()
def prepare_data(from_value: int, to_value: int, chunk_size: int, **kwargs) -> list[int]:
    """
    Task to prepare data for processing.

    Args:
        from_value: Start value for data preparation.
        to_value: End value for data preparation.
        chunk_size: Size of data chunks.

    Returns:
        List of batches of integers.
    """

    list_of_int = list(range(from_value, to_value + 1, 1))
    chunks = [list_of_int[i:i + chunk_size] for i in range(0, len(list_of_int), chunk_size)]

    return chunks


@task()
def process_data(data: list[int], **kwargs) -> int:
    """
    Task to process data.

    Args:
        data: List of integers to process.

    Returns:
        Sum of all integers in the input list.
    """
    return sum(data)


@task()
def aggregate_result(**kwargs) -> None:
    """
    Task to aggregate results from multiple tasks.

    Args:
        kwargs: Context passed by Airflow.

    Returns:
        Sum of all integers processed by tasks in the pipeline.
    """
    from_value = kwargs['params']['from_value']
    to_value = kwargs['params']['to_value']

    list_task_result = kwargs['ti'].xcom_pull(task_ids="process_data")

    logging.info(f'Sum of integers from {from_value} to {to_value} is: {sum(list_task_result)}')


@dag(
    catchup=False,
    dag_id=__dag_name__,
    default_args={
        'owner': __owner__,
        'email': [__owner__],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': datetime.timedelta(seconds=60),
        'max_active_tis_per_dag': 8,
    },
    description=__description__,
    doc_md=__doc__,
    params={
        "from_value": Param(default=1,
                            type="integer",
                            description="Start value to calculate sum."),
        "to_value": Param(default=100,
                          type="integer",
                          description="End value to calculate sum."),
        "chunk_size": Param(default=10,
                            type="integer",
                            description="Size of data chunks.")
    },
    render_template_as_native_obj=True,
    schedule=__schedule__,
    start_date=__start_date__,
    tags=__tags__,
)
def generate_dag():
    _prepare_data = prepare_data(from_value="{{ dag_run.conf.from_value }}", to_value="{{ dag_run.conf.to_value }}",
                                 chunk_size="{{ dag_run.conf.chunk_size }}")
    _process_data = process_data.expand(data=_prepare_data)
    _aggregate_result = aggregate_result()

    _prepare_data >> _process_data >> _aggregate_result


dag_instance = generate_dag()

if __name__ == "__main__":
    dag_instance.test(
        run_conf={
            "from_value": 1,
            "to_value": 1000
        }
    )
