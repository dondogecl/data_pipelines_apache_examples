from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
import logging
import requests


# basic logging config
log = logging.getLogger()
logging.basicConfig(level=logging.INFO)


# variables
sql_ddl = """
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    firstname VARCHAR(255),
    lastname VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

api_endpoint = "https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json"


@dag
def user_processing():
    """Using a DDL statement it creates a table named users in a DB"""
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql=sql_ddl
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        """Sensor task detects if an API is available"""
        condition = False
        response = requests.get(api_endpoint)
        status_code = response.status_code
        logging.info("Validating if API endpoint is Available...")
        if response.status_code == 200:
            logging.info("STATUS CODE 200")
            condition = True
            fake_user = response.json()
        else:
            logging.warning(f"REQUEST FAILED. STATUS CODE {status_code}")
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)


    # declare task
    is_api_available()


# Declare dag

user_processing()