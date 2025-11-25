from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
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

# Functions
def _extract_user(ti):
    """Gets the user from the API data inside the JSON payload"""
    try:
        fake_user = ti.xcom_pull(task_ids="is_api_available")
        logging.debug(f"Extracted user: {fake_user}")
    except Exception as e:
        logging.error(f"Error trying to extract user from message, details: {e}")
        raise

    return {
        "id": fake_user["id"],
        "firstname": fake_user["personalInfo"]["firstName"],
        "lastname": fake_user["personalInfo"]["lastName"],
        "email": fake_user["personalInfo"]["email"]
    }


# DAG

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

    
    extract_user = PythonOperator(
        task_id="extract_user",
        python_callable=_extract_user
    )


    # declare task
    is_api_available()


# Declare dag

user_processing()