from datetime import datetime, timedelta
import logging
import os
import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
#from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
logger = logging.getLogger(__name__)

# ----------------------------------------------------
# Configuration
# ----------------------------------------------------

CONNECTION_ID = "local_Db"

QUERY = """
SELECT *
FROM dbo.emp;
"""
timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
OUTPUT_PATH = "/opt/airflow/output"
os.makedirs(OUTPUT_PATH,exist_ok=True)
OUTPUT_FILE = os.path.join(
    OUTPUT_PATH,
    f"employee_data_{timestamp}.csv"
)


# ----------------------------------------------------
# Task
# ----------------------------------------------------

def fetch_employee_data(**context):

    try:

        logger.info("Connecting to MSSQL...")

        hook = MsSqlHook(
            mssql_conn_id=CONNECTION_ID
        )

        logger.info("Executing Query...")

        df = hook.get_pandas_df(QUERY)

        if df.empty:
            raise AirflowException("Employee table is empty.")

        logger.info("Rows fetched : %s", len(df))

        # -----------------------------------------
        # Save CSV
        # -----------------------------------------

        os.makedirs(OUTPUT_PATH, exist_ok=True)

        csv_path = os.path.join(
            OUTPUT_PATH,
            OUTPUT_FILE
        )

        df.to_csv(
            OUTPUT_FILE,
            index=False
        )

        logger.info("CSV Saved : %s", csv_path)

        # -----------------------------------------
        # Convert to JSON
        # -----------------------------------------

        employee_json = df.to_dict(orient="records")

        # -----------------------------------------
        # Push into XCom
        # -----------------------------------------

        ti = context["ti"]

        ti.xcom_push(
            key="FILE_PATH",
            value=OUTPUT_FILE
        )

        logger.info("Employee data pushed to XCom.")

        # return {
        #     "file_path": OUTPUT_FILE,
        #     "file_name": os.path.basename(OUTPUT_FILE),
        #     "rows": len(df),
        #     "created_at": timestamp
        # }

    except Exception as e:

        logger.exception("Employee extraction failed.")

        raise AirflowException(str(e))


# ----------------------------------------------------
# Default Arguments
# ----------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ----------------------------------------------------
# DAG
# ----------------------------------------------------

with DAG(
    dag_id="employee_export_csv_xcom",
    description="Read Employee data from MSSQL, save CSV and push to XCom",
    default_args=default_args,
    start_date=datetime(2026, 7, 2),
    schedule=None,
    catchup=False,
    tags=["mssql", "xcom"],
) as dag:

    fetch_employee = PythonOperator(
        task_id="fetch_employee_data",
        python_callable=fetch_employee_data,
        provide_context=True,
        #trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    fetch_employee