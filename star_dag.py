from dbConnect import my_connection
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

# Separate files for importing raw data from source, staging model, and intermediate models (the basis for marts)
from import_raw_data import _import_raw_data
from stg_customers import _create_stg_customers
from int_models import _create_int_accounts, _create_int_ad_campaigns, _create_int_customer_devices, _create_int_customers, _create_int_fct_customer_activity


# Create the DAG for this workflow.
with DAG("BING_MultiDays_dag", schedule_interval="@daily", start_date=datetime.now()) as dag:
    # Define one task per table
    raw_data = PythonOperator(
        task_id="raw_data",
        python_callable=_import_raw_data
    )

    staging_data = PythonOperator(
        task_id="staging_data",
        python_callable=_create_stg_customers
    )

    int_accounts = PythonOperator(
        task_id="int_accounts",
        python_callable=_create_int_accounts
    )

    int_ad_campaigns = PythonOperator(
        task_id="int_ad_campaigns",
        python_callable=_create_int_ad_campaigns
    )

    int_customer_devices = PythonOperator(
        task_id="int_customer_devices",
        python_callable=_create_int_customer_devices
    )

    int_customers = PythonOperator(
        task_id="int_customers",
        python_callable=_create_int_customers
    )

    int_fct_customer_activity = PythonOperator(
        task_id="int_fct_customer_activity",
        python_callable=_create_int_fct_customer_activity
    )

    # Create branching structure: raw data -> staging model -> intermediate models
    raw_data >> staging_data >> [int_accounts, int_ad_campaigns, int_customer_devices, int_customers, int_fct_customer_activity]
