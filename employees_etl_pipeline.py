from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

# -------------------------
# Default Arguments
# -------------------------
default_args = {
    'owner': 'romaisa',
    'start_date': datetime(2026, 4, 15),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# -------------------------
# Extract
# -------------------------
def extract():
    print("Extracting data from multiple files...")

    hr = pd.read_csv('/opt/airflow/dags/hr.csv')
    it = pd.read_csv('/opt/airflow/dags/it.csv')
    finance = pd.read_csv('/opt/airflow/dags/finance.csv')

    # Add department column
    hr['department'] = 'HR'
    it['department'] = 'IT'
    finance['department'] = 'Finance'

    # Combine all data
    combined = pd.concat([hr, it, finance])

    combined.to_csv('/opt/airflow/dags/extracted.csv', index=False)

    print("Extraction Completed!")

# -------------------------
# Transform
# -------------------------
def transform():
    print("Transforming data...")

    df = pd.read_csv('/opt/airflow/dags/extracted.csv')

    # Uppercase names
    df['name'] = df['name'].str.upper()

    # Add Bonus (10%)
    df['bonus'] = df['salary'] * 0.10

    # Calculate Total Salary
    df['total_salary'] = df['salary'] + df['bonus']

    # Salary Category
    def category(salary):
        if salary >= 75000:
            return 'High'
        elif salary >= 60000:
            return 'Medium'
        else:
            return 'Low'

    df['category'] = df['salary'].apply(category)

    # Filter out low category
    df = df[df['category'] != 'Low']

    # Sort by total salary (highest first)
    df = df.sort_values(by='total_salary', ascending=False)

    df.to_csv('/opt/airflow/dags/transformed.csv', index=False)

    print("Transformation Completed!")

# -------------------------
# Load (Clean CSV + Table)
# -------------------------
def load():
    print("Loading final data into file...")

    df = pd.read_csv('/opt/airflow/dags/transformed.csv')

    # Round values for neatness
    df['bonus'] = df['bonus'].round(2)
    df['total_salary'] = df['total_salary'].round(2)

    # Save clean CSV
    df.to_csv('/opt/airflow/dags/final_employees.csv', index=False)

    # Show table in logs (VERY IMPORTANT ⭐)
    print("\nFinal Data Table:\n")
    print(df.to_string(index=False))

    print(f"\nTotal records in final file: {len(df)}")
    print("Load Completed!")

# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id='employee_multi_file_etl',
    default_args=default_args,
    schedule_interval='0 9 * * *',  # Daily at 9 AM
    catchup=False,
    description='Multi-file ETL pipeline for employee data integration'
) as dag:

    t1 = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        execution_timeout=timedelta(minutes=5)
    )

    t2 = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        execution_timeout=timedelta(minutes=5)
    )

    t3 = PythonOperator(
        task_id='load_task',
        python_callable=load,
        execution_timeout=timedelta(minutes=5)
    )

    # Dependencies
    t1 >> t2 >> t3