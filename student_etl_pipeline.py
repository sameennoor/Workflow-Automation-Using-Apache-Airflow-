from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3

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
# Extract Task
# -------------------------
def extract():
    print("Starting Extract Task...")
    df = pd.read_csv('/opt/airflow/dags/data.csv')
    df.to_csv('/opt/airflow/dags/extracted.csv', index=False)
    print("Extract Completed!")

# -------------------------
# Transform Task
# -------------------------
def transform():
    print("Starting Transform Task...")
    df = pd.read_csv('/opt/airflow/dags/extracted.csv')

    # Uppercase names
    df['name'] = df['name'].str.upper()

    # Add Grade
    def grade(marks):
        if marks >= 85:
            return 'A'
        elif marks >= 70:
            return 'B'
        elif marks >= 50:
            return 'C'
        else:
            return 'F'

    df['grade'] = df['marks'].apply(grade)

    # Pass/Fail
    df['status'] = df['marks'].apply(lambda x: 'Pass' if x >= 50 else 'Fail')

    # Performance Category
    def performance(marks):
        if marks >= 85:
            return 'Excellent'
        elif marks >= 70:
            return 'Good'
        elif marks >= 50:
            return 'Average'
        else:
            return 'Poor'

    df['performance'] = df['marks'].apply(performance)

    # Average marks (for logs)
    avg_marks = df['marks'].mean()
    print(f"Average Marks: {avg_marks}")

    # Keep only passed students
    df = df[df['status'] == 'Pass']

    df.to_csv('/opt/airflow/dags/transformed.csv', index=False)
    print("Transform Completed!")

# -------------------------
# Load Task
# -------------------------
def load():
    print("Starting Load Task...")
    df = pd.read_csv('/opt/airflow/dags/transformed.csv')

    conn = sqlite3.connect('/opt/airflow/dags/etl.db')
    df.to_sql('students', conn, if_exists='replace', index=False)

    print(f"Total records loaded: {len(df)}")

    conn.close()
    print("Load Completed!")

# -------------------------
# DAG Definition
# -------------------------
with DAG(
    dag_id='student_etl_pipeline',
    default_args=default_args,
    schedule_interval='0 9 * * *',  # Daily at 9 AM
    catchup=False,
    description='Student Data ETL Pipeline with Scheduling & Dependency Management'
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