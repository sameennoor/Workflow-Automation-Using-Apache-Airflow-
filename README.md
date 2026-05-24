# Airflow-Based Workflow Automation & ETL System
This project demonstrates the implementation of automated ETL (Extract, Transform, Load) pipelines using Apache Airflow and Docker. The system is designed to process and manage structured datasets through workflow orchestration, task scheduling, and containerized execution environments.
The framework includes multiple DAG-driven pipelines for academic and employee data processing, providing practical experience with modern data engineering concepts and workflow automation techniques.
# 🚀 Features
- Automated ETL workflow orchestration
- DAG-based task scheduling using Apache Airflow
- Docker containerized execution environment
- Multi-pipeline data processing architecture
- Student academic analytics pipeline
- Employee financial data processing pipeline
- SQLite database integration
- CSV-based analytical report generation
# 📊 ETL Pipelines
## 🎓 Student Academic ETL Pipeline
- Extracts academic datasets
- Cleans and preprocesses student records
- Calculates GPA and performance indicators
- Filters passing students
- Loads transformed data into SQLite database
## 💼 Employee Financial ETL Pipeline
- Reads multiple employee CSV files
- Aggregates workforce salary datasets
- Calculates bonuses and compensation metrics
- Categorizes medium and high earners
- Generates processed analytical CSV outputs
# 🔄 Workflow Architecture
The project uses Apache Airflow DAGs (Directed Acyclic Graphs) to manage execution flow, task dependencies, and automation scheduling.
## 1. Student Pipeline Flow

Extract Data → Transform Records → Analyze Performance → Load into SQLite

## 2. Employee Pipeline Flow

Read CSV Files → Process Salary Data → Compute Bonuses → Generate Reports
