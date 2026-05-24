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
## 🎓 Student Data ETL Pipeline
### Workflow:
extract_task → transform_task → load_task
**Functions**
- Reads student data from CSV files
- Assigns grades and performance labels
- Removes failed students
- Stores processed data into SQLite database (etl.db)
### Grade System
| Marks Range | Grade |
|-------------|-------|
| 85+ | A |
| 70–84 | B |
| 50–69 | C |
| Below 50 | F |
## 💼 Employee Multi-File ETL Pipeline
### Workflow:

extract_task → transform_task → load_task
**Functions**
- Reads employee data from multiple department files
- Merges datasets into a single structure
- Calculates bonuses and total salaries
- Categorizes employees by salary range
- Exports final processed dataset into final_employees.csv
### Salary Category
| Salary Range | Category |
|--------------|----------|
| 75k+ | High |
| 60k–74,999 | Medium |
| Below 60k | Low |

# ⚙️ Technology Stack
| Technology | Purpose |
|------------|---------|
| Apache Airflow | Workflow Orchestration |
| Docker | Containerization |
| Python | ETL Logic & Automation |
| Pandas | Data Processing |
| SQLite | Database Storage |
| CSV Processing | Structured Data Handling |
# 🛠️ Requirements
- Windows 10/11
- WSL 2 Enabled
- Docker Desktop
- Python 3.x
- Apache Airflow
# ✅ Conclusion
This project demonstrates practical implementation of workflow automation and ETL orchestration using Apache Airflow and Docker, providing hands-on experience with modern data engineering concepts and automated pipeline management.
