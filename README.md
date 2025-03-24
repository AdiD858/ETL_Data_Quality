# LendingClub ETL Data Quality Validation System

## 🚀 Project Overview
This project simulates a data quality validation system for a peer-to-peer lending company (LendingClub).The data passes through an ETL pipeline and is validated using PySpark, orchestrated by Airflow and NiFi, and stored in MinIO.
By daily automatic loans data processing and data quality execution, the business stakeholders in Lending-club could analyze valid data files efficiently, extract powerful insights and implement data driven business decisions. 



## 📦 Technologies Used
- **Docker**: Container orchestration that contains the following integrated services in the project.
- **Apache NiFi**: Data ingestion tool, great for creating data files ETL pipeline, menegement data\ metadata and more.
- **MinIO**: Object storage (S3 compatible) to store the files in variuse formats.
- **PySpark**: Python library for big data processing and quality checks, on spark -distributed and parallel engine.
- **Apache Airflow**: Workflow scheduling and data orchastration.
- **PostgreSQL**: object-relational database management system, through LC users could access Logging and results (optional)
- **Tableau / CSV Output**: Visualization of the data quality results or report output. 

## 🔄 Data Flow

## Steps
1. **Upload CSV files to MinIO**
2. **Read and process data with PySpark**
3. **Perform data quality checks**
4. **Store cleaned data in Parquet format**
5. **Automate the pipeline with NiFi & Airflow**
6. **Generate the results report \ visualization with (CSV / PostgreSQL)**



CSV / XLSX files → NiFi → MinIO → PySpark (Validations) → Airflow DAG → Output (CSV / PostgreSQL)

🧪 Validation Types Implemented

- **Completeness**: Null checks, record count
- **Uniqueness**: Duplicate rows, file-level duplicates
Consistency: Value stability between files
Foreign Key: Cross-file key validation
Validations: Date format, numeric range, allowed values
Freshness: File recency, date ranges
Accuracy: Aggregated column sums, moving average

📁 File Structure

├── data/
├── scripts/
├── dags/
├── results/
├── README.md
├── dqa_specification.md
├── docker-compose.yml

⚙️ How to Run the Project
# Start services
docker-compose up -d

# Run PySpark validation script
python scripts/run_dqa.py

📊 Output Example
Validation results will be saved to results/status_table.csv or sent to PostgreSQL if configured.

📄 Full DQA Specification
See dqa_specification.md for detailed validation logic and column mapping.

