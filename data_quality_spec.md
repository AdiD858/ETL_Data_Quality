# DQA Specification – LendingClub ETL Project


## 1. Introduction

This document describes the Data Quality Assurance (DQA) logic and validation checks applied on LendingClub loan data, simulating an ETL testing framework used in banking environments. 
The goal is to ensure data integrity, accuracy, completeness, and consistency before ingestion into a data warehouse by automatic and efficient way.

## 2. Business Context and Data Understanding

LendingClub is a US-based peer-to-peer lending platform connecting borrowers and investors. The dataset contains historical loan data from 2007–2015 and includes loan details, credit info, income, and repayment status.

### Sample Fields:
- `loan_id`: Unique identifier of each loan
- `annual_inc`: Borrower's self-reported income
- `loan_status`: Loan repayment status (e.g., Fully Paid, Charged Off)
- `issue_d`: Date when the loan was issued

---

## 3. System Architecture Overview
The system consists of the following components:

- **MinIO**: Object storage for raw and processed data
- **PySpark**: Executes validation functions
- **Airflow**: Schedules and manages validation pipeline
- **PostgreSQL**: (Optional) Stores logs and results


---

## 4. Data Flow and Components

### 4.1 Data Ingestion
 files ingested via NiFi

### 4.2 Data Storage
CSV/XLSX Files saved in MinIO, thena after the validation process, kept in MinIO different Backet at Parquet format

### 4.3 DQA Engine (PySpark)
PySpark commands to read the data, running tests, and writes logs

### 4.4 Orchestration (Airflow)
Airflow DAG defines full ETL + DQA flow

### 4.5 Monitoring
Logs saved as CSV or into a database

---

## 5. Validation Logic

| Validation Type | Description |
|------------------|-------------|
| Completeness     | Nulls, record counts |
| Uniqueness       | Duplicates in records or files |
| Consistency      | Value matching across files |
| Foreign Key      | Referential integrity |
| Validations      | Format, range, allowed values |
| Freshness        | Recent file & data range |
| Accuracy         | Sum checks, moving average |

---

## 6. Test Mapping Table

| Column Name   | Data Type | Business Meaning                  | Relevant Validations                     |
|---------------|-----------|------------------------------------|------------------------------------------|
| loan_id       | string    | Unique loan ID                    | Nulls, Duplicates                        |
| customer_id   | string    | Borrower identifier               | Foreign Key, Uniqueness                  |
| annual_inc    | float     | Annual income                     | Range check, Outlier detection           |
| issue_d       | date      | Date loan was issued              | Format, Range, Freshness                 |
| loan_status   | string    | Current loan status               | Allowed values, Consistency              |
| int_rate      | float     | Interest rate                     | Range check                              |

---

## 7. Operational Table Description

The `Operational_table.csv` file defines which test runs on which file and column:

| Column         | Description                                      |
|----------------|--------------------------------------------------|
| File_Name      | The file to be tested                            |
| TCID           | Test Case ID                                     |
| is_active      | Whether to run the test (1/0)                    |
| test           | Function name to run                             |
| tested_field   | Column being tested                              |
| expected_value | Expected value, range, or condition              |

* is_active currently called 'Enabled': True\False , at the current flow all the tests marked as True
* Critical - current field: if the test failes, True-stop the process, False- mentaion in the report. currently only test type 'validation' columns marked as critical. 
---

## 8. Logging and Outputs

Results are saved to `status_table.csv`, containing:

| Test Case ID | File | Test       | Status | Message               |
|--------------|------|------------|--------|------------------------|
| TC001        | loans.parquet | check_nulls | Passed | No nulls found       |
| TC002        | loans.parquet | check_sum   | Failed | Expected 1M, got 980K|

---

## 9. Recommendations and Next Steps

- Add unit tests per function
- Add real-time monitoring for test failures
- Improve handling of large file ingestion with NiFi
- Automate dashboard updates with Tableau

---
