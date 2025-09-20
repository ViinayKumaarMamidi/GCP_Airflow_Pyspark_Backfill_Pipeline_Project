# GCP_Airflow_Pyspark_Backfill_Pipeline_Project
This repo contains details about how to use Airflow to create a Backfill DAG using Python and Pyspark and load the data accordingly from source to destination GCS buckets, Thanks

**Data Flow Details:** 
In this Project, Source Data lands into GCS folder with date paremeter( *_YYYYMMDD.csv). I built Airflow Backfill Parameterized DAG using Python and Pyspark to check for the date based file and load into Output folder with correspoinding date file. Pyspark Job is implemented/executed using Ephemeral Dataproc cluster

**End to End Implementation Steps:**

1. Spin up Airflow 3 using composer, provide required permissions service account to access GCS bucket and Dataproc Cluster
2. Drop the Daily CSV files by creating folder in the GCS bucket
3. Create Pyspark/Python file which reads the data from source bucket, filters the data with Status = Completed for the orders datasets
4. Load the data into CSV format in the output folder with corresponding date parameter
5. In the Airflow DAG, Include functionality for Execution_Date paramenter so that user can pass the value from Airflow UI and DAG operates based on this date and loads the data
6. Make sure to implement right Airflow operators for DataProc Cluster creation, Pyspark Job Execution and Dataproc Cluster Deletion
7. Implement Airflow Variables to store sensitive credentials like GCP Proejct Name, Dataproc cluster name and Region Name
8. Airflow Dag Flow- Run Python task to pull execution date for next steps--> Run Dataproc Cluster Creation --> Run Pyspark Job --> Delete Dataproc Cluster


**Source Files:**
1. https://github.com/ViinayKumaarMamidi/GCP_Airflow_Pyspark_Backfill_Pipeline_Project/blob/main/orders_20250919.csv
2. https://github.com/ViinayKumaarMamidi/GCP_Airflow_Pyspark_Backfill_Pipeline_Project/blob/main/orders_20250920.csv

**Destination Files:**
1. https://github.com/ViinayKumaarMamidi/GCP_Airflow_Pyspark_Backfill_Pipeline_Project/blob/main/output_processed_orders_20250919_part-00000-c711e842-3c13-4a0a-b4c0-f880540c42b9-c000.csv
2. https://github.com/ViinayKumaarMamidi/GCP_Airflow_Pyspark_Backfill_Pipeline_Project/blob/main/output_processed_orders_20250920_part-00000-e921ec43-99af-4ec5-9548-f778bf3b63f7-c000.csv


**Airflow Execution Log Files:**

1. Execution_Date: 20250919: https://github.com/ViinayKumaarMamidi/GCP_Airflow_Pyspark_Backfill_Pipeline_Project/blob/main/dag_id%3Dorders_backfilling_dag_run_id%3Dmanual__2025-09-20T15_25_29%2B00_00_task_id%3Dget_execution_date_attempt%3D1.log
2. Execution_Date: 20250920: https://github.com/ViinayKumaarMamidi/GCP_Airflow_Pyspark_Backfill_Pipeline_Project/blob/main/dag_id%3Dorders_backfilling_dag_run_id%3Dmanual__2025-09-20T15_13_26%2B00_00_task_id%3Ddelete_dataproc_cluster_attempt%3D1.log
   

**GCS Source Files Details:**

<img width="1419" height="561" alt="image" src="https://github.com/user-attachments/assets/9b39a76f-f0e2-4981-a1b9-abfd9f813a89" />

**GCS Destination Files Details:**


<img width="1411" height="472" alt="image" src="https://github.com/user-attachments/assets/a00a709f-5adc-4843-8032-42992e9738e4" />

**Airflow Dag Flow:**

<img width="1407" height="638" alt="image" src="https://github.com/user-attachments/assets/77268367-ec3b-4d30-80c1-be5fc511972c" />

**GCP Ephemeral Dataproc Cluster Details:**

<img width="1300" height="583" alt="image" src="https://github.com/user-attachments/assets/ce8fb528-0de2-4071-a139-32736a891b77" />

**Airflow Python Xcom Value Details:**

<img width="1428" height="469" alt="image" src="https://github.com/user-attachments/assets/27842b54-bfed-4aff-8944-55ff18a0051c" />


**Airflow Variables Implementation:**

<img width="1420" height="457" alt="image" src="https://github.com/user-attachments/assets/d932cea4-fe8d-4903-9c3c-82e11e3451af" />


