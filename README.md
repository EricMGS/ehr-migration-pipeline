# The Project: EHR Migration Pipeline
   
 **Data migration pipeline for EHRs, built for enhanced reliability**  

This software is a data migration pipeline for EHRs and related data, like patient records and appointments.   
It aims to simplify the creation and maintenance of scripts that convert and migrate data between different system standards.  


The software provides an orchestrated pipeline with the following processes:  
  
1. **System identification and file versioning:** The software automatically identifies the source system and file version to ensure compatibility.  
2. **Feasibility testing:** The software performs tests to confirm the viability of the migration.  
3. **Data categorization:** The software categorizes the data before conversion to apply centralized rules and maintain database format independence.  
4. **Data conversion:** The software converts the data using a centralized repository of rules.  
5. **Data import:** The software imports the converted data into the destination system.  


## Benefits:  
  
- **Rules-based development:** Define data conversion rules, not code, for easier script creation and maintenance.  
- **Automatic system and version identification:** Ensures the correct conversion rules are applied.  
- **Increased flexibility:** Migrate data from various systems.  
- **Improved accuracy:** Data validation features minimize errors.  
- **Centralized maintenance:** Easily manage and update conversion rules.  
- **Destination database format independence:** Centrally maintain rules regardless of database format changes.  
- **Scale and Extensibility:** Migrate data from a wide range of EHR and related systems.  
    
This software simplifies EHR data migration with enhanced reliability, making it a valuable tool for organizations navigating complex data transfer.  
   
## Used technologies
- Airflow
- Amazon S3  
- Amazon RDS  
- Postgres  

## Pipeline tasks
1. **Get files:** The files of the origin system should be stored in an S3 bucket. This task recover the files from S3 based on the _Migration ID_   
2. **System identification and file versioning:** The software automatically identifies the source system and file version to ensure compatibility. The version could be informed by the user, or identified automatically.  
3. **Feasibility testing:** The software performs tests to confirm the viability of the migration. It compares the files with models with rules of integrity like datatype, null, min and max, regex, list of values, etc.
4. **Data categorization:** The software categorizes the data before conversion to apply centralized rules and maintain database format independence. The categorization transforms the origin files in intermediate files with global standards. The intermediate files are uploaded to S3.
5. **Data conversion:** The software converts the intermediate files using a centralized repository of rules. The intermediate files provides database independence and centralized maintaince. The converted files are uploaded to S3.
6. **Import validation:** The software validates the possibility of importing the converted files.
7. **Data import:** The software imports the converted data into the destination system.  

## Orchestration
All the tasks are orchestrated by a _Migration Order_. That migration order contains general informations like order id, system, version and informations about the task like date of execution and status.   
If a task has already been completed successfully, the orchestrator will jump to the next pending step.

## Installation
### Using Docker


### From zero (tested on Ubuntu 22.04.3 LTS)
1. Install initial packages and create a postgres user:    
```
$ sudo apt update  
$ sudo apt upgrade  
$ sudo apt install python3-pip  
$ sudo apt install sqlite3  
$ sudo apt install python3.10-venv  
$ sudo apt-get install libpq-dev   
$ python3 -m venv venv  
$ source venv/bin/activate  
$ pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"  
$ airflow db init  
$ sudo apt-get install postgresql postgresql-contrib  
$ sudo -i -u postgres  
postgres$ psql  
> CREATE DATABASE airflow;  
> CREATE USER airflow WITH PASSWORD 'airflow';  
> GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;  
> \q  
postgres$ exit
```
   
2. Copy and paste everything inside the "airflow-files" folder into the airflow root   
3. Install dependencies:   
```
$ pip install -r dependencies.txt   
```   
   
4. Create airflow user and start DAG
```
$ airflow db init  
$ airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow@gmail.com -p airflow     
$ airflow dags unpause ehr-transformation-pipeline
```

5. Start scheduler and webserver (you need to do this everytime the machine boots)
```
$ source venv/bin/activate
$ airflow scheduler &
$ airflow webserver
```

## Directory Tree  
The project should have the following directory structure:   

<pre>
airflow
├── dags  
│   ├── ehr-transformation-dag.py                Pipeline Orchestrator. Run this to execute the pipeline
│   └── tasks  
│       ├── categorization.py 
│       ├── conversion.py  
│       ├── csv_schema_validation.py  
│       ├── get_files.py  
│       └── import_data.py  
├── logs                                         Logs From every task on pipeline
│   ├── categorization.log  
│   ├── conversion.log  
│   ├── csv_schema_validation.log  
│   ├── csv_schema_validation_detailed.log  
│   ├── import.log  
│   ├── import_validation.log  
│   ├── import_validation_detailed.log  
├── migrations                                   This folder is downloaded from S3, according to the order_id. It contains 3 subfiles. 
│   └── migration_{ORDER_ID}   
│       ├── categorized_files                    csv files categorized   
│       ├── converted_files                      csv files converted to target system   
│       └── input_files                          csv files from source system
└── project                                      Here goes all the models that make the pipeline work. It is independent of the source code and varies depending on the target system.   
    ├── definitions                              Constant rules of target system   
    │   ├── conversion_models                    Defines rules to convert csv files to the same structure of the target system tables  
    │   └── import_schemas                       Schemas that validate the converted files and rules to import them
    ├── migration_order.json                     File with pipeline run infomations. This file indicates the tasks execution status.
    └── models                                   Models to validate and categorize csv files from source systems. It will have as many folders as source systems.
        └── system-1{ORDER_ID}   
            └── v1                               Version of the system. It will have as many folders as versions.
                ├── categorization_scripts       Models to categorize source system csv files
                └── schemas                      Models to validate source system csv files and identify version, if not given  
</pre>


## Using
### Creating S3 Bucket
The S3 bucket should have the following directory structure:   
<pre>
migrations
├── migration_{MIGRATION_ID}  
│   ├── input_files               The origin files goes here
│   ├── categorized_files
│   └── converted_files
├── migration_{MIGRATION_ID}
|   ├── ...
...
</pre>

### Creating Postgres tables
The postgres server should have the following tables:   
- migration_orders
- patients
- schedules
- records

The migration_orders table will contain all migration orders while the patients, schedules and records tables will contain the application data where the imports will be made.   
Patients, schedules and records tables could have any desired format, and this formats should be defined in the **definition models**   
The table migration_orders should have the following data:   
```sql
order_id integer primary key,
order_date date,
order_system varchar(30),
order_version varchar(10),
pre_validation_date date,
pre_validation_status bool,
categorization_date date,
categorization_status bool,
conversion_date date,
conversion_status bool,
post_validation_date date,
post_validation_status bool,
import_date date,
import_status bool
```


### Creating connections
Generate AWS access keys in: Security Credentials -> Access keys -> Generate access key   
Access airflow webserver: http://localhost:8080  
Login with the created user (user: airflow, password: airflow)   
Create a new connection of the type Amazon Web Services and name it **aws_conn**. Paste the access keys.   
Create a new connection of the type Postgres and name it **postgres_conn**. Insert the host and credentials.   

### Creating definition models

### Creating validation models

### Creating categorization and conversion models

### Configuring constants


### Running
```
$ airflow dags trigger ehr-transformation-pipeline --conf '{"order_id":"{ORDER_ID}"}'      
```

## Troubleshooting

