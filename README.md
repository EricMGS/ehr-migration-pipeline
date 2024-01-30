# EHR Migration Pipeline
   
 **Data migration pipeline for EHRs, built for enhanced reliability**  

This software is a data migration pipeline for EHRs and related data, like patient records and appointments.   
It aims to simplify the creation and maintenance of scripts that convert and migrate data between different system standards.  

-----
The software provides an orchestrated pipeline with the following processes:  
  
1. **System identification and file versioning:** The software automatically identifies the source system and file version to ensure compatibility.  
2. **Feasibility testing:** The software performs tests to confirm the viability of the migration.  
3. **Data categorization:** The software categorizes the data before conversion to apply centralized rules and maintain database format independence.  
4. **Data conversion:** The software converts the data using a centralized repository of rules.  
5. **Data import:** The software imports the converted data into the destination system.  

-----
Benefits:  
  
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


### Creating Postgres tables


### Creating connections
Gere chaves de acesso em AWS -> Security Credentials -> Chaves de Acesso -> Criar chave de acesso  
Access http://localhost:8080  
Login with the created user  
Crie uma conexao pelo airflow webserver de tipo: Amazon Web Services e nome: aws_conn. Copie e cole as chaves de acesso  
Crie uma conexao pelo airflow webserver de tipo: Postgres e nome: postgres_conn. Insira as informacoes de acesso ao servidor Postgres  

### Creating models

### Running
$ airflow dags trigger ehr-transformation-pipeline --conf '{"order_id":"{ORDER_ID}"}'      


## Troubleshooting

