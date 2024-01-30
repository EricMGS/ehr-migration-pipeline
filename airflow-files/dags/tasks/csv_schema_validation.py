import json, os, subprocess
from datetime import datetime
from airflow import AirflowException
from tasks.import_data import RunSQL

def SchemaValidationOperator(migration_order, files_path, schemas_path, log_path, db_connector, tablename):
    f = open(log_path + 'csv_schema_validation.log', 'w')
    f_det = open(log_path + 'csv_schema_validation_detailed.log', 'w')
    #log time
    f.write('{}\n\n'.format(datetime.now()))
    f_det.write('{}\n\n'.format(datetime.now()))

    #opening migration_order for reading
    try:
        order_file = open(migration_order, 'r')
        order_dict = json.load(order_file)
        order_file.close()
        #if already did then skip
        if order_dict['pre_validation_status'] == True: 
            f.write('Already did')
            f_det.write('Already did')
            return
    except:
        raise AirflowException("Can't open Migration Order")
    
    #define constants and variables
    ORDER_ID = order_dict['order_id']
    SYSTEM = order_dict['order_system']
    files_list = os.listdir(files_path)
    versions_path = schemas_path + SYSTEM + '/'
    versions_list = [ f.path for f in os.scandir(versions_path) if f.is_dir() ]
    versions_list.sort()
        
    #update migration_order try status in database
    try:
        a = RunSQL(
            task_id = "update_migration_order",
            connector = f"{db_connector}",
            sql = f'''
                UPDATE {tablename}
                SET pre_validation_status = false,
                    pre_validation_date = '{datetime.today().strftime('%Y-%m-%d')}'
                WHERE order_id = {ORDER_ID}
                '''
        )
    except:
        raise AirflowException("Can't update Migration Order")

    #Validate every version and files
    versions_status = {}
    for v in versions_list:
        schemas_path = v + '/schemas/'
        files_status = {}
        version_name = v.split('/')[-1]
        f.write('SYSTEM: {}\nVERSION: {}\n\n'.format(SYSTEM, version_name))
        f_det.write('SYSTEM: {}\nVERSION: {}\n\n'.format(SYSTEM, version_name))

        for s in os.listdir(schemas_path):
            schema = schemas_path + s
            csv_file = files_path + s.split('.')[0] + '.csv'
            csv_filename = csv_file.split('/')[-1]
            schema_name = s

            #check if file exists
            if os.path.isfile(csv_file):
                process = subprocess.Popen('csv-schema validate-csv {} {}'.format(csv_file, schema).split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                output, errors = process.communicate()
                process.wait()

                output_split = output.split('\n')
                errors_found = len(output_split)-2
                status = False if errors_found > 0 else True
                f.write('SCHEMA: {}\n{} Errors Found\n\n'.format(schema_name, errors_found))
                f_det.write('SCHEMA: {}\n{}\n\n'.format(schema_name, output))
            else:
                f.write('SCHEMA: {}\n{}\n\n'.format(schema_name, 'File Not found'))
                f_det.write('SCHEMA: {}\n{}\n\n'.format(schema_name, 'File Not found'))
                status = False
                
            files_status.update({csv_filename: status})
        
        versions_status.update({version_name: all(files_status.values())})
        if versions_status[version_name] == True:
            f.write('VERSION {} STATUS: OK\n{}\n\n'.format(version_name, '-'*100))
            f_det.write('VERSION {} STATUS: OK\n{}\n\n'.format(version_name, '-'*100))
        else:
            f.write('VERSION {} STATUS: INVALID\n{}\n\n'.format(version_name, '-'*100))
            f_det.write('VERSION {} STATUS: INVALID\n{}\n\n'.format(version_name, '-'*100))
    f.close()
    f_det.close()

    #Select version
    selected_versions = []
    for key, version in versions_status.items():
        if version == True:
            selected_versions.append(key)
    if len(selected_versions) == 0:
        raise AirflowException("All versions are invalid")
    elif len(selected_versions) > 1:
        raise AirflowException("There's more than one valid version")
    else:
        valid_version = selected_versions[0]

    #update migration_order try status in database
    try:
        a = RunSQL(
            task_id = "update_migration_order",
            connector = f"{db_connector}",
            sql = f'''
                UPDATE {tablename}
                SET pre_validation_status = true,
                    order_version = '{valid_version}'
                WHERE order_id = {ORDER_ID}
                '''
        )
    except:
        raise AirflowException("Can't update Migration Order")
    
    #opening migration_order file for update_version
    try:
        order_dict['order_version'] = valid_version
        with open(migration_order, 'w') as order_file:
            json.dump(order_dict, order_file)
    except:
        raise AirflowException("Can't open Migration Order")

    
    return

def ImportValidationOperator(migration_order, files_path, schemas_path, log_path, db_connector, tablename):
    f = open(log_path + 'import_validation.log', 'w')
    f_det = open(log_path + 'import_validation_detailed.log', 'w')
    f.write('{}\n\n'.format(datetime.now()))
    f_det.write('{}\n\n'.format(datetime.now()))


    #opening migration_order for reading
    try:
        order_file = open(migration_order, 'r')
        order_dict = json.load(order_file)
        order_file.close()
        #if already did then skip
        if order_dict['post_validation_status'] == True: 
            f.write('Already did')
            f_det.write('Already did')
            return
    except:
        raise AirflowException("Can't open Migration Order")
    
    #define constants and variables
    ORDER_ID = order_dict['order_id']
    SYSTEM = order_dict['order_system']
    files_list = os.listdir(files_path)

    #update migration_order try status in database
    try:
        a = RunSQL(
            task_id = "update_migration_order",
            connector = f"{db_connector}",
            sql = f'''
                UPDATE {tablename}
                SET post_validation_status = false,
                    post_validation_date = '{datetime.today().strftime('%Y-%m-%d')}'
                WHERE order_id = {ORDER_ID}
                '''
        )
    except:
        raise AirflowException("Can't update Migration Order")

    #Validate every version and files
    for s in os.listdir(schemas_path):
        schema = schemas_path + s
        csv_file = files_path + s.split('.')[0] + '.csv'
        csv_filename = csv_file.split('/')[-1]
        schema_name = s

        #check if file exists
        if os.path.isfile(csv_file):
            process = subprocess.Popen('csv-schema validate-csv {} {}'.format(csv_file, schema).split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            output, errors = process.communicate()
            process.wait()

            output_split = output.split('\n')
            errors_found = len(output_split)-2
            status = False if errors_found > 0 else True
            f.write('SCHEMA: {}\n{} Errors Found\n\n'.format(schema_name, errors_found))
            f_det.write('SCHEMA: {}\n{}\n\n'.format(schema_name, output))
        else:
            f.write('SCHEMA: {}\n{}\n\n'.format(schema_name, 'File Not found'))
            f_det.write('SCHEMA: {}\n{}\n\n'.format(schema_name, 'File Not found'))
            status = False

    #update migration_order try status in database
    try:
        a = RunSQL(
            task_id = "update_migration_order",
            connector = f"{db_connector}",
            sql = f'''
                UPDATE {tablename}
                SET post_validation_status = true
                WHERE order_id = {ORDER_ID}
                '''
        )
    except:
        raise AirflowException("Can't update Migration Order")

    f.close()
    f_det.close()
    return

