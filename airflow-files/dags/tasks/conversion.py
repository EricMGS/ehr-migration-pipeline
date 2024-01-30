import json, os
import pandas as pd
import numpy as np
from datetime import datetime
from airflow import AirflowException
from tasks.get_files import UploadOperator
from tasks.import_data import RunSQL


def ConversionOperator(migration_order, files_path, schemas_path, out_path, log_path, aws_connector, aws_bucket, aws_path, db_connector, tablename):
    log_file = log_path + 'conversion.log'
    f = open(log_file, 'w')
    f.write('{}\n\n'.format(datetime.now()))

    #opening migration_order for reading
    try:
        order_file = open(migration_order, 'r')
        order_dict = json.load(order_file)
        order_file.close()
        #if already did then skip
        if order_dict['conversion_status'] == True: 
            f.write('Already did')
            return
    except:
        raise AirflowException("Can't open Migration Order")
   
    #define constants and variables
    order_id = order_dict['order_id']
    files_list = os.listdir(files_path)
    system = order_dict['order_system']
    version = order_dict['order_version']

    #update migration_order try status in database
    try:
        a = RunSQL(
            task_id = "update_migration_order",
            connector = f"{db_connector}",
            sql = f'''
                UPDATE {tablename}
                SET conversion_status = false,
                    conversion_date = '{datetime.today().strftime('%Y-%m-%d')}'
                WHERE order_id = {order_id}
                '''
        )
    except:
        raise AirflowException("Can't update Migration Order")

    scripts_path = schemas_path
    #Convert every file
    for s in os.listdir(scripts_path):
        script = scripts_path + s
        cat_script_file = open(script, 'r')
        cat_script_data = json.load(cat_script_file)
        csv_file = files_path + cat_script_data['Definitions']['target_file']
        csv_filename = csv_file.split('/')[-1]
        script_name = s
        origin = pd.read_csv(csv_file)

        f.write('SCRIPT: {}\n'.format(script_name))
        #check if file exists
        if os.path.isfile(csv_file):
            f.write('FILE: {} EXISTS\n'.format(csv_filename))

            # Load files and create dataframe based on model
            model_columns = {}
            for key, value in cat_script_data['Conversion'].items():
                model_columns[key] = pd.Series(dtype= str)
            df_out = pd.DataFrame(model_columns)
            df_out.index.name = 'Index'

            # Row append
            for key, value in cat_script_data['Conversion'].items():
                df_out[key] = eval(value)

            out_file = out_path + script_name.split('.')[0] + '.csv'
            f.write('CONVERTED FILE: {}\n'.format(out_file))
            f.write('Success\n\n')
            if not os.path.exists(out_path):
                os.mkdir(out_path)
            df_out.to_csv(out_file, index=False)
            cat_script_file.close()

        else:
            f.write('FILE: {} NOT EXISTS\n\n'.format(csv_filename))
            raise AirflowException("File {} not exists".format(csv_filename))

    #upload files
    for file in os.listdir(out_path):
        UploadOperator(
            connector = aws_connector,
            bucket_name = aws_bucket,
            file = out_path + file,
            path = aws_path
        )
    f.write('FILES UPLOADED\n\n'.format(csv_filename))

    #update migration_order try status in database
    try:
        a = RunSQL(
            task_id = "update_migration_order",
            connector = f"{db_connector}",
            sql = f'''
                UPDATE {tablename}
                SET conversion_status = true
                WHERE order_id = {order_id}
                '''
        )
    except:
        raise AirflowException("Can't update Migration Order")

    f.close()
    return
    