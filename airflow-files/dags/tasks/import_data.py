from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow import AirflowException
import pandas as pd
import os, json

def ImportOperator(connector, files_path, models_path, log_path, order_table, order_id, migration_order):
    f = open(log_path + 'import.log', 'w')
    f.write('{}\n\n'.format(datetime.now()))

    #opening migration_order for reading
    try:
        order_file = open(migration_order, 'r')
        order_dict = json.load(order_file)
        order_file.close()
        #if already did then skip
        if order_dict['import_status'] == True: 
            f.write('Already did')
            return
    except:
        raise AirflowException("Can't open Migration Order")

    #update migration_order try status in database
    try:
        a = RunSQL(
            task_id = "update_migration_order",
            connector = f"{connector}",
            sql = f'''
                UPDATE {order_table}
                SET import_status = false,
                    import_date = '{datetime.today().strftime('%Y-%m-%d')}'
                WHERE order_id = {order_id}
                '''
        )
    except:
        raise AirflowException("Can't update Migration Order")

    for model_name in os.listdir(models_path):
        model_json = open(models_path + model_name, 'r')
        model = json.load(model_json)
        table_name = model['name']
        file_name = model['filename']['regex']
        csv_file = pd.read_csv(files_path+file_name,sep=',',index_col=False, dtype=str, quotechar="'")
        values = str(list(csv_file.itertuples(index=False, name=None)))[1:-1]

        f.write('MODEL: {}\nFILE: {}\nTABLE: {}\n'.format(model_name, file_name, table_name))

        try:
            PostgresOperator(
                task_id = 'postgres-import-{}'.format(table_name),
                postgres_conn_id = connector,
                sql = '''
                    INSERT INTO {tabela}
                    VALUES {values}
                    ON CONFLICT DO NOTHING;
                    '''.format(tabela = table_name, values = values),
            ).execute(dict())
            f.write('Success\n\n')
        except:
            f.write('Import Error')
            raise AirflowException("Can't import file")
    
    #update migration_order try status in database
    try:
        a = RunSQL(
            task_id = "update_migration_order",
            connector = f"{connector}",
            sql = f'''
                UPDATE {order_table}
                SET import_status = true
                WHERE order_id = {order_id}
                '''
        )
    except:
        raise AirflowException("Can't update Migration Order")
        
def RunSQL(task_id, connector, sql):

    try:
        PostgresOperator(
            task_id = 'postgres-run-{}'.format(task_id),
            postgres_conn_id = connector,
            sql = sql,
            autocommit = True
        ).execute(dict())
    except:
        raise AirflowException("Can't execute this query")
