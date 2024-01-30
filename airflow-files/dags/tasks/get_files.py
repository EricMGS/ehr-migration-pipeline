from airflow.hooks.S3_hook import S3Hook
from airflow import AirflowException

def check_if_bucket_exists(connector, bucket_name):
    hook = S3Hook(connector)
    result = hook.check_for_bucket(
        bucket_name = bucket_name
    )
    return result

def check_if_key_exists(connector, bucket_name, key):
    hook = S3Hook(connector)
    result = hook.check_for_key(
        bucket_name = bucket_name,
        key = key
    )
    return result

def list_keys(connector, bucket_name, path):
    hook = S3Hook(connector)
    files_list = []
    result = hook.list_keys(
        bucket_name = bucket_name,
        prefix = path
    )
    for key in result:
        if key[-1] != '/': files_list.append(key)
    return files_list

def download_from_s3(connector, bucket_name, files_list, destination) -> None:
    hook = S3Hook(connector)

    try:
        for file in files_list:
            filename = destination + '/'.join(file.split('/')[:-1])
            hook.download_file(
                key = file,
                bucket_name = bucket_name,
                local_path = filename,
                preserve_file_name = True,
                use_autogenerated_subdir = False
            )
    except:
        raise AirflowException("Coudn't download file {} from {}".format(file, bucket_name))

def upload_to_s3(connector, bucket_name, file, path) -> None:
    filename = file.split('/')[-1]
    try:
        hook = S3Hook(connector)
        hook.load_file(
            filename = file,
            key = path + filename,
            bucket_name = bucket_name,
            replace = True
        )
    except:
        raise AirflowException("Coudn't upload file {} to {}/{}".format(file, bucket_name, path))

def DownloadOperator(connector, bucket_name, path, destination):
    if check_if_bucket_exists(connector, bucket_name) is False:
        raise AirflowException("Bucket {} not found".format(bucket_name))
    
    if check_if_key_exists(connector, bucket_name, path) is False:
        raise AirflowException("Key {} not found".format(path))
    
    files_list =  list_keys(connector, bucket_name, path)
    download_from_s3(connector, bucket_name, files_list, destination)

def UploadOperator(connector, bucket_name, file, path):
    if check_if_bucket_exists(connector, bucket_name) is False:
        raise AirflowException("Bucket {} not found".format(bucket_name))
    
    if check_if_key_exists(connector, bucket_name, path) is False:
        raise AirflowException("Key {} not found".format(path))
    
    upload_to_s3(connector, bucket_name, file, path)
