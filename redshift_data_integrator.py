#!/usr/bin/env python3
import os
import json
import shutil
from datetime import datetime
import time
import psycopg2 # PostgreSQL database adapter for Python
import boto3
from botocore.exceptions import ClientError
import base64

import pandas as pd



############################################################  

def read_manifest(file_name):
    f = open(file_name, "rt")
    manifest_content = f.read()
    f.close()
    config = json.loads(manifest_content)
    return config

def generate_sqls_from_lq_templates(config): # generate generic SQL commands (DDL,COPY,DL)
    
    ruby_parser = os.getcwd() + '/' + config["ruby_parser"]
    lq_create_table = os.getcwd() + '/' + config["lq_create_table"]
    lq_scd2_stg = os.getcwd() + '/' + config["lq_scd2_stg"]
    lq_copy_data = os.getcwd() + '/' + config["lq_copy_data"]

    params_dir = os.getcwd() + '/' + config["params_dir"]
    stg_ddl_dir = os.getcwd() + '/' + config["stg_ddl_dir"]
    stg_dl_dir = os.getcwd() + '/' + config["stg_dl_dir"]
    src_copy_dir = os.getcwd() + '/' + config["src_copy_dir"]


    
    print('\n[{}] --2-- Generation of SQLs\n'.format(time.ctime())) #Ruby is used to parse liquid templates, suitable Python package not found
    
    for folder, dirs, files in os.walk(params_dir):
        for file in files:
            if file.endswith('.json'):
                fullpath = os.path.join(folder, file)
                dst_path = file.replace('.json','.sql')

                os.system('ruby' + ' ' + ruby_parser + ' ' + lq_create_table + ' ' + fullpath + ' ' + '>' + ' ' + stg_ddl_dir + dst_path)
                print('[{}] DDL SQLs: \t {}'.format(time.ctime(),dst_path))
                
                os.system('ruby' + ' ' + ruby_parser + ' ' + lq_copy_data + ' ' + fullpath + ' ' + '>' + ' ' + src_copy_dir + dst_path)
                print('[{}] COPY SQLs: \t {}'.format(time.ctime(),dst_path))

                os.system('ruby' + ' ' + ruby_parser + ' ' + lq_scd2_stg + ' ' + fullpath + ' ' + '>' + ' ' + stg_dl_dir + dst_path)
                print('[{}] DL SQLs: \t {}'.format(time.ctime(),dst_path))

def get_entity_param_definitions(config): # get parameter from JSON
    
    config = read_manifest("config.json")
    
    params_dir = os.getcwd() + '/' + config["params_dir"]
    src_copy_dir = os.getcwd() + '/' + config["src_copy_dir"] 

    entity_params = []
    
    for folder, dirs, files in os.walk(params_dir):
            for file in files:
                if file.endswith('.json'):
                    fullpath = os.path.join(folder, file)
                
                    with open(fullpath) as f:
                        data = json.load(f)
                        
                        file_format_short = ('gz' if data['file_format'].lower() == 'gzip' else data['file_format']).lower()
                        file_format_short_comma = "." + file_format_short
                        file_format_long = data['file_format'].upper()
                        delimiter_string = data['delimiter_string']
                        #print (file.replace('.json','') + ',' + data['ignore_header'])
                        #print(item['date'])

                        list = (file.replace('.json',''), data['ignore_header'],delimiter_string,file_format_short,file_format_short_comma,file_format_long)
                        entity_params.append(list)

                    #with open('config.json', 'w') as f:
                    #    json.dump(data, f)
    ep_df = pd.DataFrame(entity_params, columns =['param_file', 'ignore_header','delimiter_string','file_format_short','file_format_short_comma','file_format_long'])
    #print(ep_df)
    return(ep_df)


def pair_entity_param_definitions_with_s3_files(config): # get parameter from JSON
    
    config = read_manifest("config.json")
    
    parent_child_df = list_s3_files(config)
    ep_df = get_entity_param_definitions(config)

    merge_df = pd.merge(parent_child_df,ep_df,on='param_file',how='left')

    #lst3 = [(i, ignore_header, b, c, d) for i, ignore_header in entity_params for j, b, c, d in parent_child if i==j]

    return(merge_df)
    

                    

def create_dirs(config):
   
    stg_ddl_dir = os.getcwd() + '/' + config["stg_ddl_dir"]
    stg_dl_dir = os.getcwd() + '/' + config["stg_dl_dir"]
    src_copy_dir = os.getcwd() + '/' + config["src_copy_dir"]
    src_copy_comm_dir = os.getcwd() + '/' + config["src_copy_comm_dir"]


    print('\n[{}] --1-- Creation of dirs\n'.format(time.ctime()))
    try:
              
        shutil.rmtree(stg_ddl_dir, ignore_errors=True)
        shutil.rmtree(stg_dl_dir, ignore_errors=True)
        shutil.rmtree(src_copy_dir, ignore_errors=True)

        os.makedirs( stg_ddl_dir, exist_ok=True)
        print('[{}] DDL SQL DIR: \t {}'.format(time.ctime(),config["stg_ddl_dir"]))
        
        os.makedirs( stg_dl_dir, exist_ok=True)
        print('[{}] DL SQL DIR: \t {}'.format(time.ctime(),config["stg_dl_dir"]))

        os.makedirs( src_copy_dir, exist_ok=True)
        print('[{}] COPY SQL DIR: \t {}'.format(time.ctime(),config["src_copy_dir"]))

        os.makedirs( src_copy_comm_dir, exist_ok=True)
        print('[{}] COPY COMMAND SQL DIR: \t {}'.format(time.ctime(),config["src_copy_comm_dir"]))

     
    except Exception as e:
        print('Processed records exception.', e)
        raise e

    
############################################################

def connect_to_db():
    
    s3 = boto3.client('s3')
    config = read_manifest("config.json")
    
    conn = connection(config)
    
    print('\n[{}] --4-- Connect to db and executing SQL\n'.format(time.ctime()))
    
    try:
        global cursor
        cursor = conn.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(conn.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT version();")
        # Fetch result
        version = cursor.fetchone()
        print("You are connected to - ", version, "\n")
        
        execute_sql_stg_ddl(conn, cursor,config)
        execute_sql_src_copy_data(conn, cursor,config)
        execute_sql_stg_dl(conn, cursor,config)
        

    except Exception as e:
        print('Processed records exception.', e)
        raise e
    finally:
        if (conn):
            cursor.close()
            conn.close()
        print("PostgreSQL connection is closed")

def connection(config):
    try:
        conn = psycopg2.connect("dbname=" + config["db_database"]
            + " user=" + config["db_user"]
            + " password=" + config["db_password"]
            + " port=" + config["db_port"]
            + " host=" + config["db_host"]
            + " sslmode=" + config["sslmode"]
            + " sslrootcert=" + config["sslrootcert"]
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        print("Invalid connection", e)
        raise e


############################################################
def list_s3_files(config):

    params_dir = os.getcwd() + '/' + config["params_dir"]
    
    parent_child = []
    for folder, dirs, files in os.walk(params_dir): # for each file in "params_dir" get its "data_location" (S3), for each csv file in that "data location" return (param_file, s3_file,s3_bucket,data_location) = parent_child
        for file in files:
            fullpath = os.path.join(folder, file)
            read_manifest(fullpath)
            
            #Creating Session With Boto3.
            session = boto3.Session(
            aws_access_key_id = get_secret(read_manifest(fullpath)["s3_secret_arn"])["S3-data-access-key"], # get s3 secrets("s3_secret_arn") from params file (fullpath)
            aws_secret_access_key = get_secret(read_manifest(fullpath)["s3_secret_arn"])["S3-data-secret-key"] # get s3 secrets("s3_secret_arn") from params file (fullpath)
            ) 
            #Creating S3 Resource From the Session.
            s3 = session.resource('s3')

            data_location = read_manifest(fullpath)["data_location"]
            data_processed_dir = read_manifest(fullpath)["data_processed_dir"]
            bucket = s3.Bucket(read_manifest(fullpath)["s3_bucket"])

            file_format_short_2 = ('gz' if read_manifest(fullpath)['file_format'].lower() == 'gzip' else read_manifest(fullpath)['file_format']).lower()
            file_format_short_comma_2 = "." + file_format_short_2

            
            for object_summary in bucket.objects.filter(Prefix=data_location):
                key = object_summary.key
                if key.endswith(file_format_short_comma_2):
                    should_skip = any(substring in key for substring in data_processed_dir)
                    if not should_skip:
                            param_file = file.replace('.json','')
                            s3_file =  key
                            s3_bucket = read_manifest(fullpath)["s3_bucket"]
                            data_location = data_location


                            list = (param_file, s3_file,s3_bucket,data_location)
                            parent_child.append(list)

            parent_child_df = pd.DataFrame(parent_child, columns =['param_file', 's3_file','s3_bucket','data_location'])

    return(parent_child_df)
    #print(parent_child_df)

def move_s3_files_to_processed_dir(config):

    params_dir = os.getcwd() + '/' + config["params_dir"]
    
    print('\n[{}] --5-- Move S3 files to processed folder\n'.format(time.ctime()))

    for folder, dirs, files in os.walk(params_dir): # for each file in "params_dir" get its "data_location" (S3), for each csv file in that "data location" return (param_file, s3_file,s3_bucket,data_location) = parent_child
        for file in files:
            fullpath = os.path.join(folder, file)
            read_manifest(fullpath)
            
            #Creating Session With Boto3.
            session = boto3.Session(
            aws_access_key_id = get_secret(read_manifest(fullpath)["s3_secret_arn"])["S3-data-access-key"], # get s3 secrets("s3_secret_arn") from params file (fullpath)
            aws_secret_access_key = get_secret(read_manifest(fullpath)["s3_secret_arn"])["S3-data-secret-key"] # get s3 secrets("s3_secret_arn") from params file (fullpath)
            ) 
            #Creating S3 Resource From the Session.
            s3 = session.resource('s3')

            data_location = read_manifest(fullpath)["data_location"]
            data_processed_dir = read_manifest(fullpath)["data_processed_dir"]
            bucket = s3.Bucket(read_manifest(fullpath)["s3_bucket"])

            file_format_short_2 = ('gz' if read_manifest(fullpath)['file_format'].lower() == 'gzip' else read_manifest(fullpath)['file_format']).lower()
            file_format_short_comma_2 = "." + file_format_short_2


            for object_summary in bucket.objects.filter(Prefix=data_location):
                key = object_summary.key
                if key.endswith(file_format_short_comma_2):
                    should_skip = any(substring in key for substring in data_processed_dir)
                    if not should_skip:
                    
                        param_file = file.replace('.json','')
                        s3_bucket = read_manifest(fullpath)["s3_bucket"]
                        current_date_dir = datetime.today().strftime('%Y%m%d') # current date subfolder in processed dir

                        tgt_key = key.replace(data_location,data_processed_dir[-1] +  current_date_dir + "/")  #take last member from data_processed_dir- rewrite this

                        #copy file to processed folder = copy + delete
                        s3.Object(s3_bucket, tgt_key).copy_from(CopySource=s3_bucket + "/" + key)
                        s3.Object(s3_bucket, key).delete()

                        print('[{}] {} - {} --> {}.\n'.format(time.ctime(),param_file,s3_bucket+ "/" + key,s3_bucket + "/" + tgt_key))
                    
                    

def generate_copy_commands(config): # generate specific COPY command for each file specified in parent_child (paired by table_name, S3 dir = param_definitons file)
    src_copy_dir = os.getcwd() + '/' + config["src_copy_dir"]
    src_copy_comm_dir = os.getcwd() + '/' + config["src_copy_comm_dir"]
    parent_child = list_s3_files(config)
    merge_df = pair_entity_param_definitions_with_s3_files(config).reset_index() 

    iam_role = config["iam_role"]

    
    print('\n[{}] --3-- Generation of COPY COMMANDS for S3 files\n'.format(time.ctime()))
    
    for folder, dirs, files in os.walk(src_copy_dir):
        for file in files:
            fullpath = os.path.join(folder, file)
            for index, row in merge_df.iterrows():
                #print(row['param_file'], row['s3_bucket'])
                if file.replace('.sql','') == row['param_file']:

                    row_s3_file = row['s3_file'].replace('/','_')
                    #print(fullpath,src_copy_comm_dir,row['s3_file'])

                    shutil.copyfile(fullpath,src_copy_comm_dir + (row_s3_file.replace(row['file_format_short_comma'],'.sql').replace(row['data_location'],'')))
#
                    with open(src_copy_comm_dir + (row_s3_file.replace(row['file_format_short_comma'],'.sql').replace(row['data_location'],''))) as f:
                            s = f.read()
                            s = s.replace('src_file', 's3://{}/{}'.format(row['s3_bucket'],row['s3_file']))
                            s = s.replace('iam_role', '{}'.format(iam_role))
                            s = s.replace('ignore_header', '{}'.format(row['ignore_header']))
                            s = s.replace('delimiter_string', '{}'.format(row['delimiter_string']))
                            s = s.replace('format_param', '{}'.format(row['file_format_long'] + ' REMOVEQUOTES' if row['file_format_long'] == 'GZIP' else row['file_format_long'] ))
                    with open(src_copy_comm_dir + (row_s3_file.replace(row['file_format_short_comma'],'.sql').replace(row['data_location'],'')), "w") as f:
                            f.write(s)
                    
                            print('[{}] COPY COMMAND SQL: \t {}'.format(time.ctime(),(row_s3_file.replace(row['file_format_short_comma'],'.sql').replace(row['data_location'],''))))


def generate_unload_to_s3_commands(config):
    stg_dl_dir = os.getcwd() + '/' + config["stg_dl_dir"]
    unload_to_s3_s3_dir  = config["unload_to_s3_s3_dir"]
    unload_to_dev_s3_s3_dir = config["unload_to_dev_s3_s3_dir"] # DEV S3 bucket
    iam_role = config["iam_role"]

       
    for folder, dirs, files in os.walk(stg_dl_dir):
        for file in files:
            fullpath = os.path.join(folder, file)

            with open(fullpath) as f:
                    s = f.read()
                    s = s.replace('iam_role', '{}'.format(iam_role))
                    s = s.replace('<unload_to_s3_s3_dir>', '{}'.format(unload_to_s3_s3_dir))
                    s = s.replace('<unload_to_dev_s3_s3_dir>', '{}'.format(unload_to_dev_s3_s3_dir))
                    s = s.replace('<yyyymmddHHMMSS>', '{}'.format(datetime.today().strftime('%Y%m%d%H%M%S')))
            with open(fullpath, "w") as f:
                    f.write(s)
            
############################################################
def execute_sql_stg_ddl(connection, cursor,config):
    
    stg_ddl_dir = os.getcwd() + '/' + config["stg_ddl_dir"]
    
    print('[{}] Creation of src and stg tables:\n'.format(time.ctime()))
    
    for folder, dirs, files in os.walk(stg_ddl_dir):
        for file in files:
            fullpath = os.path.join(folder, file)
            
            f = open(fullpath, "rt")
            sql = f.read()
        
            try:
                cursor.execute(sql)
                #print("Schema is ready.")
                print('[{}] \t{} - src + stg tables created.\n'.format(time.ctime(),file.replace('.sql','')))
            except Exception as e:
                print("Error while preparing schema.", e)
                cursor.close()
                connection.close()
                raise e

def execute_sql_src_copy_data(connection, cursor,config):
    
    src_copy_comm_dir = os.getcwd() + '/' + config["src_copy_comm_dir"]
    
    print('[{}] Copy data:\n'.format(time.ctime()))
    
    for folder, dirs, files in os.walk(src_copy_comm_dir):
        for file in files:
            fullpath = os.path.join(folder, file)
            
            f = open(fullpath, "rt")
            sql = f.read()
        
            try:
                cursor.execute(sql)
                #print("Schema is ready.")
                print('[{}] \t{} - copied to src.\n'.format(time.ctime(),file.replace('.sql','')))
            except Exception as e:
                print("Error while preparing schema.", e)
                cursor.close()
                connection.close()
                raise e

def execute_sql_stg_dl(connection, cursor,config):
    
    stg_dl_dir = os.getcwd() + '/' + config["stg_dl_dir"]
    
    print('[{}] Execution of stg tables load:\n'.format(time.ctime()))
    
    for folder, dirs, files in os.walk(stg_dl_dir):
        for file in files:
            fullpath = os.path.join(folder, file)
            
            f = open(fullpath, "rt")
            sql = f.read()
        
            try:
                cursor.execute(sql)
                #print("Schema is ready.")
                print('[{}] \t{} - stg dl executed.\n'.format(time.ctime(),file.replace('.sql','')))
            except Exception as e:
                print("Error while preparing schema.", e)
                cursor.close()
                connection.close()
                raise e

                
############################################################
def get_secret(secret_name):

    region_name = "us-east-2"
    secret = ""

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return json.loads(secret) 

############################################################

def main():

    config = read_manifest("config.json")

    
    create_dirs(config)  
    generate_sqls_from_lq_templates(config)
    
    pair_entity_param_definitions_with_s3_files(config)
       #list_s3_files(config)
       #get_entity_param_definitions(config)
    generate_copy_commands(config)
    generate_unload_to_s3_commands(config)
    connect_to_db()
    move_s3_files_to_processed_dir(config)

############################################################
if __name__ == "__main__":
    main()
