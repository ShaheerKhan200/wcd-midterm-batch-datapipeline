import json
import boto3
import time
import subprocess
from datetime import datetime, timedelta
from send_email import send_email

def lambda_handler(event, context):
    s3_file_list = []
    
    s3_client=boto3.client('s3')
    for object in s3_client.list_objects_v2(Bucket='midterm-data-dump-sk')['Contents']:
        s3_file_list.append(object['Key'])
    print('s3_file_list', s3_file_list)
    
    #datestr = time.strftime("%Y%m%d")
    #yesterdays date
    print((datetime.today()-timedelta(days=1)).strftime("%Y%m%d"))
    
    # to get the date in the following format 20230703
    datestr = (datetime.today()-timedelta(days=1)).strftime("%Y%m%d") #(time.strftime("%Y%m%d"))-1
    print(datestr)
    
    required_file_list = [f'calendar_{datestr}.csv', f'inventory_{datestr}.csv', f'product_{datestr}.csv', f'sales_{datestr}.csv', f'store_{datestr}.csv']
    
    print('required_file_list', required_file_list)
    
    # scan S3 bucket
    if all(item in list(set(s3_file_list)) for item in list(set(required_file_list))):
        s3_file_url = ['s3://' + 'midterm-data-dump-sk/' + a for a in required_file_list]#s3_file_list]
        table_name = [a[:-13] for a in required_file_list]#s3_file_list]
        output = {}
        for a in list(set(table_name)):
            for b in s3_file_url:
                if a in b:
                    if 'conf' not in output:
                        output['conf'] = []
                    output['conf'].append({a: b})
        result_dict = {}
        # output['conf'] is list of dicts
        for d in output['conf']:
            result_dict.update(d)
        
        #print(result_dict)
        output_new = {}
        output_new['conf'] = result_dict
        
        data = json.dumps(output_new)
        #data = json.dumps({'conf':{a:b for a in list(set(table_name)) for b in s3_file_url}})
        print(data)
        
    # send signal to Airflow    
        endpoint= 'http://3.135.25.193:8080/api/v1/dags/midterm_dag/dagRuns'
    
        subprocess.run(['curl',
                        '-X',
                        'POST',
                        endpoint,
                        '-H',
                        'Content-Type: application/json',
                        '--user',
                        'airflow:airflow',
                        '--data',
                        data,
                        ])
        print('File are sent to Airflow')
    else:
        print("doesn't work")
        send_email()