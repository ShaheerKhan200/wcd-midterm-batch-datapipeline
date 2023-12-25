import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator


# this is just an example of how to use SPARK_STEPS, you need to define your own steps
SPARK_STEPS = [
    {
        'Name': 'wcd_data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'client',
                's3://midterm-artifact/transformation.py',
                #'--input_bucket', "<data input bucket>",
                '-d', '{{ ds }}',  # tomorrow_ds
                '-data', "{{ task_instance.xcom_pull('parse_request', key='sales') }}",\
                "{{ task_instance.xcom_pull('parse_request', key='calendar') }}",\
                "{{ task_instance.xcom_pull('parse_request', key='inventory') }}",\
                "{{ task_instance.xcom_pull('parse_request', key='store') }}",\
                "{{ task_instance.xcom_pull('parse_request', key='product')}}", 
            ]
        }
    }
]

#{"Name": "Hadoop", "Version": "3.3.3"},{"Name": "Spark","Version": "3.3.1"}            

JOB_FLOW_OVERRIDES = {
    "Name": "midterm-cluster-sk",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "LogUri": "s3://midterm-emr-logs-sk/",
    "Instances": {
        "Ec2SubnetId": "subnet-038398ed2ed828959",
        "InstanceGroups": [
            {
                "Name": "Master",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "ON_DEMAND", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "arn:aws:iam::622392631460:role/service-role/AmazonEMR-ServiceRole-20230615T181640",#"AmazonEMR-ServiceRole-20230615T181640",
}

#CLUSTER_ID = "j-116YXY8CG9ZSG"

#arg.parse

DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    #'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


def _emr_output(**kwargs):
    xcomoutput = kwargs['ti'].xcom_pull(task_ids='add_steps', key='return_value')
    print(f'step id: {xcomoutput[0]}')
    print(f'All output json: {xcomoutput}')

def _start_cluster_output(**kwargs):
    xcomoutput2 = kwargs['ti'].xcom_pull(task_ids='create_emr_cluster', key='return_value')
    #print(f'step id: {xcomoutput[0]}')
    print(f'Cluster ID: {xcomoutput2}')


def retrieve_s3_files(**kwargs):
    sales_data = kwargs['dag_run'].conf['sales']
    kwargs['ti'].xcom_push(key = 'sales', value = sales_data)
    #print(kwargs)
    #print(kwargs.keys())
    product_data = kwargs['dag_run'].conf['product']
    kwargs['ti'].xcom_push(key = 'product', value = product_data)
    
    store_data = kwargs['dag_run'].conf['store']
    kwargs['ti'].xcom_push(key = 'store', value = store_data)
    
    inventory_data = kwargs['dag_run'].conf['inventory']
    kwargs['ti'].xcom_push(key = 'inventory', value = inventory_data)
    
    calendar_data = kwargs['dag_run'].conf['calendar']
    kwargs['ti'].xcom_push(key = 'calendar', value = calendar_data)
    #print(kwargs['ti'].xcom_pull(key='sales', task_ids=['parse_request']))


dag = DAG(
    'midterm_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None
)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# prints out the response from step_adder
start_cluster_response = PythonOperator(task_id = 'start_cluster_response',
                                provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
                                python_callable = _start_cluster_output,
                                dag = dag
                                ) 

# gets the output from the lambda function
parse_request = PythonOperator(task_id = 'parse_request',
                                provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
                                python_callable = retrieve_s3_files,
                                dag = dag
                                )

# adds spark steps 
step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = "{{task_instance.xcom_pull('create_emr_cluster', key='return_value')}}",#CLUSTER_ID,
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)

# prints out the response from step_adder
step_adder_response = PythonOperator(task_id = 'step_adder_response',
                                provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
                                python_callable = _emr_output,
                                dag = dag
                                ) 

step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = "{{ task_instance.xcom_pull('create_emr_cluster', key='return_value')}}",#CLUSTER_ID,
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id = "aws_default", 
    dag = dag
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)
#to check start and terminate cluster works automated
#create_emr_cluster>>start_cluster_response>>terminate_emr_cluster

parse_request>>create_emr_cluster>>step_adder>>step_checker>>terminate_emr_cluster

#step_adder>>parse_request>>step_adder_response>>step_checker

#step_adder.set_upstream(parse_request)
#parse_request.set_upstream(step_adder_response)
#step_checker.set_upstream(step_adder)



# {
#     "Cluster": {
#         "Id": "j-116YXY8CG9ZSG",
#         "Name": "midterm-cluster-sk",
#         "Status": {
#             "State": "WAITING",
#             "StateChangeReason": {
#                 "Message": "Cluster ready after last step completed."
#             },
#             "Timeline": {
#                 "CreationDateTime": "2023-07-31T22:50:50.355000+00:00",
#                 "ReadyDateTime": "2023-07-31T22:55:20.073000+00:00"
#             }
#         },
#         "Ec2InstanceAttributes": {
#             "Ec2KeyName": "ssh-key-aws",
#             "Ec2SubnetId": "subnet-038398ed2ed828959",
#             "RequestedEc2SubnetIds": [
#                 "subnet-038398ed2ed828959"
#             ],
#             "Ec2AvailabilityZone": "us-east-2a",
#             "RequestedEc2AvailabilityZones": [],
#             "IamInstanceProfile": "EMR_EC2_DefaultRole",
#             "EmrManagedMasterSecurityGroup": "sg-04c9dbf9c0af2bb47",
#             "EmrManagedSlaveSecurityGroup": "sg-0a59ad8921440f966",
#             "AdditionalMasterSecurityGroups": [],
#             "AdditionalSlaveSecurityGroups": []
#         },
#         "InstanceCollectionType": "INSTANCE_GROUP",
#         "LogUri": "s3n://midterm-emr-logs-sk/",
#         "ReleaseLabel": "emr-6.10.0",
#         "AutoTerminate": false,
#         "TerminationProtected": false,
#         "VisibleToAllUsers": true,
#         "Applications": [
#             {
#                 "Name": "Hadoop",
#                 "Version": "3.3.3"
#             },
#             {
#                 "Name": "Spark",
#                 "Version": "3.3.1"
#             }