from airflow.decorators import dag
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator

@dag(
    dag_id="Search Performance Domain",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    )

def ssh_dag():
    task_1=SSHOperator(
        task_id="ssh_id",
        ssh_conn_id='ec2_connection',
        command="sh /home/adobe/scripts/shell/copyfromS3.sh ",
    )
    task_2=SSHOperator(
        task_id="ssh_id2",
        ssh_conn_id='ec2_connection',
        command="sh /home/adobe/scripts/shell/spark-run.sh ",
    )
    task_2=SSHOperator(
        task_id="ssh_id3",
        ssh_conn_id='ec2_connection',
        command="sh /home/adobe/scripts/shell/transfer_cleanup.sh ",
    )


search_performance_dag = ssh_dag()