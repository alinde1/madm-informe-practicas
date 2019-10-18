from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import paramiko
import os
import time
from anytree import Node, LevelOrderGroupIter


################################################################
# DAG PARAMETERS
################################################################

dag_parameters = {
    'jobs_dir': 'dags/tareas',                        # Directory containing SAS jobs (*.sas)
    'key': 'dags/emr.pem',
    'user': '',
    'host': '',
}

default_args = {
    'owner': 'CI-IT_SAS',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 7, 0, 0, 0),
    'email': ['admin@example.com',
              ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}


dag_sas = DAG('sas_tareas_v3',
              default_args=default_args,
              schedule_interval='0 * * * *',
              concurrency=1,
              max_active_runs=1,
              )


################################################################
# BUILD DAG FROM DIRECTORY CONTENTS
################################################################

def sas_command(name):
    date = time.strftime('%Y%m%d_%H.%M.%S')
    logfile = "/sas/Logs/{name}_{date}.log".format(**locals())
    sasfile = "/sas/{name}.sas".format(**locals())
    sascmd = "/sas/sasbatch.sh"

    return '{sascmd} -log {logfile} -batch -noterminal -logparm "rollover=session" -sysin {sasfile}'.format(**locals())


def get_bash_operator(dag, name, path):
    command = 'ssh -i {{ params.key }} {{ params.host }} "echo {{ params.path }}/{{ params.job_name }}.sas {{ ds }}"'
    bash_operator = BashOperator(
                        task_id=name.split('-')[-1],
                        bash_command=command,
                        params={
                            'key': dag_parameters['key'],
                            'host': '{user}@{host}'.format(**dag_parameters),
                            'path': path,
                            'job_name': name
                        },
                        dag=dag)
    return bash_operator


def get_dummy_operator(dag, name):
    return DummyOperator(task_id=name, dag=dag)


def job_list(path):
    if path[-1] != "/":
        path = '{}/'.format(path)

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=dag_parameters['host'],
                   username=dag_parameters['user'],
                   key_filename=dag_parameters['key'])

    ssh_command = 'find {} -type f -name "*.sas"'.format(path)
    _, stdout, _ = client.exec_command(ssh_command)

    lines = [line.split('\n')[0].replace(path, '') for line in stdout.readlines()]
    lines.sort()
    print("lines: {}".format(lines))

    return lines


def get_child(tree, node_name):
    for node in tree.children:
        if node.name == node_name:
            return node
    return None


def insert_nodes(tree, nodes):
    p = tree
    for c in nodes.split('/'):
        n = get_child(p, c)
        if n:
            p = n
        else:
            n = Node(c, parent=p)
            p = n


def tree_name(path):
    if path[-1] == "/":
        path = path[:-1]
    return os.path.basename(path)


def mysort(items):
    leaf_nodes = [node for node in items if node.is_leaf]
    inner_nodes = [node for node in items if not node.is_leaf]
    return leaf_nodes + inner_nodes


def job_name(node_name):
    filename = os.path.basename(node_name)
    return filename.split('.')[0]


def get_path(node):
    return '.'.join([p.name for p in node.path])


jobs_dir = dag_parameters['jobs_dir']
jobs = job_list(jobs_dir)
tareas = Node(tree_name(jobs_dir))
for job in jobs:
    insert_nodes(tareas, job)
print(jobs)

for children in LevelOrderGroupIter(tareas):
    children_sorted = mysort(children)
    sibling = children_sorted[0]
    parent = sibling.parent
    for child in children_sorted:
        if child.parent != sibling.parent:
            parent = child.parent

        if child.is_leaf:
            task = get_bash_operator(dag_sas, job_name(child.name), jobs_dir)
            child.dag = task
            parent.dag >> task
            parent = child
        else:
            task = get_dummy_operator(dag_sas, get_path(child))
            child.dag = task
            if parent:
                parent.dag >> task

        sibling = child
