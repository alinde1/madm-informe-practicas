from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import paramiko
import os
from anytree import Node, LevelOrderGroupIter

def sas_command(path, name):
    return '/sas/schedule/scripts/lanza_proceso_sas.sh {path} {name}'.format(**locals())


def get_bash_operator(dag, dag_parameters, name):
    path = dag_parameters['jobs_dir']
    command = 'ssh -o StrictHostKeyChecking=no -i {{ params.key }} {{ params.host }} "{{ params.sas_command }}"'
    bash_operator = BashOperator(
        task_id=name.split('-')[-1],
        bash_command=command,
        params={
            'key': dag_parameters['key'],
            'host': '{user}@{host}'.format(**dag_parameters),
            'sas_command': sas_command(path, name),
        },
        dag=dag)
    return bash_operator


def get_dummy_operator(dag, name):
    return DummyOperator(task_id=name, dag=dag)


def job_list(dag_parameters):
    path = dag_parameters['jobs_dir']
    if path[-1] != "/":
        path = '{}/'.format(path)

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=dag_parameters['host'],
                   username=dag_parameters['user'],
                   key_filename=dag_parameters['key']
                   )
    ssh_command = 'find {} -type f -name "*.sas"'.format(path)
    _, stdout, _ = client.exec_command(ssh_command)

    lines = [line.split('\n')[0].replace(path, '') for line in stdout.readlines()]
    lines.sort()

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
    items_by_parent = []
    for _parent in set([item.parent for item in items]):
        leaf_nodes = [node for node in items if node.is_leaf and node.parent == _parent]
        inner_nodes = [node for node in items if not node.is_leaf and node.parent == _parent]
        items_by_parent += leaf_nodes + inner_nodes
    return items_by_parent


def job_name(node_name):
    filename = os.path.basename(node_name)
    return filename.split('.')[0]


def get_path(node):
    return '.'.join([p.name for p in node.path])


def build_sas_dag(dag, jobs_dir, key='/usr/local/airflow/dags/emr.pem', user='user', host='X.X.X.X'):
    dag_parameters = {
        'jobs_dir': jobs_dir, # Directory containing SAS jobs (*.sas)
        'key': key,
        'user': user,
        'host': host,
    }

    jobs = job_list(dag_parameters)
    tareas = Node(tree_name(jobs_dir))
    for job in jobs:
        insert_nodes(tareas, job)

    for children in LevelOrderGroupIter(tareas):
        children_sorted = mysort(children)
        sibling = children_sorted[0]
        parent = sibling.parent
        for child in children_sorted:
            if child.parent != sibling.parent:
                parent = child.parent

            if child.is_leaf:
                task = get_bash_operator(dag, dag_parameters, job_name(child.name))
                child.dag = task
                parent.dag >> task
                parent = child
            else:
                task = get_dummy_operator(dag, get_path(child))
                child.dag = task
                if parent:
                parent.dag >> task

            sibling = child
