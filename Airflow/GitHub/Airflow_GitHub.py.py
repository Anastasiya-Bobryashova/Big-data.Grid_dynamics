from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.operators.python import PythonOperator
from operator import itemgetter
from datetime import datetime
from airflow import DAG
import requests
import json

token = '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'
headers = {'Accept': 'application/vnd.github+json', 'authorization': f'Bearer {token}'}
max_id = 0


def get_organizations():
    list_of_organizations = []
    url = 'https://api.github.com/organizations?per_page=100'

    for _ in range(2):
        global max_id
        parameter = {'since': max_id}
        response = requests.get(url, params = parameter, headers = headers)
        response = response.json()
        list_of_organizations.extend([organization['login'] for organization in response])
        max_id = max([organization['id'] for organization in response]) + 1
    return json.dumps(list_of_organizations)


def get_repositories(organizations):
    list_of_repositories = []
    organizations = json.loads(organizations)
    for organization in organizations:
        link = 'https://api.github.com/orgs/' + organization + '/repos'
        repositories = requests.get(link, headers = headers).json()
        list_of_repositories.extend(
            [[repository['owner']['login'], repository['name'], repository['stargazers_count']] for repository in
             repositories])
    return json.dumps(list_of_repositories)


def get_top_repositories(repositories):
    repositories = json.loads(repositories)
    repositories.sort(key = itemgetter(2), reverse = True)
    top_repositories = repositories[:20]
    return json.dumps(top_repositories)


def insert_top_repositories_in_table(top_repositories):
    top_repositories = json.loads(top_repositories)
    hook = SqliteHook()

    for repository in top_repositories:
        parameters = [repository[0], repository[1], repository[2]]
        hook.run("""
        INSERT INTO Top_repositories (Organization, Repository, Number_of_stars) VALUES (?, ?, ?)
        """, parameters = parameters)


with DAG(
        dag_id = 'GitHub_top_20_repos',
        start_date = datetime(2022, 2, 22),
        schedule_interval = '@hourly',
        catchup = False) as dag:

    fetch_orgs = PythonOperator(
        task_id = 'fetch_organizations',
        python_callable = get_organizations,
    )

    fetch_repos = PythonOperator(
        task_id ='fetch_repositories',
        python_callable = get_repositories,
        op_kwargs = {'organizations': "{{task_instance.xcom_pull(task_ids = 'fetch_organizations')}}"}
    )

    fetch_top_repos = PythonOperator(
        task_id = 'fetch_top_repositories',
        python_callable = get_top_repositories,
        op_kwargs = {'repositories': "{{task_instance.xcom_pull(task_ids='fetch_repositories')}}"}
    )

    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = """
        CREATE TABLE Top_repositories (
            Organization TEXT,
            Repository TEXT,
            Number_of_stars TEXT
        );
        """
    )

    insert_data_in_table = PythonOperator(
        task_id = 'insert_data_in_table',
        python_callable = insert_top_repositories_in_table,
        op_kwargs = {'top_repositories': "{{task_instance.xcom_pull(task_ids='fetch_top_repositories')}}"}
    )

    drop_table = SqliteOperator(
        task_id = 'drop_table',
        sql = """
            DROP TABLE IF EXISTS Top_repositories;
            """
    )

drop_table >> create_table
fetch_orgs >> fetch_repos >> fetch_top_repos >> insert_data_in_table
