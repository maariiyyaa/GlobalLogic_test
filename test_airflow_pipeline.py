from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from datetime import datetime
from sklearn import datasets
import pandas as pd
import numpy as np
import json

default_args = {
    'start_date': datetime(2021, 2, 15),
    'schedule_interval': '@once',
}

def _iris_data_generation():
    iris_ds = datasets.load_iris()
    iris_dataframe = pd.DataFrame(iris_ds['data'], columns = iris_ds['feature_names'])
    iris_dataframe['target'] = iris_ds['target']
    # ???
    # result = iris_dataframe.to_json(orient="records")
    # iris_json = json.loads(result)
    # ti.xcom_push(key = 'iris_df', value = iris_json)
    iris_dataframe.to_csv('/tmp/iris.csv', index=None, header=False)

def _extract_iris_with_hook(ti):
    request = "SELECT * FROM iris"
    #connection from Connection wiev in Airflow UI
    postgres_hook = PostgresHook(postgres_conn_id = 'postgres_default')
    #Connection from postgreshook
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sourse = cursor.fetchall()
    print(sourse)
    ti.xcom_push(key = 'iris_data', value =sourse)


def _train_model(ti, **kwargs):
    dataset = np.array(ti.xcom_pull(key = 'iris_data',
                                    task_ids = 'extract_iris_with_hooh'))
    features = dataset[:, :3]
    target = dataset[:,4]
    Xtrain, Xtest, Ytrain, Ytest = train_test_split(features, target, test_size = 0.2)
    model = LogisticRegression(penalty = kwargs['penalty'])
    model.fit(Xtrain, Ytrain)
    result = model.predict(Xtest)
    accuracy = accuracy_score(Ytest, result)
    ti.xcom_push(key = 'model_accuracy', value = accuracy)

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(
        key = 'model_accuracy', task_ids=[
            'train_models.train_model_1',
            'train_models.train_model_2'
        ])
    print(f'the best model with Accuracy: {np.max(accuracies)}')



with DAG('test_airflow_pipeline', default_args =default_args,
         catchup = False) as dag:
    
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres_default',
        sql = '''
        CREATE TABLE IF NOT EXISTS iris(
            sepal_length float8 NOT NULL,
            sepal_width float8 NOT NULL,
            petal_length float8 NOT NULL,
            petal_width float8 NOT NULL,
            target SMALLINT NOT NULL
        )
        '''
        )

    iris_data_generation = PythonOperator(
        task_id = 'iris_data_generation',
        python_callable = _iris_data_generation,
        provide_context = True #######
    )
    # ???
    # store_iris = BashOperator(
    #     task_id = 'store_iris',
    #     bash_command = 'echo -e ".separator ", "\n.import /tmp/iris.csv iris" | set PGPASSWORD=airflow sudo -u postgres psql postgres@localhost/postgres'
    # )
    store_iris_sql = PostgresOperator(
        task_id = 'store_iris_data',
        postgres_conn_id = 'postgres_default',
        sql = '''COPY iris FROM '/tmp/iris.csv' DELIMITER ',' CSV'''
    )
    
    extract_iris_with_hooh = PythonOperator(
        task_id = 'extract_iris_with_hooh',
        python_callable = _extract_iris_with_hook
    )

    with TaskGroup('train_models') as tasks:
        train_model_1 = PythonOperator(
            task_id = 'train_model_1',
            python_callable = _train_model,
            op_kwargs={'penalty':'none'},
            provide_context = True
        )
        train_model_2 = PythonOperator(
            task_id = 'train_model_2',
            python_callable = _train_model,
            op_kwargs={'penalty':'l2'},
            provide_context = True
        )
    choose_best_model = PythonOperator(
        task_id = 'choose_best_model',
        python_callable = _choose_best_model
    )

    create_table >> iris_data_generation >> store_iris_sql >> extract_iris_with_hooh >> tasks >> choose_best_model