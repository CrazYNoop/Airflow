from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from custom_operator.s3_operator import S3_Operator
from airflow.decorators import task
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas as pd
import json

key_access = Variable.get("coinmarket_api_key")
#config = Variable.get("api_coin_dag",deserialize_json=True)
#key_access = config['coinmarket_api_key']
aws_access_key_id = Variable.get('AWS_S3_ACCESS_KEY')
aws_secret_access_key = Variable.get('AWS_S3_SECRET_KEY')

default_args = {
    'owner': 'keka',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'api_coin_dag',
    default_args=default_args,
    description='A simple api_coin_dag',
    schedule_interval= "0 12 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    file_xcom = "{{ ti.xcom_pull(task_ids=api_coin_task) }}"

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
    

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    @task(task_id="api_coin_task", provide_context=True)
    def api_coin(key_access):
        file_path='/mnt/c/airflow/files/'
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        parameters = {
            'start':'1',
            'limit':'10', #только до 5000
            'convert':'USD'
            }
        headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': key_access,
        }
        session = Session()
        session.headers.update(headers)
        try:
            response = session.get(url, params=parameters)
            data = json.loads(response.text)
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)
        df=pd.DataFrame(data['data'])
        df=df[['name','symbol','date_added','cmc_rank','quote']]
        df['usd_price']=df['quote'].apply(lambda x: x['USD']['price'])
        df['volume_change_24h']=df['quote'].apply(lambda x: x['USD']['volume_change_24h'])
        df['percent_change_24h']=df['quote'].apply(lambda x: x['USD']['percent_change_24h'])
        df=df.drop(columns='quote')
        df.to_csv(file_path+'api_coin.csv')
        return str(file_path+'api_coin.csv')
    t3 = api_coin(key_access)

    t4 = S3_Operator( # не работает xcom
        task_id='s3_upload_file',
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        #file = file_xcom,
        file = '/mnt/c/airflow/files/api_coin.csv',
        s3_path='api_coin',
       
    )


    t1 >> t2 >> t3 >> t4