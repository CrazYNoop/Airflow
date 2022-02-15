from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas as pd

def api_coin(key_access):
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
        return data 

    def transform_data(data):  
        #file_path='/mnt/c/airflow/files/' 
        file_path='/'
        df=pd.DataFrame(data['data'])
        df=df[['name','symbol','date_added','cmc_rank','quote']]
        df['usd_price']=df['quote'].apply(lambda x: x['USD']['price'])
        df['volume_change_24h']=df['quote'].apply(lambda x: x['USD']['volume_change_24h'])
        df['percent_change_24h']=df['quote'].apply(lambda x: x['USD']['percent_change_24h'])
        df=df.drop(columns='quote')
        df.to_csv(file_path+'api_coin.csv')
        return str(file_path+'api_coin.csv')