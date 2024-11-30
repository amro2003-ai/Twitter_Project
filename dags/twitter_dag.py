from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from ntscraper import Nitter
import pandas as pd
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Extract JSON data, transform, and load into DB every hour',
    schedule_interval='@hourly',  
    start_date=datetime(2024, 11, 30),
    catchup=False,
) as dag:
    
    @task
    def extract():
     scrapper = Nitter(log_level = 1, skip_instance_check=False)

     tweets = scrapper.get_tweets("elonmusk", mode="user", number=10)

     return tweets
    
    @task
    def transform(tweets):
        data = {
            'text': [],
            'likes':[],
            'quotes':[],
            'retweets':[],
            'comments':[]
        }

        for tweet in tweets['tweets']:
            data['text'].append(tweet['text'])
            data['likes'].append(tweet['stats']['likes'])
            data['quotes'].append(tweet['stats']['quotes'])
            data['retweets'].append(tweet['stats']['retweets'])
            data['comments'].append(tweet['stats']['comments'])

        df = pd.DataFrame(data)

        return df
    
    @task
    def load(df):
       
       engine = create_engine('mysql+mysqlconnector://root:1111@localhost/tweets')
       
       df.to_sql(name='tweets_data', con=engine, if_exists='append', index=False)


    tweets = extract()
    df = transform(tweets)
    load(df)

    