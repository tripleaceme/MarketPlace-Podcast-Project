from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import datetime, timedelta 
import requests
import xmltodict
import os


os.environ["no_proxy"]="*"

default_args = {'owner': 'Ayoade Abel',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        }
urls = ['https://www.marketplace.org/feed/podcast/marketplace-morning-report',
        'https://www.marketplace.org/feed/podcast/marketplace-tech',
        'https://feeds.megaphone.fm/WWO1595437966', # this might require an extra look.
        'https://www.marketplace.org/feed/podcast/marketplace',
        'https://www.marketplace.org/feed/podcast/make-me-smart',
        'https://www.marketplace.org/feed/podcast/how-we-survive',
        'https://www.marketplace.org/feed/podcast/this-is-uncomfortable-reema-khrais',
        'https://www.marketplace.org/feed/podcast/corner-office-from-marketplace',
        'https://www.marketplace.org/feed/podcast/the-uncertain-hour',
        'https://www.marketplace.org/feed/podcast/million-bazillion'
        ]

# change the url, save the code , clear the _et_episodes and populate_db task in webserver to get the new feed into the database

p_url = 'https://www.marketplace.org/feed/podcast/marketplace-morning-report'


def _get_episodes(**kwargs):

    data = requests.get(url=p_url)
    feed = xmltodict.parse(data.text)
    results = feed['rss']['channel']['item']
    return results



def _load_database(ti):
    populate_db = SqliteHook(sqlite_conn_id='podcasts')
    results = ti.xcom_pull(key='return_value', task_ids=['get_episodes'])
    stored_episodes = populate_db.get_pandas_df("SELECT * FROM episodes;")
    new_episodes = []
    for episodes in results:
        for episode in episodes:
            if episode['link'] not in stored_episodes['p_link'].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([# podcast episodes
                                    episode['title'],
                                    episode['itunes:subtitle'],
                                    episode['link'],
                                    episode['itunes:author'].split('/')[-1],
                                    episode['pubDate'][:-15],
                                    episode['pubDate'][-14:-6],
                                    episode['description'],
                                    episode['itunes:duration'],
                                    episode['enclosure']['@url'],
                                    episode['link'].split('/')[-2],
                                    filename]
                                    )
        populate_db.insert_rows(table='episodes', rows=new_episodes, target_fields=['p_title',
                                                                                    'p_subtitle',
                                                                                    'p_link',
                                                                                    'p_author' ,
                                                                                    'p_date_pub' ,
                                                                                    'p_time_pub', 
                                                                                    'p_description',
                                                                                    'p_duration',
                                                                                    'p_download',
                                                                                    'P_category',
                                                                                    'p_filename']
                                                                                    )


with DAG(dag_id='dowload_podcasts',default_args=default_args,description=' download podcast episode from Marketplace',
        start_date=datetime(2022,1,1),schedule_interval='@daily',catchup=False) as dag:


# CREATE DATABASE
        create_db = SqliteOperator(
            task_id = 'create_table',
            sql = r"""
            CREATE TABLE IF NOT EXISTS episodes(
                p_title TEXT,
                p_subtitle TEXT,
                p_link TEXT PRIMARY KEY,
                p_author TEXT ,
                p_date_pub DATE ,
                p_time_pub TIME, 
                p_description TEXT,
                p_duration TEXT,
                p_download TEXT,
                p_category TEXT,
                p_filename TEXT
            )
            """,
            sqlite_conn_id='podcasts'
        )

# DOWNLOAD PODCASTS
        get_episodes = PythonOperator(
            task_id = 'get_episodes',
            python_callable=_get_episodes
        )


# POPULATE DATABASE
        populate_db = PythonOperator(
            task_id='populate_db',
            python_callable=_load_database
            )

# Set dependencies
create_db >> get_episodes >> populate_db
