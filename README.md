# MarketPlace Podcast Project
 Airflow Pipeline to download Podcast data from MarketPlace

# Requirements
- Setup Airflow
- create an sqlite connection by running:
  - airflow connections add podcasts --conn-type 'sqlite' --conn-host '*/Users/mac/Documents/local_airflow/episode_db.db*'  to add databse connection
change that to the databse file directory
