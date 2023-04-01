import json
from prefect import flow, get_run_logger, task, unmapped
import requests
from bs4 import BeautifulSoup as bs
import datetime as dt
import pandas as pd
from prefect.deployments import Deployment
from pymongo import MongoClient
from pathlib import Path
from nkb import get_race_id_list, RaceDataLoader, PayoffDataLoader
import time

import os

from prefect.filesystems import GitHub

github_block = GitHub.load("github-repo")

@task
def get_mongo_url():
  cred_file = open(os.path.join(str(Path.home()), '.config/.mongo_credential.json')) 
  mongo_cred = json.load(cred_file)
  cred_file.close()
  url = f"mongodb+srv://{mongo_cred['user']}:{mongo_cred['pw']}@horse.wqv7atq.mongodb.net/?retryWrites=true&w=majority"
  return url

@task
def read_race_id_list_from_date(date, mongourl):
    client = MongoClient(mongourl)
    db = client['horseRaceJP']
    collection = db['raceId']
    res = pd.DataFrame(
       list(collection.find({'date':{'$eq':date.strftime('%Y-%m-%d')}}))
       )['raceId'].tolist()
    client.close()
    return res
    
@task
def load_race_horse_data(ids, mongourl):
    client = MongoClient(mongourl)
    db = client['horseRaceJP']
    for id in ids:
      RaceDataLoader(id).save(db)
      time.sleep(0.1)
    client.close()

@flow(name="Load Race data")
def load_race_data(date:dt.date = dt.datetime.now().date()):
    logger = get_run_logger()
    logger.info(f'create mongo client')
    url = get_mongo_url()
    logger.info(f'loading race id for date: {date}')
    raceIds = read_race_id_list_from_date(date, url)
    logger.info(raceIds)
    if len(raceIds) ==0:
      logger.info('no race on {date}')
    else:
      logger.info(f'loading race data for date: {date}')
      load_race_horse_data(raceIds, url)
    logger.info(f'done')

# def deploy():
#     deployment = Deployment.build_from_flow(
#         flow=load_race_data,
#         name="load-race-data"
#     )
#     deployment.apply()

# if __name__ == "__main__":
#     load_race_data(date=dt.date(2023,3,11))
    # RaceDataLoader(202306020501).data