import json
from typing import Any, Optional
from copy import deepcopy
from prefect import flow, get_run_logger, task
import requests
from bs4 import BeautifulSoup as bs
import bs4
import re
import datetime as dt
from prefect.deployments import Deployment
from pymongo import MongoClient


from pathlib import Path
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
def get_race_id_list_from_date(today):
    date = f'{today.year:04}{today.month:02}{today.day:02}'
    url = 'https://db.netkeiba.com/race/list/' + date
    html = requests.get(url)
    html.encoding = "EUC-JP"
    soup = bs4.BeautifulSoup(html.text, "html.parser")
    race_list = soup.find('div', attrs={"class": 'race_list fc'})
    if race_list is None:
        return list()
    a_tag_list = race_list.find_all('a')  # type: ignore
    href_list = [a_tag.get('href') for a_tag in a_tag_list]
    race_id_list = list()
    for href in href_list:
        for race_id in re.findall('[0-9]{12}', href):
            race_id_list.append(race_id)
    return list(set(race_id_list))

@task
def insertIntoMongoDB(date, idlist, mongoClient):
    db = mongoClient['horseRaceJP']
    collection = db['raceId']
    data = [{'date':date.strftime('%Y-%m-%d'), 'raceId':id} for id in idlist]
    collection.insert_many(data)

@flow(name="Load Race ID")
def load_race_id(date:dt.date = dt.datetime.now().date()):
    logger = get_run_logger()
    logger.info(f'loading race id for date: {date}')
    raceIds = get_race_id_list_from_date(date)
    logger.info(f'race ids for date: {date} loaded')
    if len(raceIds)>0:
      logger.info(f'create mongo client')
      url = get_mongo_url()
      client = MongoClient(url)
      logger.info(f'writing race id into mongo db')
      insertIntoMongoDB(date, raceIds, client)
    else:
        logger.info('No race for {date}')
    logger.info(f'done')

def deploy():
    deployment = Deployment.build_from_flow(
        flow=load_race_id,
        name="load-race-id"
    )
    deployment.apply()

if __name__ == "__main__":
    deploy()