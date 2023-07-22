from prefect.deployments import Deployment
from prefect import flow, get_run_logger, task
from prefect.filesystems import GitHub
from prefect.tasks import task_input_hash

from pathlib import Path
import os
import json
from pymongo import MongoClient
import pymongo
import datetime as dt

import abt


github_block = GitHub.load("github-repo")

def get_mongo_url():
    cred_file = open(os.path.join(str(Path.home()), '.config/.mongo_credential.json')) 
    mongo_cred = json.load(cred_file)
    cred_file.close()
    url = f"mongodb+srv://{mongo_cred['user']}:{mongo_cred['pw']}@scheduler.gyvxeuz.mongodb.net/?retryWrites=true&w=majority"
    return url

@task(cache_key_fn=task_input_hash, cache_expiration=dt.timedelta(hours=10))
def load_schedule():
    logger = get_run_logger()
    url = get_mongo_url()
    client = MongoClient(url)
    db = client['schedule']
    scheduler =  abt.JRASchedule()
    data = scheduler.data()
    data['date'] = data['date'].apply(lambda x: x.strftime('%Y%m%d'))
    id_columns = ['date', 'location', 'round', 'day', 'race']
    id_builder = lambda x:f'{x["date"]}{abt.RaceLocation[x["location"]].value:02d}_{x["round"]}_{x["day"]}_{x["race"]}'
    data['_id'] = data[id_columns].apply(id_builder, axis=1)
    data = data.to_dict('records')
    col = db['central']
    try:
        col.insert_many(data)
    except pymongo.errors.BulkWriteError as e: # type: ignore
        logger.warning('exists')
    client.close()
      


@flow(name="Load Race Schedule")
def get_race_schedule():
    load_schedule()

def deploy():
    deployment = Deployment.build_from_flow(
        flow=get_race_schedule,
        name="get-race-schedule",
        storage = github_block,
    )
    deployment.apply()

if __name__ == "__main__":
    deploy()

