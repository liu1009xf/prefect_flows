from prefect.deployments import Deployment
from prefect import Flow, flow, get_run_logger, task
from prefect.filesystems import GitHub
from prefect.tasks import task_input_hash
from prefect.deployments import run_deployment
import asyncio

from pathlib import Path
import datetime as dt
import os
import json
from pymongo import MongoClient
import pandas as pd

import abt

github_block = GitHub.load("github-repo")

def get_mongo_url():
    cred_file = open(os.path.join(str(Path.home()), '.config/.mongo_credential.json')) 
    mongo_cred = json.load(cred_file)
    cred_file.close()
    url = f"mongodb+srv://{mongo_cred['user']}:{mongo_cred['pw']}@scheduler.gyvxeuz.mongodb.net/?retryWrites=true&w=majority"
    return url

@task(cache_key_fn=task_input_hash, cache_expiration=dt.timedelta(hours=10))
def get_schedule(date:str) -> pd.DataFrame:
    url = get_mongo_url()
    client = MongoClient(url)
    db = client['schedule']
    col = db['central']

    date_filter = {
        "date": {
            '$eq': date,
        }
    } 
    client.close()
    return pd.DataFrame(list(col.find(date_filter)))


# def get_next_race(data, time) -> pd.DataFrame:
#     timeFunc = lambda x: dt.datetime.strptime(x['date'], '%Y%m%d').replace(hour=x["startHour"], minute=x["startMinute"])
#     data['startTime'] = data[['date', 'startHour', 'startMinute']].apply(lambda x: timeFunc(x), axis=1)
#     next_race = data[data['startTime'].apply(lambda x: time>=(x-dt.timedelta(minutes=3)) and time<(x-dt.timedelta(minutes=2)))]
#     logger.info(next_race)
#     print(next_race)
#     return next_race

@task
def register_flows(data:pd.DataFrame, deployment_name:str = 'run_strategy/run_strategy') -> None:
    logger = get_run_logger()
    jst = tzinfo=dt.timezone(dt.timedelta(hours=9))
    timeFunc = lambda x: dt.datetime.strptime(x['date'], '%Y%m%d').replace(hour=x["startHour"],
                                                                           minute=x["startMinute"])
    time = dt.datetime.now()
    logger.info(time)
    data['startTime'] = data[['date', 'startHour', 'startMinute']].apply(lambda x: timeFunc(x), axis=1)
    data['scheduleTime'] = data['startTime'].apply(lambda x: x - dt.timedelta(minutes=5))
    data['race_info'] = data[["location", "round", "day", "race"]].apply(lambda x:"_".join([str(x) for x in x]), axis=1)
    data = data.sort_values('startTime')
    for _, x in data.iterrows():
        logger.info(f'skip {x["scheduleTime"]}')
        if time <= x['scheduleTime']:
          logger.info(f'schedule: {x["scheduleTime"]}')
          response = run_deployment(
            name=deployment_name,
            scheduled_time=x['scheduleTime'].replace(tzinfo=jst),
            parameters={'name':x['race_info'],'jraurl':x['url'], 'nkburl':x['netkeibaURL']}),
          
          logger.info(f'scheduled for {x[["date", "location", "round", "day", "race"]].to_dict()} {response}')
    
    # data.apply(lambda x: run_deployment(name=deployment_name,
    #                                         scheduled_time=x['scheduleTime'],
    #                                         parameters={'jraurl':x['url'], 'nkburl':x['netkeibaURL']}
    #                                   ) if time<= x['scheduleTime'] else None, axis=1)    
    # logger.info(data['date', 'location', 'round', 'day', 'race'])    

@flow
def schedule_race(deployment_name:str='run-strategy/run_strategy'):
    today = dt.date.today().strftime("%Y%m%d")
    schedule = get_schedule(today)
    register_flows(schedule, deployment_name)
    # time = dt.datetime.now()
    # get_next_race(schedule, time)
    

def deploy():
    deployment = Deployment.build_from_flow(
        flow=schedule_race,
        name="schedule_race",
        storage = github_block,
    )
    deployment.apply()

if __name__ == "__main__":
    deploy()

