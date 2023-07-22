from prefect.deployments import Deployment
from prefect import flow, get_run_logger, task
from prefect.filesystems import GitHub
# from prefect_email import EmailServerCredentials, email_send_message
from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("abet")
# import asyncio

# email_credentials_block = EmailServerCredentials.load("xlatombet")

github_block = GitHub.load("https://github.com/liu1009xf/prefect_flows")


import abt
import pandas as pd
import datetime as dt

@task
def get_shutsuba_data(jraurl:str, nkburl:str) -> pd.DataFrame:
  dl = abt.NKBJRAShutsuba(jraurl=jraurl, nkburl= nkburl)
  return dl.data()


# def email_prediction(race:str, prediction):
#     email_send_message(
#         email_server_credentials=email_credentials_block, # type: ignore
#         subject=f"race: {race}, prediction: {prediction}",
#         msg=f"race: {race}, prediction: {prediction}",
#         email_to=email_credentials_block.username,
#     )

@flow
def run_strategy(name:str, jraurl:str, nkburl:str):
    data = get_shutsuba_data(jraurl, nkburl)
    predict = data[data['popularity']==1].iloc[0]
    slack_webhook_block.notify(f'''
    Race: {name}, 
    prediction: {predict['horseNum']}, 
    popularity: {predict['popularity']}
    odds: {predict['odds']}
    ''') # type: ignore
    logger = get_run_logger()
    logger.info(data)

    

def deploy():
    deployment = Deployment.build_from_flow(
        flow=run_strategy,
        name="run_strategy",
        storage = github_block,
    )
    deployment.apply()

if __name__ == "__main__":
    deploy()
