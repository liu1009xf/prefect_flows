from prefect.deployments import Deployment
from prefect import flow, get_run_logger, task
from prefect.filesystems import GitHub
from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("abet")
github_block = GitHub.load("github-repo")


import abt
import pandas as pd
import datetime as dt

@task
def get_result_data(url) -> pd.DataFrame:
  dl = abt.NKBJRAShutsuba(jraurl=jraurl, nkburl= nkburl)
  return dl.data()

@flow
def get_result(name:str, tickers, resulturl:str):
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
        name="run_strategy"
    )
    deployment.apply()

if __name__ == "__main__":
    deploy()
