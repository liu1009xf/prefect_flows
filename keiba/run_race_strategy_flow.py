from prefect.deployments import Deployment
from prefect import flow, get_run_logger, task
from prefect.filesystems import GitHub
# from prefect_github.repository import GitHubRepository

# from prefect_email import EmailServerCredentials, email_send_message
from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("abet")

github_block = GitHub.load("github-repo")
# github_block = GitHubRepository.load("github-repo")
github_block.get_directory(local_path='keiba')


import abt
import pandas as pd
import datetime as dt

@task
def get_shutsuba_data(jraurl:str, nkburl:str) -> pd.DataFrame:
  dl = abt.NKBJRAShutsuba(jraurl=jraurl, nkburl= nkburl)
  return dl.data()


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
