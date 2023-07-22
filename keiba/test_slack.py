from prefect.deployments import Deployment
from prefect import flow, get_run_logger, task
from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("abet")
slack_webhook_block.notify("Hello from Prefect!")