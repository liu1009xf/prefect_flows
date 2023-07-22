from prefect.deployments import Deployment
from prefect import flow, get_run_logger, task
from prefect_email import EmailServerCredentials, email_send_message

email_credentials_block = EmailServerCredentials.load("xlatombet")

def email_prediction():
    email_send_message(
        email_server_credentials=email_credentials_block, # type: ignore
        subject=f"Test Message",
        msg=f"Test",
        email_to=email_credentials_block.username,
    )

@flow
def run_strategy():
    email_prediction()
    logger = get_run_logger()
    logger.info("email sent")

if __name__ == "__main__":    
  run_strategy()