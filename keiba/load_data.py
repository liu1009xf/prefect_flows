from prefect import flow, get_run_logger
from prefect.deployments import Deployment

@flow(name="Prefect Cloud Quickstart")
def quickstart_flow():
    logger = get_run_logger()
    logger.warning("Local quickstart flow is running!")

def deploy():
    deployment = Deployment.build_from_flow(
        flow=quickstart_flow,
        name="prefect-example-deployment"
    )
    deployment.apply()

if __name__ == "__main__":
    deploy()