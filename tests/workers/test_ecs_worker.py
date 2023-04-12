from uuid import uuid4

import pytest
from prefect.server.schemas.core import FlowRun

from prefect_aws.workers.ecs import ECSJobConfiguration, ECSWorker


@pytest.fixture
def flow_run():
    return FlowRun(flow_id=uuid4())


async def test_container_command(aws_credentials, flow_run):
    configuration = ECSJobConfiguration(
        aws_credentials=aws_credentials, command="prefect version"
    )

    session = aws_credentials.get_boto3_session()
    ecs_client = session.client("ecs")

    async with ECSWorker(work_pool_name="test") as worker:
        await worker.run(flow_run, configuration)
