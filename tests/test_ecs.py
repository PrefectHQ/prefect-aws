import json

import anyio
from moto import mock_ec2, mock_ecs
from moto.ec2.utils import generate_instance_identity_document

from prefect_aws.ecs import ECSTask


def create_test_cluster(session):
    ecs_client = session.client("ecs")
    ec2_client = session.client("ec2")
    ec2_resource = session.resource("ec2")

    ecs_client.create_cluster(clusterName="default")

    images = ec2_client.describe_images()
    image_id = images["Images"][0]["ImageId"]

    test_instance = ec2_resource.create_instances(
        ImageId=image_id, MinCount=1, MaxCount=1
    )[0]

    ecs_client.register_container_instance(
        cluster="default",
        instanceIdentityDocument=json.dumps(
            generate_instance_identity_document(test_instance)
        ),
    )


async def test_task_run(aws_credentials):

    task = ECSTask(
        aws_credentials=aws_credentials,
        command=["prefect", "version"],
        launch_type="EC2",
    )
    with mock_ecs():
        with mock_ec2():
            session = aws_credentials.get_boto3_session()
            create_test_cluster(session)
            print(task.preview())
            await task.run()
