from prefect import flow, task, get_run_logger
import boto3,os




"""
This method sets the s3_resource object to either use localstack for local development
if the LOCALSTACK_ENDPOINT_URL variable is defined and returns the object
"""
def set_s3_resource():
    localstack_endpoint=os.environ.get('LOCALSTACK_ENDPOINT_URL')
    if(localstack_endpoint!=None):
        
        AWS_REGION = 'us-east-1'
        AWS_PROFILE = 'localstack'
        ENDPOINT_URL = localstack_endpoint
        boto3.setup_default_session(profile_name=AWS_PROFILE)
        s3_resource = boto3.resource("s3", region_name=AWS_REGION,
                            endpoint_url=ENDPOINT_URL)
    else:
        s3_resource=boto3.resource('s3')
    return s3_resource

"""
This method sets copies files from the source_bucket to the destination_bucket
using the s3 object which is set to either use localstack OR actual AWS endpoint
URL
"""
@task
def copy_all_s3_objects(source_bucket, destination_bucket):
    # Set the s3 resource object for local or remote execution
    s3 = set_s3_resource()
    source = s3.Bucket(source_bucket)
    destination = s3.Bucket(destination_bucket)
    # Copy all objects to target bucket
    for obj in source.objects.all():
        copy_source = {
            'Bucket': source_bucket,
            'Key': obj.key
        }
        
        destination.copy(copy_source, obj.key)

"""
This is the main flow method which is called by the deployment
"""
@flow(name="S3 Copier AKM",log_prints=True)
def runner(source_bucket, destination_bucket):
    logger = get_run_logger()
    copy_all_s3_objects(source_bucket,destination_bucket)

   


if __name__ == "__main__":
    # These bucket names are default. They are overridden when the deployment executes with the variables that are provided.
    source_bucket = 'my-source-bucket'
    destination_bucket = 'my-dest-bucket'
    runner(source_bucket, destination_bucket)
