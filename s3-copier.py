from prefect import flow, task, get_run_logger
import boto3

@task
def copy_s3_objects(source_bucket, destination_bucket, file_name):
    s3_client = boto3.client('s3')
    source_name = '%s/%s' % (source_bucket,file_name)
    response = s3_client.copy_object(
    CopySource=source_name,
    Bucket=destination_bucket,      # Destination bucket
    Key=file_name                   # Destination path/filename
    )

@task
def create_bucket_manifest(bucket_url):
    resource = boto3.resource('s3')
    my_bucket = resource.Bucket(bucket_url)
    manifest = []
    for my_bucket_object in my_bucket.objects.all():
       manifest.append(my_bucket_object.key)
    return manifest

@flow(name="S3 Copier")
def runner(source_bucket, destination_bucket):
    logger = get_run_logger()
    manifest = create_bucket_manifest(source_bucket)
    for file in manifest:
        logger.info("Copy file: %s" % file)
        copy_s3_objects(source_bucket, destination_bucket, file)


if __name__ == "__main__":
    source_bucket = ""
    destination_bucket = ""
    runner(source_bucket, destination_bucket)
