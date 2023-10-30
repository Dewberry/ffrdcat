import boto3


def get_object_datetime(bucket_name, file_key):
    s3 = boto3.client("s3")

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_key)

        if "Contents" in response:
            object_data = response["Contents"][0]
            last_modified = object_data["LastModified"]

            return last_modified
        else:
            print(f"No object found with key: {file_key}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
