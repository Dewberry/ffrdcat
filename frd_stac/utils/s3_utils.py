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


def download_from_s3(bucket_name, file_key, local_file_path):
    s3 = boto3.client("s3")

    try:
        s3.download_file(bucket_name, file_key, local_file_path)
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def split_s3_path(s3_path):
    s3_path = s3_path.replace("s3://", "")
    parts = s3_path.split("/", 1)
    bucket_name = parts[0]
    object_key = parts[1] if len(parts) > 1 else ""

    return bucket_name, object_key


def s3_key_exists(bucket_name, key):
    s3 = boto3.client("s3")

    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            print("An error occurred:", e)
            return False
