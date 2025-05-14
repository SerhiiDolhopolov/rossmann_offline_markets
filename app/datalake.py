import datetime
from typing import BinaryIO
from pathlib import Path

from minio import Minio
from minio.error import S3Error

from app.config import MINIO_HOST, MINIO_PORT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY


endpoint = f"{MINIO_HOST}:{MINIO_PORT}"
client = Minio(endpoint=endpoint,
               access_key=MINIO_ACCESS_KEY,
               secret_key=MINIO_SECRET_KEY,
               secure=False,
               )


def create_bucket(bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket already exists", bucket_name)
        
def upload_file(bucket_name: str, 
                distination_file: str, 
                data: BinaryIO, 
                content_type: str = 'application/octet-stream'):
    """Upload a file to MinIO bucket from data.
    Args:
        bucket_name (str): name of the bucket
        distination_file (str): name of the file in the bucket
        data (BinaryIO): data to upload
        content_type (str, optional): content type, more in minio put_object method.
    """
    try:
        data.seek(0)
        client.put_object(
            bucket_name=bucket_name,
            object_name=distination_file,
            data=data,
            length=data.getbuffer().nbytes,
            content_type=content_type,
        )
    except S3Error as err:
        print("Minio S3 error:", err)
        raise
    except Exception as err:
        print("Upload file error:", err)
        raise
    
def save_transactions_report(data: BinaryIO, shop_id: int, date: datetime.date):
    BUCKET_NAME = f"transactions"
    SHOP_PATH = Path(f'shop_{shop_id}')
    MONTH_PATH = Path(date.strftime('%Y-%m'))
    FILE_PATH = Path(f'trans_rep_shop{shop_id}_{date}.csv')
    DESTINATION_PATH = SHOP_PATH / MONTH_PATH / FILE_PATH
    
    create_bucket(BUCKET_NAME)
    upload_file(BUCKET_NAME, str(DESTINATION_PATH), data, content_type='text/csv')