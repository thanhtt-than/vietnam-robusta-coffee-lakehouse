from minio_client import get_minio_client, get_bucket_name


client = get_minio_client()
bucket_name = get_bucket_name()

print("Đang kết nối MinIO...")
print("Bucket cấu hình:", bucket_name)

buckets = client.list_buckets()

print("\nDanh sách bucket:")

for bucket in buckets:
    print("-", bucket.name)

if client.bucket_exists(bucket_name):
    print(f"\nOK: Bucket '{bucket_name}' tồn tại.")
else:
    print(f"\nERROR: Bucket '{bucket_name}' chưa tồn tại.")