# oseh jobs

job queue for oseh using [redis](https://redis.io/)

## Getting Started

NOTE: This should be installed in the same directory as the `backend` when running in
development mode; rather than using an external S3 bucket, files will be stored in
`S3_LOCAL_BUCKET_PATH` which would typically be set to `../backend/s3_buckets`, causing
files to be stored at `../backend/s3_buckets/{bucket_name}/{key}` which, for development,
is:

-   cheaper, since you don't have to pay for S3 storage
-   easier to debug, since you can see the files in the filesystem
-   easier to setup, since you don't need to setup an S3 bucket
-   faster, since you don't have to wait for S3 to upload the files
-   more available, since you don't need internet access

```sh
python -m venv venv
. venv/bin/activate
python -m pip install -U pip
pip install -r requirements.txt
python main.py
```

## Contributing

This project uses [black](https://github.com/psf/black) for linting
