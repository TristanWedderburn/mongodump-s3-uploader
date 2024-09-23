# mongodump-s3-uploader
Testing performance of JVM and AWS SDK for uploading mongodump to S3

# Build
`bazel build //:mongodump_s3_uploader`

To build the .jar to run with java -jar, run:
`bazel build //:mongodump_s3_uploader_deploy.jar`

Note: .jar files are created under `bazel-bin/`

# mongodump command
Set the mongodump command used using:
```
export MONGODUMP_COMMAND=<command>
```

# Run
Basic runner:
`bazel-bin/mongodump_s3_uploader`

Running the .jar:
`time java -Xms16g -Xmx16g -jar bazel-bin/mongodump_s3_uploader_deploy.jar`

# AWS credentials setup
Follow https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html to set env variables for AWS credentials

Also, export s3 bucket name using: `export S3_BUCKET_NAME=your-bucket-name`

