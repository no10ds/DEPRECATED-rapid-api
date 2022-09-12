# Large Dataset Upload Limitations

## Limitations

To handle large datasets, most of the processing is done asynchronously. This is because of several limitations:

1. The application resources are limited and we cannot load a large (>500mb) file into volatile memory and process it
2. Browsers have a timeout when waiting for a response to be returned (typically 1-5 minutes)

## User experience

There are two main constraining factors to the user experience when uploading a dataset.

1. The user’s internet speed (particularly upload speed, see table below)
    1. We have seen 5GB files take >30mins on a slow connection and < 10mins on a fast one)
2. The time it takes to process the uploaded file in the background before it is able to be queried etc.

### File upload and processing rough benchmarks

- `Time to process` refers to the time it takes to process the file once it has been saved to disk and until it is ready
  to be queried.

| File size | Upload speed | Time to upload | Time to process |
|-----------|--------------|----------------|-----------------|
| 20MB      | 3.7Mb/s      | 20s            | ~10s            |
| 200MB     | 3.7Mb/s      | 2mins          | ~1min 40s       |
| 780MB     | 3.7Mb/s      | 8mins          | ~7mins          |
| 780MB     | 17Mb/s       | 2mins          | ~7mins          |
| 5GB       | 17Mb/s       | 10mins         | ~35mins         |

## Possible improvements

Potential areas to focus on to improve performance could be:

1. Increase the application resource allowances (CPU and Memory) to improve processing time
    1. Increase the chunk size in which the uploading file is stored to disk. Currently we use a 50MB chunk size but
       this could be over 100-200MB.
    2. Allow larger chunks of data to be processed (currently we process 200,000 rows at a time, but this could be well
       over 1,000,000 if sufficient memory were available)
2. Provide a pre-signed URL to upload the file directly to S3 and then use AWS tools (Lambda, Glue, etc.) to perform
   processing exclusively in ‘the cloud’.
3. The maximum file size that can be uploaded is now restricted by the persistent storage available to the application.
   This is currently 20GB,
   however [could be increased up to 200GB](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-storage.html)
   .