package com.example;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.time.Duration;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncMongodumpS3Uploader {
    private static final int PART_SIZE = 16 * 1024 * 1024;  // 16 MB part size
    private static final int MAX_CONCURRENT_UPLOADS = 100;  // Max number of concurrent uploads

    // Method to execute the mongodump command and return an InputStream
    private static InputStream runMongoDump() throws IOException {
        String mongodumpCommand = System.getenv("MONGODUMP_COMMAND");
        if (mongodumpCommand == null || mongodumpCommand.isEmpty()) {
            throw new IllegalArgumentException("MONGODUMP_COMMAND environment variable is not set");
        }

        String[] commandParts = mongodumpCommand.split(" ");
        ProcessBuilder processBuilder = new ProcessBuilder(commandParts);
        processBuilder.redirectErrorStream(true);
        return processBuilder.start().getInputStream();
    }

    public static void multipartUploadWithS3AsyncClient(S3AsyncClient s3AsyncClient, String bucketName, String key, InputStream inputStream, int maxConcurrency) throws IOException, InterruptedException, ExecutionException {
        Logger logger = Logger.getLogger(AsyncMongodumpS3Uploader.class.getName());

        // Step 1: Initiate the multipart upload
        CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadResponseFuture =
                s3AsyncClient.createMultipartUpload(CreateMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build());

        CreateMultipartUploadResponse createMultipartUploadResponse = createMultipartUploadResponseFuture.join();
        String uploadId = createMultipartUploadResponse.uploadId();
        logger.info("Multipart upload initiated with upload ID: " + uploadId);

        List<CompletedPart> completedParts = Collections.synchronizedList(new ArrayList<>());
        byte[] buffer = new byte[PART_SIZE * 2];
        int partNumber = 1;
        int bytesRead;
        int currentBufferSize = 0;

        // Concurrency control
        AtomicInteger inFlightRequests = new AtomicInteger(0);
        List<CompletableFuture<CompletedPart>> inFlightUploads = new ArrayList<>();

        try {
            // Step 2: Read from input stream and schedule uploads with concurrency control
            while (true) {
                // Read data into buffer
                bytesRead = inputStream.read(buffer, currentBufferSize, PART_SIZE);

                if (bytesRead == -1 && currentBufferSize == 0) {
                    // If there's nothing more to read and no data in the buffer, break
                    logger.info("Reached end of stream with no more data to process.");
                    break;
                }

                // Update the buffer size
                if (bytesRead > 0) {
                    currentBufferSize += bytesRead;
                }

                // If the buffer is full, or we reach EOF and still have data in the buffer
                if (currentBufferSize >= PART_SIZE || (bytesRead == -1 && currentBufferSize > 0)) {
                    logger.info("Buffer full or EOF reached for part " + partNumber + ", preparing for upload...");

                    byte[] partData = Arrays.copyOf(buffer, currentBufferSize);  // Copy buffer to avoid overwriting it during upload
                    // Make request to upload part to S3
                    CompletableFuture<CompletedPart> uploadFuture = uploadPartAsync(s3AsyncClient, bucketName, key, uploadId, partData, partNumber);

                    inFlightRequests.incrementAndGet();  // Increment in-flight requests counter
                    inFlightUploads.add(uploadFuture);   // Add future to in-flight list
                    partNumber++;

                    // Handle completion of the upload asynchronously
                    uploadFuture.whenComplete((completedPart, throwable) -> {
                        if (throwable != null) {
                            // TODO: Could handle retrying upload of part and not decrement?
                            logger.severe("Failed to upload part " + completedPart.partNumber() + ": " + throwable.getMessage());
                            throw new CompletionException(throwable);
                        } else {
                            completedParts.add(completedPart);
                            logger.info("Part " + completedPart.partNumber() + " completed.");
                            inFlightRequests.decrementAndGet();  // Decrement in-flight counter when upload completes
                        }
                    });

                    currentBufferSize = 0;  // Reset buffer after part is scheduled

                    // Check if we've hit the concurrency limit
                    if (inFlightRequests.get() >= maxConcurrency) {
                        logger.info("Max concurrency limit reached, waiting...");
                        // Wait for at least one future to complete using CompletableFuture.anyOf()
                        CompletableFuture<Void> firstCompleted = CompletableFuture.anyOf(inFlightUploads.toArray(new CompletableFuture[0]))
                                .thenRun(() -> {
                                    // Remove any completed futures from the in-flight list
                                    inFlightUploads.removeIf(CompletableFuture::isDone);
                                });

                        firstCompleted.join();  // This doesn't block indefinitely, it waits for any in-flight upload to finish
                    }
                }
            }

            // Wait for remaining uploads to complete
            CompletableFuture<Void> allUploads = CompletableFuture.allOf(inFlightUploads.toArray(new CompletableFuture[0]));
            allUploads.join();  // Await completion of all in-flight uploads

            // Step 3: Complete the multipart upload
            completedParts.sort(Comparator.comparing(CompletedPart::partNumber));
            CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build();

            s3AsyncClient.completeMultipartUpload(completeRequest).join();
            logger.info("Upload completed successfully with all parts.");

        } catch (IOException e) {
            logger.severe("Error during upload: " + e.getMessage());
            abortMultipartUploadAsync(s3AsyncClient, bucketName, key, uploadId);
            throw e;
        }
    }

   private static CompletableFuture<CompletedPart> uploadPartAsync(S3AsyncClient s3AsyncClient, String bucketName, String key, String uploadId, byte[] partData, int partNumber) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(partData);

        UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength((long) partData.length)
                .build();

        CompletableFuture<UploadPartResponse> uploadPartResponseFuture = s3AsyncClient.uploadPart(uploadPartRequest, AsyncRequestBody.fromByteBuffer(byteBuffer));

        return uploadPartResponseFuture
            .thenApply(uploadPartResponse -> {
                System.out.println("Uploaded part " + partNumber + " (size: " + partData.length + " bytes)");
                return CompletedPart.builder()
                        .partNumber(partNumber)
                        .eTag(uploadPartResponse.eTag())
                        .build();
            })
            .exceptionally(e -> {
                System.err.println("Error uploading part " + partNumber + ": " + e.getMessage());
                // Handle the error (e.g., log or rethrow, depending on your need)
                throw new RuntimeException("Failed to upload part", e);
            });
    }

    // Async helper method to abort multipart upload in case of failure
    private static void abortMultipartUploadAsync(S3AsyncClient s3AsyncClient, String bucketName, String key, String uploadId) {
        s3AsyncClient.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .build()).join();
        System.out.println("Multipart upload aborted.");
    }

    public static void main(String[] args) {
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(150)  // Increase concurrency based on system resources
                .connectionTimeout(Duration.ofSeconds(120))  // Increased connection timeout
                .writeTimeout(Duration.ofSeconds(120))  // Increased write timeout
                .connectionAcquisitionTimeout(Duration.ofSeconds(180))  // Increased acquisition timeout
                .maxPendingConnectionAcquires(10_000)
                .build();
        
        S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
                .httpClient(httpClient)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build();

        String bucketName = System.getenv("S3_BUCKET_NAME");
        if (bucketName == null) {
            System.err.println("Error: S3_BUCKET_NAME environment variable is not set.");
            return;
        }

        Instant now = Instant.now();
        String formattedTimestamp = DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                .withZone(ZoneId.of("UTC"))
                .format(now);

        System.out.println("S3 object key: " + formattedTimestamp);

        try {
            InputStream dumpInputStream = runMongoDump();
            multipartUploadWithS3AsyncClient(s3AsyncClient, bucketName, formattedTimestamp, dumpInputStream, MAX_CONCURRENT_UPLOADS);
        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            s3AsyncClient.close();
            httpClient.close();
        }
    }
}
